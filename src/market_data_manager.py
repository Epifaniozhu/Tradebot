# -*- coding: utf-8 -*-
import asyncio
import os
if os.name == 'nt':
    from asyncio import WindowsSelectorEventLoopPolicy
    asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())

from typing import Dict, List, Optional, Any, Union
import ccxt
import pandas as pd
from loguru import logger
import sqlite3
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import time
from collections import defaultdict
from contextlib import contextmanager
from functools import wraps
import numpy as np
import aiohttp
import json
import websockets
import websockets.client
import socket
import socks
from urllib.parse import urlparse
import ssl
import zlib
import logging

# Load environment variables
load_dotenv()

def retry_on_network_error(max_retries: int = 3, delay: float = 1.0):
    """网络请求重试装饰器"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for retry in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (ccxt.NetworkError, ccxt.ExchangeNotAvailable) as e:
                    last_error = e
                    if retry < max_retries - 1:
                        sleep_time = delay * (retry + 1)
                        logger.warning(f"网络请求失败，{sleep_time}秒后重试: {str(e)}")
                        time.sleep(sleep_time)
                    continue
                except Exception as e:
                    logger.error(f"请求失败: {str(e)}")
                    raise
            logger.error(f"重试{max_retries}次后仍然失败: {str(last_error)}")
            raise last_error
        return wrapper
    return decorator

def validate_symbol(func):
    """交易对格式验证装饰器"""
    @wraps(func)
    def wrapper(self, symbol: str, *args, **kwargs):
        if not isinstance(symbol, str):
            raise ValueError("交易对必须是字符串类型")
        if not symbol.endswith('USDT'):
            raise ValueError("只支持USDT永续合约")
        return func(self, symbol, *args, **kwargs)
    return wrapper

def validate_timeframe(func):
    """时间周期验证装饰器"""
    @wraps(func)
    def wrapper(self, *args, timeframe: str = '1h', **kwargs):
        valid_timeframes = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
        if timeframe not in valid_timeframes:
            raise ValueError(f"无效的时间周期，支持的时间周期: {', '.join(valid_timeframes)}")
        return func(self, *args, timeframe=timeframe, **kwargs)
    return wrapper

def validate_ohlcv(ohlcv_data: List[List[Union[int, float]]]) -> bool:
    """
    验证OHLCV数据格式
    :return: True if valid, False otherwise
    """
    if not isinstance(ohlcv_data, list):
        return False
    
    for candle in ohlcv_data:
        # 检查K线数据格式
        if not isinstance(candle, list) or len(candle) != 6:
            return False
        
        timestamp, open_price, high, low, close, volume = candle
        
        # 检查时间戳
        if not isinstance(timestamp, (int, float)) or timestamp <= 0:
            return False
            
        # 检查价格数据
        if not all(isinstance(x, (int, float)) and x > 0 for x in [open_price, high, low, close]):
            return False
            
        # 检查价格逻辑关系
        if not (low <= open_price <= high and low <= close <= high):
            return False
            
        # 检查交易量
        if not isinstance(volume, (int, float)) or volume < 0:
            return False
            
    return True

class ProxyWebSocket:
    def __init__(self, proxy_url):
        self.proxy_url = proxy_url
        parsed = urlparse(proxy_url)
        self.proxy_host = parsed.hostname
        self.proxy_port = parsed.port
        self.session = None
        self.ws = None
        self.reconnect_attempts = 0
        self.MAX_RECONNECT_ATTEMPTS = 5
        self.RECONNECT_DELAY = 5
        self.last_pong = None
        self.is_connected = False
        logger.info(f"初始化ProxyWebSocket，代理地址: {proxy_url}")

    async def connect(self, ws_url):
        """创建WebSocket连接
        
        Args:
            ws_url: WebSocket服务器地址
            
        Returns:
            aiohttp.ClientWebSocketResponse: WebSocket连接对象
        """
        while self.reconnect_attempts < self.MAX_RECONNECT_ATTEMPTS:
            try:
                logger.info(f"开始建立WebSocket连接: {ws_url}")
                
                # 创建SSL上下文
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                ssl_context.set_ciphers('DEFAULT:@SECLEVEL=1')
                
                # 创建连接器
                connector = aiohttp.TCPConnector(
                    ssl=ssl_context,
                    enable_cleanup_closed=True,
                    force_close=False,
                    keepalive_timeout=30,
                    ttl_dns_cache=300,
                    limit=100
                )
                
                # 创建客户端会话
                self.session = aiohttp.ClientSession(
                    connector=connector,
                    timeout=aiohttp.ClientTimeout(
                        total=60,
                        connect=10,
                        sock_connect=10,
                        sock_read=10
                    ),
                    headers={
                        'User-Agent': 'Mozilla/5.0',
                        'Accept': 'application/json'
                    }
                )
                
                logger.debug("创建会话成功，准备建立WebSocket连接")
                
                # 创建WebSocket连接
                self.ws = await self.session.ws_connect(
                    ws_url,
                    proxy=self.proxy_url,
                    heartbeat=20,
                    compress=0,
                    autoclose=True,
                    autoping=True,
                    max_msg_size=0,
                    timeout=30,
                    receive_timeout=30,
                    protocols=['websocket']
                )
                
                logger.info("WebSocket连接建立成功")
                
                # 发送测试消息并等待响应
                await self.ws.ping()
                pong = await self.ws.receive(timeout=5)
                if pong.type in [aiohttp.WSMsgType.PONG, aiohttp.WSMsgType.TEXT]:
                    logger.debug("WebSocket连接测试成功")
                    self.reconnect_attempts = 0  # 重置重连计数
                    self.is_connected = True
                    self.last_pong = time.time()
                    return self.ws
                else:
                    logger.warning(f"收到意外的响应类型: {pong.type}")
                    await self.close()
                    
            except asyncio.TimeoutError:
                logger.error("建立WebSocket连接超时")
                await self.close()
            except aiohttp.ClientError as e:
                logger.error(f"建立WebSocket连接失败 (ClientError): {str(e)}")
                await self.close()
            except Exception as e:
                logger.error(f"建立WebSocket连接失败: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                await self.close()
                
            self.reconnect_attempts += 1
            if self.reconnect_attempts < self.MAX_RECONNECT_ATTEMPTS:
                delay = self.RECONNECT_DELAY * self.reconnect_attempts
                logger.info(f"等待 {delay} 秒后尝试重连...")
                await asyncio.sleep(delay)
            
        logger.error(f"WebSocket连接失败: 重试{self.MAX_RECONNECT_ATTEMPTS}次后仍然失败")
        raise ConnectionError("无法建立WebSocket连接")

    async def close(self):
        """关闭连接和会话"""
        try:
            logger.debug("开始关闭WebSocket连接和会话")
            
            if self.ws and not self.ws.closed:
                await self.ws.close()
                logger.debug("WebSocket连接已关闭")
                
            if self.session and not self.session.closed:
                await self.session.close()
                logger.debug("会话已关闭")
                
            self.is_connected = False
            logger.info("WebSocket连接和会话清理完成")
            
        except Exception as e:
            logger.error(f"关闭WebSocket连接失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            
    async def check_connection(self):
        """检查连接状态"""
        if not self.is_connected or not self.ws or self.ws.closed:
            return False
            
        try:
            if self.last_pong and time.time() - self.last_pong > 30:
                logger.warning("WebSocket心跳超时")
                return False
                
            await self.ws.ping()
            pong = await self.ws.receive(timeout=5)
            if pong.type in [aiohttp.WSMsgType.PONG, aiohttp.WSMsgType.TEXT]:
                self.last_pong = time.time()
                return True
            return False
        except Exception:
            return False
            
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

class MarketDataManager:
    def __init__(self, websocket_server=None):
        # API configuration
        self.api_key = os.getenv('BINANCE_API_KEY', '')
        self.api_secret = os.getenv('BINANCE_API_SECRET', '')
        self.FUTURES_BASE_URL = 'https://fapi.binance.com/fapi/v1'
        self.FUTURES_WS_URL = 'wss://fstream.binance.com/ws'
        self.base_url = 'https://fapi.binance.com'
        self.ws_url = 'wss://fstream.binance.com'
        self.proxy = 'http://127.0.0.1:4780'
        
        # WebSocket server reference
        self.websocket_server = websocket_server
        
        # WebSocket tasks set
        self.ws_tasks = set()
        
        # Data cache
        self.real_time_cache = {
            'tick': {},  # Tick data cache
            '1m': {},   # 1-minute kline cache
            '5m': {}    # 5-minute kline cache
        }
        
        # Cache limits configuration
        self.CACHE_LIMITS = {
            'tick': 1000,  # 保存最近1000个tick数据
            '1m': 120,     # 保存最近120个1分钟K线
            '5m': 288      # 保存最近288个5分钟K线（24小时）
        }
        
        # Data retention configuration (in seconds)
        self.data_retention = {
            'tick_seconds': 60,        # 实时tick数据保留60秒
            'kline_1m_hours': 2,       # 1分钟K线保留2小时
            'history_days': 15         # 历史数据保留15天
        }
        
        # Latest price cache
        self.latest_prices = {}
        
        # Initialize symbol_stats dictionary
        self.symbol_stats = {}
        
        # Set logger
        self.logger = logger
        
        # Initialization flag
        self._initialized = False
        
        # HTTP session
        self.session = None
        
        # Data directory paths
        self.data_paths = {
            'realtime': 'data/realtime',
            'history': 'data/history',
            'mink': 'data/mink',
            'trades': 'data/trades'
        }
        
        # Subscribed symbols
        self.subscribed_symbols = set()
        
        # Initialize data directories
        self._init_data_directories()
        
    def _init_data_directories(self):
        """初始化数据目录"""
        try:
            # 确保data目录存在
            if not os.path.exists('data'):
                os.makedirs('data')
                logger.info("创建主数据目录: data/")
                
            # 创建所有子目录
            for name, path in self.data_paths.items():
                if not os.path.exists(path):
                    os.makedirs(path)
                    logger.info(f"创建数据子目录: {path}/")
                    
                # 确保目录中有一个空的.gitkeep文件以保持目录结构
                gitkeep_file = os.path.join(path, '.gitkeep')
                if not os.path.exists(gitkeep_file):
                    with open(gitkeep_file, 'w') as f:
                        pass
                    
            logger.info("数据目录初始化完成")
            
        except Exception as e:
            logger.error(f"初始化数据目录失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
        
        # 相关性分析数据
        self.correlation_data = {}  # 用于相关性分析的1h数据
        
        # 数据库配置
        self.db_path = "data/market_data.db"
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # 初始化数据库
        self._init_database()
        
        # 信号阈值配置
        self.signal_thresholds = {
            'tick_change': 0.001,    # 0.1%的秒级价格变化
            'volume_spike': 1.5,     # 1.5倍于平均成交量
            'trend_confirmation': 0.7,  # 70%趋势确认度
            'volatility_threshold': 0.002  # 0.2%的波动率阈值
        }
        
        # Add last_summary_time to control summary output frequency
        self.last_summary_time = 0
        self.SUMMARY_INTERVAL = 5  # Output summary every 5 seconds
        
    def _init_database(self):
        """初始化数据库表"""
        try:
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                
                # 启用WAL模式高写入性能
                cursor.execute('PRAGMA journal_mode=WAL')
                cursor.execute('PRAGMA synchronous=NORMAL')
                cursor.execute('PRAGMA cache_size=10000')
                cursor.execute('PRAGMA temp_store=MEMORY')
                
                # 创建K线数据表
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS kline_data (
                    symbol TEXT,
                    timeframe TEXT,
                    timestamp INTEGER,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    quote_volume REAL,
                    trades INTEGER DEFAULT 0,
                    PRIMARY KEY (symbol, timeframe, timestamp)
                )
                ''')
                
                # ���建交易机会分析表
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS trading_opportunities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER,
                    symbol TEXT,
                    score REAL,
                    price REAL,
                    trend_strength REAL,
                    volatility REAL,
                    volume_ratio REAL,
                    signals TEXT,
                    is_major INTEGER,
                    status TEXT DEFAULT 'new'
                )
                ''')
                
                # 创建索引
                cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_kline_symbol_time 
                ON kline_data(symbol, timestamp)
                ''')
                cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_opportunities_time 
                ON trading_opportunities(timestamp)
                ''')
                
                conn.commit()
                logger.info("数据库��初始化完成")
                
        except Exception as e:
            logger.error(f"初始化数据库失败: {str(e)}")
            raise

    def _table_exists(self, cursor, table_name):
        """检查表是否存在"""
        cursor.execute('''
        SELECT COUNT(*) FROM sqlite_master 
        WHERE type='table' AND name=?
        ''', (table_name,))
        return cursor.fetchone()[0] > 0
        
    async def initialize(self):
        """初始化市场数据管理器"""
        try:
            if self._initialized:
                logger.info("市场数据管理器已初始化过")
                return True
                
            logger.info("开始初始化市场数据管理器")
            
            # 创建HTTP会话
            connector = aiohttp.TCPConnector(
                ssl=False,
                force_close=True,
                enable_cleanup_closed=True
            )
            self.session = aiohttp.ClientSession(connector=connector)
            logger.info("HTTP会话创建成功")
            
            # 获取交易量大的期货合约
            symbols = await self.get_top_volume_symbols()
            if not symbols:
                logger.error("获取交易对失败")
                return False
                
            self.subscribed_symbols = set(symbols)
            logger.info(f"获取到 {len(symbols)} 个交易对: {', '.join(symbols)}")
            
            # 初始化每个交易对的缓存
            for symbol in symbols:
                self.real_time_cache['tick'][symbol] = []
                self.real_time_cache['1m'][symbol] = []
                self.real_time_cache['5m'][symbol] = []
                logger.info(f"初始化 {symbol} 的数据缓存")
                
                # ��查并获取历史数据
                await self._init_symbol_history(symbol)
            
            # 启动价格更新
            if not await self.start_price_updates():
                logger.error("启动价格更新失败")
                return False
                
            # 启动数据完整性检查
            asyncio.create_task(self._periodic_data_check())
            # 启动数据验证任务
            asyncio.create_task(self._verify_data_updates())
            logger.info("启动数据监控任务")
            
            self._initialized = True
            logger.info("市场数据管理器初始化完成")
            return True
            
        except Exception as e:
            logger.error(f"初始化市场数据管理器失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    async def _init_symbol_history(self, symbol: str):
        """初始化单个交易对的历史数据"""
        try:
            # 检查本地文件是否存在
            mink_file = os.path.join(self.data_paths['mink'], f"{symbol}_1m.csv")
            history_file = os.path.join(self.data_paths['history'], f"{symbol}_5m.csv")
            
            # 获取1分钟K线数据
            logger.info(f"获取 {symbol} 的1分钟历史数据...")
            klines_1m = await self.get_klines_data(
                symbol,
                interval='1m',
                limit=120  # 2小时 = 120分钟
            )
            if klines_1m:
                # 转换为DataFrame
                df_1m = pd.DataFrame(klines_1m, columns=[
                    'timestamp',      # 开盘时间
                    'open',          # 开盘价
                    'high',          # 最高价
                    'low',           # 最低价
                    'close',         # 收盘价
                    'volume',        # 成交量
                    'close_time',    # 收盘时间
                    'quote_volume',  # 成交额
                    'trades',        # 成交笔数
                    'taker_buy_volume',  # 主动买入成交量
                    'taker_buy_quote_volume',  # 主动买入成交额
                    'ignore'         # 忽略字段
                ])
                # 只保留需要的列
                df_1m = df_1m[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trades']]
                # 转换时间戳
                df_1m['timestamp'] = pd.to_datetime(df_1m['timestamp'], unit='ms')
                # 确保数值类型正确
                for col in ['open', 'high', 'low', 'close', 'volume', 'quote_volume']:
                    df_1m[col] = pd.to_numeric(df_1m[col], errors='coerce')
                # 保存到CSV
                df_1m.to_csv(mink_file, index=False)
                logger.info(f"保存了 {len(df_1m)} 条1分钟K线数据到 {mink_file}")
            
            # 获取5分钟K线数据
            logger.info(f"获取 {symbol} 的5分钟历史数据...")
            klines_5m = await self.get_klines_data(
                symbol,
                interval='5m',
                limit=288  # 24小时 = 288个5分钟
            )
            if klines_5m:
                # 转换为DataFrame
                df_5m = pd.DataFrame(klines_5m, columns=[
                    'timestamp',      # 开盘时间
                    'open',          # 开盘价
                    'high',          # 最高价
                    'low',           # 最低价
                    'close',         # 收盘价
                    'volume',        # 成交量
                    'close_time',    # 收盘时间
                    'quote_volume',  # 成交额
                    'trades',        # 成交笔数
                    'taker_buy_volume',  # 主动买入成交量
                    'taker_buy_quote_volume',  # 主动买入成交额
                    'ignore'         # 忽略字段
                ])
                # 只保留需要的列
                df_5m = df_5m[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trades']]
                # 转换时间戳
                df_5m['timestamp'] = pd.to_datetime(df_5m['timestamp'], unit='ms')
                # 确保数值类型正确
                for col in ['open', 'high', 'low', 'close', 'volume', 'quote_volume']:
                    df_5m[col] = pd.to_numeric(df_5m[col], errors='coerce')
                # 保存到CSV
                df_5m.to_csv(history_file, index=False)
                logger.info(f"保存了 {len(df_5m)} 条5分钟K线数据到 {history_file}")
                
        except Exception as e:
            logger.error(f"初始化{symbol}历史数据失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    async def _periodic_data_check(self):
        """定期检查数据完整性"""
        while True:
            try:
                await self.check_data_continuity()
                await asyncio.sleep(60)  # 每分钟检查一次
            except Exception as e:
                logger.error(f"定期数据检查失败: {str(e)}")
                await asyncio.sleep(5)  # 出错后等待5秒再试
                
    async def start_price_updates(self):
        """启动价格更新"""
        try:
            # 启动WebSocket连接
            await self._start_websockets()
            
            # 动数据聚合器
            self._start_data_aggregator()
            
            logger.info("启动价格更新成功")
            return True
            
        except Exception as e:
            logger.error(f"启动价格更新失败: {str(e)}")
            return False

    async def stop_price_updates(self):
        """停止价格更新"""
        try:
            # 清理资源
            await self.cleanup()
            logger.info("停止价格更新成功")
            return True
        except Exception as e:
            logger.error(f"停止价格更新失败: {str(e)}")
            return False

    async def _load_correlation_data(self):
        """加载用于相关性分析的1小时K线数据"""
        try:
            current_time = int(time.time() * 1000)
            success_count = 0
            
            for symbol in self.subscribed_symbols:
                # 获取最新的1小时K线数据
                klines = await self.get_klines_data(
                    symbol,
                    interval='1h',
                    limit=500  # 约20天的数据
                )
                
                if klines and len(klines) > 0:
                    # 保存到数据库
                    await self._save_klines_to_db(symbol, '1h', klines)
                    success_count += 1
                    
                    # 更新相关性数据缓存
                    self.correlation_data[symbol] = self._process_klines(klines)
                    
                await asyncio.sleep(0.1)  # 避免请求过快
                
            logger.info(f"成功加载 {success_count} 个交易对的相关性分析数据")
            
        except Exception as e:
            logger.error(f"加载相关性数据失败: {str(e)}")
            
    def _process_klines(self, klines: list) -> dict:
        """处理K线数据为便于分析的格式"""
        try:
            return {
                'timestamps': [k[0] for k in klines],
                'closes': [float(k[4]) for k in klines],
                'volumes': [float(k[5]) for k in klines]
            }
        except Exception as e:
            logger.error(f"处理K线数据失败: {str(e)}")
            return {'timestamps': [], 'closes': [], 'volumes': []}

    async def _start_websockets(self):
        """启动WebSocket连接"""
        try:
            logger.info(f"开始启动WebSocket连接，共 {len(self.subscribed_symbols)} 个交易对")
            
            # 构建所有交易对的streams
            streams = []
            for symbol in self.subscribed_symbols:
                symbol = symbol.lower()
                streams.extend([
                    f"{symbol}@aggTrade",    # 逐笔成交
                    f"{symbol}@kline_1m"     # 1分钟K线
                ])
            
            # 构建单个WebSocket URL，包含所有streams
            ws_url = f"{self.FUTURES_WS_URL}/{'/'.join(streams)}"
            logger.debug(f"WebSocket URL: {ws_url}")
            
            # 创建单个WebSocket连接
            proxy_ws = ProxyWebSocket(self.proxy)
            ws = await proxy_ws.connect(ws_url)
            logger.info("WebSocket连接成功建立")
            
            # 启动连接状态检查任务
            check_task = asyncio.create_task(
                self._monitor_connection(proxy_ws, ws_url, 0, list(self.subscribed_symbols))
            )
            
            # 启动消息处理任务
            process_task = asyncio.create_task(
                self._handle_websocket_messages(ws, 0, list(self.subscribed_symbols))
            )
            
            # 添加任务到集合
            self.ws_tasks.add(check_task)
            self.ws_tasks.add(process_task)
            check_task.add_done_callback(self.ws_tasks.discard)
            process_task.add_done_callback(self.ws_tasks.discard)
            
            logger.info("所有WebSocket任务启动成功")
            
        except Exception as e:
            logger.error(f"启动WebSocket失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    async def _monitor_connection(self, proxy_ws, ws_url, batch_idx, symbols):
        """监控WebSocket连接状态"""
        while True:
            try:
                # 检查连接状态
                is_connected = await proxy_ws.check_connection()
                if not is_connected:
                    logger.warning(f"批次 {batch_idx + 1}: WebSocket连接已断开，尝试重连")
                    
                    # 关闭旧连接
                    await proxy_ws.close()
                    
                    # 重新连接
                    ws = await proxy_ws.connect(ws_url)
                    if ws:
                        logger.info(f"批次 {batch_idx + 1}: WebSocket重连成功")
                        
                        # 动新的消息处理任务
                        process_task = asyncio.create_task(
                            self._handle_websocket_messages(ws, batch_idx, symbols)
                        )
                        self.ws_tasks.add(process_task)
                        process_task.add_done_callback(self.ws_tasks.discard)
                
                await asyncio.sleep(10)  # 每10秒检查一次连接状态
                
            except asyncio.CancelledError:
                logger.info(f"批次 {batch_idx + 1}: 连接监控任务被取消")
                break
            except Exception as e:
                logger.error(f"批次 {batch_idx + 1}: 连接监控错误: {str(e)}")
                await asyncio.sleep(5)  # 出错后等待5秒再继续

    async def _handle_websocket_messages(self, ws, batch_idx: int, symbols: list):
        """处理WebSocket消息"""
        try:
            logger.info(f"开始处理批次 {batch_idx} 的WebSocket消息，交易对: {', '.join(symbols)}")
            message_queue = asyncio.Queue()
            process_task = None
            
            # 记录每个交易对的最后更新时间
            last_update_time = {symbol: 0 for symbol in symbols}
            last_frontend_update = 0
            FRONTEND_UPDATE_INTERVAL = 1  # 每秒更新一次前端
            
            async def process_messages():
                while True:
                    try:
                        message = await message_queue.get()
                        if message is None:  # 停止信号
                            break
                            
                        if isinstance(message, dict):
                            if 'e' in message:
                                symbol = message.get('s', '')
                                
                                if symbol in symbols:
                                    current_time = int(time.time() * 1000)
                                    last_update_time[symbol] = current_time
                                    
                                    if message['e'] == 'aggTrade':
                                        await self._process_trade(message)
                                    elif message['e'] == 'kline':
                                        await self._process_kline(message)
                                        
                                # 检查是否需要更新前端
                                nonlocal last_frontend_update
                                if current_time - last_frontend_update >= FRONTEND_UPDATE_INTERVAL * 1000:
                                    market_summary = self.get_market_summary()
                                    if market_summary:
                                        await self._send_to_frontend(market_summary)
                                    last_frontend_update = current_time
                                    
                            # 每30秒检查一次数据更新状态
                            if time.time() % 30 == 0:
                                current_time = int(time.time() * 1000)
                                for symbol, last_time in last_update_time.items():
                                    if current_time - last_time > 60000:  # 1分钟没有更新
                                        logger.warning(f"{symbol} 数据超过1分钟未更新")
                                        
                    except Exception as e:
                        logger.error(f"理消息失败: {str(e)}")
                    finally:
                        message_queue.task_done()
            
            # 启动消息处理任务
            process_task = asyncio.create_task(process_messages())
            
            try:
                async for message in ws:
                    if message.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(message.data)
                            await message_queue.put(data)
                        except json.JSONDecodeError:
                            logger.warning(f"无效的JSON消息: {message.data}")
                            continue
                    elif message.type == aiohttp.WSMsgType.CLOSED:
                        logger.warning(f"批次 {batch_idx}: WebSocket连接关闭")
                        break
                    elif message.type == aiohttp.WSMsgType.ERROR:
                        logger.error(f"批次 {batch_idx}: WebSocket错��")
                        break
                        
            except asyncio.CancelledError:
                logger.info(f"批次 {batch_idx}: WebSocket任务被取消")
                
            except Exception as e:
                logger.error(f"处理WebSocket消息失败: {str(e)}")
                
            finally:
                # 发送停止信号并等��处理任务完成
                await message_queue.put(None)
                if process_task:
                    await process_task
                await ws.close()
                logger.info(f"批次 {batch_idx} 的WebSocket连接已关闭")
                
        except Exception as e:
            logger.error(f"WebSocket消息处理器失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    async def _process_trade(self, data: dict):
        """处理逐笔成交数据"""
        try:
            # 验证数据完整性
            required_fields = ['s', 'T', 'p', 'q']
            if not all(field in data for field in required_fields):
                logger.warning(f"成交数据缺少必要字段: {data}")
                return
                
            # 验证数据类型
            try:
                symbol = str(data['s'])
                timestamp = int(data['T'])
                price = float(data['p'])
                quantity = float(data['q'])
            except (ValueError, TypeError) as e:
                logger.error(f"成交数据类型转换失败: {str(e)}")
                return
                
            # 保存到realtime目录
            await self._save_realtime_trade(symbol, {
                'timestamp': timestamp,
                'price': price,
                'quantity': quantity
            })
            
            # 更新最新价格缓存
            if symbol not in self.latest_prices:
                self.latest_prices[symbol] = {}
                
            self.latest_prices[symbol].update({
                'price': price,
                'volume': quantity,
                'timestamp': timestamp,
                'symbol': symbol
            })
            
            # 更新tick缓存
            if symbol not in self.real_time_cache['tick']:
                self.real_time_cache['tick'][symbol] = []
                
            self.real_time_cache['tick'][symbol].append({
                'timestamp': timestamp,
                'price': price,
                'quantity': quantity
            })
            
            # 维护缓存大小
            if len(self.real_time_cache['tick'][symbol]) > self.CACHE_LIMITS['tick']:
                self.real_time_cache['tick'][symbol] = \
                    self.real_time_cache['tick'][symbol][-self.CACHE_LIMITS['tick']:]
                    
        except Exception as e:
            logger.error(f"处理成交数据失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    async def _save_realtime_trade(self, symbol: str, trade_data: dict):
        """保存实时成交数据到CSV文件"""
        try:
            file_path = os.path.join(self.data_paths['trades'], f"{symbol}_trades.csv")
            
            # 准备新数据
            new_data = pd.DataFrame([{
                'timestamp': trade_data['timestamp'],
                'price': float(trade_data['price']),
                'quantity': float(trade_data['quantity']),
                'buyer_maker': trade_data.get('buyer_maker', False),
                'trade_id': trade_data.get('trade_id', ''),
            }])
            
            # 检查文件是否存在
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                try:
                    # 读取现有数据
                    df = pd.read_csv(file_path)
                    # 合并数据
                    df = pd.concat([df, new_data], ignore_index=True)
                except pd.errors.EmptyDataError:
                    # 如果文件为空，直接使用新数据
                    df = new_data
            else:
                # 如果文件不存在或为空，直接使用新数据
                df = new_data
            
            # 保存数据
            df.to_csv(file_path, index=False)
            
        except Exception as e:
            logger.error(f"保存实时成交数据失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    def _cleanup_realtime_data(self, symbol: str):
        """清理过期的实时数据"""
        try:
            import pandas as pd
            from datetime import datetime, timedelta
            
            file_path = os.path.join(self.data_paths['realtime'], f"{symbol}_trades.csv")
            if not os.path.exists(file_path):
                return
            
            # 读取CSV文件
            df = pd.read_csv(file_path)
            if len(df) == 0:
                return
            
            # 只保留最近60秒的数据
            current_time = int(time.time() * 1000)
            cutoff_time = current_time - (self.data_retention['tick_seconds'] * 1000)
            df = df[df['timestamp'] >= cutoff_time]
            
            # 保存回文件
            df.to_csv(file_path, index=False)
            
        except Exception as e:
            logger.error(f"清理实时数据失败: {str(e)}")

    async def _process_kline(self, data: dict):
        """处理K线数据"""
        try:
            k = data['k']
            symbol = data['s']
            
            # 验证数据完整性
            required_fields = ['t', 'o', 'h', 'l', 'c', 'v', 'q', 'n', 'x']
            if not all(field in k for field in required_fields):
                logger.warning(f"K线数据缺少必要字段: {symbol}")
                return
                
            # 验证数据类型
            try:
                kline_data = {
                    'timestamp': int(k['t']),
                    'open': float(k['o']),
                    'high': float(k['h']),
                    'low': float(k['l']),
                    'close': float(k['c']),
                    'volume': float(k['v']),
                    'quote_volume': float(k['q']),
                    'trades': int(k['n']),
                    'closed': bool(k['x'])
                }
            except (ValueError, TypeError) as e:
                logger.error(f"K线数据类型转换失败: {symbol} - {str(e)}")
                return
            
            # 更新最新价格缓存
            if symbol not in self.latest_prices:
                self.latest_prices[symbol] = {}
                
            self.latest_prices[symbol].update({
                'price': float(k['c']),
                'open': float(k['o']),
                'high': float(k['h']),
                'low': float(k['l']),
                'volume': float(k['v']),
                'quote_volume': float(k['q']),
                'trades': int(k['n']),
                'timestamp': int(k['t']),
                'symbol': symbol
            })
            
            # 更新1分钟K线缓存
            if symbol not in self.real_time_cache['1m']:
                self.real_time_cache['1m'][symbol] = []
                
            # 更新或添加K线数据
            existing_index = None
            for i, existing_kline in enumerate(self.real_time_cache['1m'][symbol]):
                if existing_kline['timestamp'] == kline_data['timestamp']:
                    existing_index = i
                    break
                
            if existing_index is not None:
                self.real_time_cache['1m'][symbol][existing_index] = kline_data
            else:
                self.real_time_cache['1m'][symbol].append(kline_data)
                
            # 维护缓存大小
            if len(self.real_time_cache['1m'][symbol]) > self.CACHE_LIMITS['1m']:
                self.real_time_cache['1m'][symbol] = \
                    self.real_time_cache['1m'][symbol][-self.CACHE_LIMITS['1m']:]
                
            # 如果是已完成的K线，尝试更新5分钟K线
            if kline_data['closed']:
                await self._update_5m_kline(symbol)
                
        except Exception as e:
            logger.error(f"处理K线数据失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    async def _save_1m_kline(self, symbol: str, kline_data: dict):
        """保存1分钟K线数据到minK目录"""
        try:
            import pandas as pd
            file_path = os.path.join(self.data_paths['mink'], f"{symbol}_1m.csv")
            
            # 创建新的DataFrame
            new_data = pd.DataFrame([{
                'timestamp': pd.to_datetime(kline_data['timestamp'], unit='ms'),
                'open': kline_data['open'],
                'high': kline_data['high'],
                'low': kline_data['low'],
                'close': kline_data['close'],
                'volume': kline_data['volume'],
                'quote_volume': kline_data['quote_volume'],
                'trades': kline_data['trades']
            }])
            
            # 读取现有数据或创建新文件
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = pd.concat([df, new_data], ignore_index=True)
            else:
                df = new_data
                
            # 删除重复数据
            df = df.drop_duplicates(subset=['timestamp'], keep='last')
            
            # 只保留最近1小时的数据
            cutoff_time = pd.Timestamp.now() - pd.Timedelta(hours=self.data_retention['kline_1m_hours'])
            df = df[df['timestamp'] >= cutoff_time]
            
            # 按时间排序
            df = df.sort_values('timestamp')
            
            # 保存到CSV
            df.to_csv(file_path, index=False)
            
        except Exception as e:
            logger.error(f"保存1分钟K线数据失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    async def _update_5m_kline(self, symbol: str):
        """更新5分钟K线数据"""
        try:
            if symbol not in self.real_time_cache['1m'] or len(self.real_time_cache['1m'][symbol]) < 5:
                return
                
            # 获取最新的1分钟K线时间戳
            latest_1m = self.real_time_cache['1m'][symbol][-1]['timestamp']
            
            # 计算5分钟时间戳
            latest_5m = (latest_1m // (5 * 60 * 1000)) * (5 * 60 * 1000)
            
            # 如果最新的1分钟K线不是5分钟的整数倍，直接返回
            if latest_1m % (5 * 60 * 1000) != 0:
                return
                
            # 获取最近5根1分钟K线
            recent_1m_klines = []
            for k in reversed(self.real_time_cache['1m'][symbol]):
                if k['timestamp'] > latest_5m - 5 * 60 * 1000:
                    recent_1m_klines.append(k)
                else:
                    break
                
            if len(recent_1m_klines) < 5:
                return
                
            # 生成5分钟K线数据
            kline_5m = {
                'timestamp': latest_5m,
                'open': recent_1m_klines[-1]['open'],
                'high': max(k['high'] for k in recent_1m_klines),
                'low': min(k['low'] for k in recent_1m_klines),
                'close': recent_1m_klines[0]['close'],
                'volume': sum(k['volume'] for k in recent_1m_klines),
                'quote_volume': sum(k['quote_volume'] for k in recent_1m_klines)
            }
            
            # 保存到history目录
            await self._save_5m_kline_to_csv(symbol, kline_5m)
            
            # 更新5分钟缓存
            if symbol not in self.real_time_cache['5m']:
                self.real_time_cache['5m'][symbol] = []
                
            # 更新或添加K线数据
            existing_index = None
            for i, k in enumerate(self.real_time_cache['5m'][symbol]):
                if k['timestamp'] == latest_5m:
                    existing_index = i
                    break
                
            if existing_index is not None:
                self.real_time_cache['5m'][symbol][existing_index] = kline_5m
            else:
                self.real_time_cache['5m'][symbol].append(kline_5m)
                
            # 维护���存大小
            if len(self.real_time_cache['5m'][symbol]) > self.CACHE_LIMITS['5m']:
                self.real_time_cache['5m'][symbol] = \
                    self.real_time_cache['5m'][symbol][-self.CACHE_LIMITS['5m']:]
                
        except Exception as e:
            logger.error(f"更新5分钟K线失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    async def _save_5m_kline_to_csv(self, symbol: str, kline_data: dict):
        """保存5分钟K线数据到history目录"""
        try:
            import pandas as pd
            from datetime import datetime, timedelta
            
            file_path = os.path.join(self.data_paths['history'], f"{symbol}_5m.csv")
            
            # 创建新的DataFrame
            new_data = pd.DataFrame([{
                'timestamp': pd.to_datetime(kline_data['timestamp'], unit='ms'),
                'open': kline_data['open'],
                'high': kline_data['high'],
                'low': kline_data['low'],
                'close': kline_data['close'],
                'volume': kline_data['volume'],
                'quote_volume': kline_data['quote_volume']
            }])
            
            # 读取现有数据或创建新文件
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = pd.concat([df, new_data], ignore_index=True)
            else:
                df = new_data
                
            # 删除重复数据
            df = df.drop_duplicates(subset=['timestamp'], keep='last')
            
            # 只保留最近15天的数据
            cutoff_date = datetime.now() - timedelta(days=self.data_retention['history_days'])
            df = df[df['timestamp'] >= cutoff_date]
            
            # 按时间排序
            df = df.sort_values('timestamp')
            
            # 保存到CSV
            df.to_csv(file_path, index=False)
            logger.debug(f"已保存 {symbol} 的5分钟K线数据到CSV")
            
        except Exception as e:
            logger.error(f"保存5分钟K线数据到CSV失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    async def _load_recent_klines(self, symbol: str):
        """从数据库加载最新的K线数据"""
        try:
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                # 获最近的K线数据
                cursor.execute('''
                SELECT timestamp, open, high, low, close, volume, quote_volume,
                       COALESCE(trades, 0) as trades
                FROM kline_data
                WHERE symbol = ? AND timeframe = '1m'
                ORDER BY timestamp DESC
                LIMIT ?
                ''', (symbol, self.CACHE_LIMITS['1m']))
                
                rows = cursor.fetchall()
                if rows:
                    # 转换为缓存格式
                    klines = [{
                        'timestamp': row[0],
                        'open': row[1],
                        'high': row[2],
                        'low': row[3],
                        'close': row[4],
                        'volume': row[5],
                        'quote_volume': row[6],
                        'trades': row[7],
                        'closed': True
                    } for row in rows]
                    
                    # 更新缓存
                    self.real_time_cache['1m'][symbol] = sorted(klines, key=lambda x: x['timestamp'])
                    logger.info(f"从数据库加载了 {len(klines)} 条 {symbol} 的历史K线数据")
                    
        except Exception as e:
            logger.error(f"加载历史K线数据失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            
    def _start_data_aggregator(self):
        """启动数据聚合器"""
        async def _aggregate_loop():
            logger.info("启动数据聚合循环")
            last_check = {}  # 记录每个交易对的最后检查时间
            
            while True:
                try:
                    current_time = int(time.time() * 1000)
                    
                    # 遍历所有交易对
                    for symbol in self.subscribed_symbols:
                        try:
                            # 检查是否需要更新
                            if symbol in last_check and current_time - last_check[symbol] < 60000:  # 1分钟
                                continue
                                
                            # 更新5分钟K线
                            await self._update_5m_kline(symbol)
                            
                            # 检查数据完整性
                            if symbol in self.real_time_cache['1m']:
                                klines = self.real_time_cache['1m'][symbol]
                                if len(klines) >= 2:
                                    latest = klines[-1]
                                    prev = klines[-2]
                                    time_diff = latest['timestamp'] - prev['timestamp']
                                    if time_diff > 60000:  # 超过1分钟
                                        logger.warning(f"{symbol} 数据可能不连续: {time_diff/1000:.1f}秒")
                                        
                            # 更新检查时间
                            last_check[symbol] = current_time
                            
                        except Exception as e:
                            logger.error(f"处理 {symbol} 的数据聚合失败: {str(e)}")
                            continue
                            
                    # 清理过期数据
                    await self._cleanup_old_data()
                    
                    # 等待一次更新
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"数据聚合循环误: {str(e)}")
                    import traceback
                    logger.error(traceback.format_exc())
                    await asyncio.sleep(5)  # 出错后等待5秒
                    
        # 创建聚合任务
        asyncio.create_task(_aggregate_loop())
        
    async def _cleanup_old_data(self):
        """清理过期数据"""
        try:
            current_time = int(time.time() * 1000)
            
            for symbol in list(self.real_time_cache['tick'].keys()):
                try:
                    # 清理超过1分钟的tick数据
                    self.real_time_cache['tick'][symbol] = [
                        tick for tick in self.real_time_cache['tick'][symbol]
                        if current_time - tick['timestamp'] <= 60000
                    ]
                except Exception as e:
                    logger.error(f"清理 {symbol} 的tick数据失败: {str(e)}")
                    continue
                    
            for symbol in list(self.real_time_cache['1m'].keys()):
                try:
                    # 清理超过5小的1分钟数据
                    self.real_time_cache['1m'][symbol] = [
                        kline for kline in self.real_time_cache['1m'][symbol]
                        if current_time - kline['timestamp'] <= 5 * 60 * 60 * 1000
                    ]
                except Exception as e:
                    logger.error(f"清理 {symbol} 的1分钟数据失败: {str(e)}")
                    continue
                    
            for symbol in list(self.real_time_cache['5m'].keys()):
                try:
                    # 清理超过24小时的5分钟数据
                    self.real_time_cache['5m'][symbol] = [
                        kline for kline in self.real_time_cache['5m'][symbol]
                        if current_time - kline['timestamp'] <= 24 * 60 * 60 * 1000
                    ]
                except Exception as e:
                    logger.error(f"清理 {symbol} 的5分钟数据失败: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"清理过期数据失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    async def _verify_data_updates(self):
        """验证所有交易对的数据更新状态"""
        while True:
            try:
                current_time = int(time.time() * 1000)
                missing_data = []
                
                # 检查每个交易对的数据
                for symbol in self.subscribed_symbols:
                    # 检查实时数据
                    if symbol not in self.real_time_cache['tick'] or not self.real_time_cache['tick'][symbol]:
                        missing_data.append(f"{symbol} (tick)")
                    
                    # 检查1分钟K线数据
                    if symbol not in self.real_time_cache['1m'] or not self.real_time_cache['1m'][symbol]:
                        missing_data.append(f"{symbol} (1m)")
                    
                    # 检查5分钟K线数据
                    if symbol not in self.real_time_cache['5m'] or not self.real_time_cache['5m'][symbol]:
                        missing_data.append(f"{symbol} (5m)")
                    
                    # 检查最新价格缓存
                    if symbol not in self.latest_prices:
                        missing_data.append(f"{symbol} (latest)")
                    elif current_time - self.latest_prices[symbol].get('timestamp', 0) > 60000:
                        missing_data.append(f"{symbol} (stale)")
                
                if missing_data:
                    logger.warning(f"以下交易对数据缺失或过期: {', '.join(missing_data)}")
                    # 尝试重新订阅这些交易对的数据
                    await self._resubscribe_missing_data(missing_data)
                else:
                    logger.info("所有交易对数据更新正常")
                
                # 每分钟检查一次
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"验证数据更新状态失败: {str(e)}")
                await asyncio.sleep(5)

    async def _resubscribe_missing_data(self, missing_data: List[str]):
        """重新订阅缺失的数据"""
        try:
            # 提取需要重新订阅的交易对
            symbols_to_resubscribe = set()
            for item in missing_data:
                symbol = item.split(' ')[0]  # 提取交易对名称
                symbols_to_resubscribe.add(symbol)
            
            if symbols_to_resubscribe:
                logger.info(f"尝试重新订阅以下交易对的数据: {', '.join(symbols_to_resubscribe)}")
                # 重新初始化这些交���对的缓存
                for symbol in symbols_to_resubscribe:
                    self.real_time_cache['tick'][symbol] = []
                    self.real_time_cache['1m'][symbol] = []
                    self.real_time_cache['5m'][symbol] = []
                
                # 重新启动WebSocket连接
                await self._start_websockets()
                
        except Exception as e:
            logger.error(f"重新订阅数据失败: {str(e)}")

    async def cleanup(self):
        """清理资源"""
        try:
            # 取消所有WebSocket任务
            for task in self.ws_tasks:
                if not task.done():
                    task.cancel()
            if self.ws_tasks:
                await asyncio.gather(*self.ws_tasks, return_exceptions=True)
            self.ws_tasks.clear()
            
            # 只清理内存缓存
            logger.info("清理内存缓存数据...")
            self.real_time_cache['tick'] = {}
            self.latest_prices = {}
            self.real_time_cache['1m'] = {}
            self.real_time_cache['5m'] = {}
            
            # 关闭HTTP会话
            if self.session and not self.session.closed:
                await self.session.close()
                self.session = None
                
            logger.info("资源清理完成：已清理存缓存，本地文件保持不变")
            
        except Exception as e:
            logger.error(f"资源清理失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    async def get_klines_data(self, symbol: str, interval='1h', limit=500, 
                           start_time=None, end_time=None) -> Optional[List]:
        """获取K线数据
        
        Args:
            symbol: 交易对
            interval: K线间隔
            limit: 获取数
            start_time: 开始时间（毫秒）
            end_time: 结束时间（毫秒）
        """
        MAX_RETRIES = 3
        RETRY_DELAY = 2
        
        for retry in range(MAX_RETRIES):
            try:
                endpoint = f"{self.FUTURES_BASE_URL}/klines"
                params = {
                    "symbol": symbol,
                    "interval": interval,
                    "limit": limit
                }
                
                if start_time:
                    params["startTime"] = start_time
                if end_time:
                    params["endTime"] = end_time
                    
                timeout = aiohttp.ClientTimeout(total=10, connect=5)
                async with self.session.get(
                    endpoint,
                    params=params,
                    proxy=self.proxy,
                    ssl=False,
                    timeout=timeout
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        if isinstance(data, list) and len(data) > 0:
                            return data
                        
                    logger.warning(f"获取K线数据失败 (尝试 {retry + 1}/{MAX_RETRIES}): {await response.text()}")
                    
            except asyncio.TimeoutError:
                logger.warning(f"获取K线数据超时 (尝试 {retry + 1}/{MAX_RETRIES})")
            except Exception as e:
                logger.warning(f"获取K线数据失败 (尝试 {retry + 1}/{MAX_RETRIES}): {str(e)}")
                
            if retry < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY * (retry + 1))
                
        logger.error(f"获取K线数据失败: 重试{MAX_RETRIES}次后仍然失败")
        return None

    async def get_top_volume_symbols(self, limit: int = 20) -> List[str]:
        """获取交易量最大的期货合约"""
        try:
            endpoint = f"{self.FUTURES_BASE_URL}/ticker/24hr"
            
            # 设置代���
            proxy = 'http://127.0.0.1:4780'
            
            async with self.session.get(endpoint, proxy=proxy) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # 过滤USDT永续合约并按成交量排序
                    usdt_pairs = [
                        item for item in data 
                        if item['symbol'].endswith('USDT')
                    ]
                    
                    sorted_pairs = sorted(
                        usdt_pairs,
                        key=lambda x: float(x['quoteVolume']),
                        reverse=True
                    )
                    
                    # 获取前limit个交易对
                    top_symbols = [pair['symbol'] for pair in sorted_pairs[:limit]]
                    logger.info(f"获取到{len(top_symbols)}个高交易量合约")
                    return top_symbols
                else:
                    logger.error(f"获取交易对失败: {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"获取高交易量合约失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    def _get_symbol_characteristics(self, symbol: str) -> dict:
        """获取交易对特性，用于调整分析参数"""
        try:
            # 严格定义主流币
            is_major = symbol in ['BTCUSDT', 'ETHUSDT']
            
            # 设置不同币种的最大杠杆
            max_leverage = {
                'BTCUSDT': 125,  # BTC最大125倍
                'ETHUSDT': 100,  # ETH最大100倍
                'default': 75    # 其他币种默认75倍
            }.get(symbol, 75)
            
            # 从缓存的K线数据计算特性
            if symbol in self.real_time_cache['1m']:
                klines = self.real_time_cache['1m'][symbol]
                if len(klines) > 0:
                    volumes = [float(k['volume']) for k in klines]
                    prices = [float(k['close']) for k in klines]
                    
                    avg_volume = np.mean(volumes)
                    price_volatility = np.std(prices) / np.mean(prices)
                    
                    self.symbol_stats[symbol] = {
                        'avg_volume': avg_volume,
                        'price_volatility': price_volatility,
                        'is_major': is_major,
                        'update_time': time.time(),
                        'initial_leverage': 20,  # 初始杠杆统一20倍
                        'max_leverage': max_leverage
                    }
            
            return self.symbol_stats.get(symbol, {
                'avg_volume': 0,
                'price_volatility': 0,
                'is_major': is_major,
                'update_time': 0,
                'initial_leverage': 20,
                'max_leverage': max_leverage
            })
            
        except Exception as e:
            logger.error(f"获取交易对特性失败: {str(e)}")
            return {
                'avg_volume': 0,
                'price_volatility': 0,
                'is_major': symbol in ['BTCUSDT', 'ETHUSDT'],
                'update_time': 0,
                'initial_leverage': 20,
                'max_leverage': max_leverage
            }

    def calculate_volatility(self, symbol: str) -> float:
        """��算波动率，根据币种特性动态调整参数"""
        try:
            if symbol not in self.real_time_cache['1m']:
                return 0.0
                
            klines = self.real_time_cache['1m'][symbol]
            if len(klines) < 2:
                return 0.0
                
            # 获取币种特性
            characteristics = self._get_symbol_characteristics(symbol)
            
            # 根据币种特性动态调整计算周期
            if characteristics['is_major']:
                window_size = 20  # 主流币使用更长周
            else:
                # 根据波动性调整窗口大小
                base_window = 10
                volatility_factor = min(max(characteristics['price_volatility'] * 10, 0.5), 2)
                window_size = int(base_window * volatility_factor)
            
            # 确保窗口大小合理
            window_size = min(max(window_size, 5), len(klines))
            
            # 计算波动率
            recent_klines = klines[-window_size:]
            returns = []
            
            for i in range(1, len(recent_klines)):
                prev_close = float(recent_klines[i-1]['close'])
                curr_close = float(recent_klines[i]['close'])
                if prev_close > 0:  # 避免除以零
                    returns.append((curr_close - prev_close) / prev_close)
                
            if not returns:
                return 0.0
                
            # 计算标差作为波动率
            volatility = float(np.std(returns))
            
            # 根据交易调整波动率权重
            volume_factor = min(max(characteristics['avg_volume'] / 1000000, 0.5), 2)
            adjusted_volatility = volatility * volume_factor
            
            return float(adjusted_volatility)
            
        except Exception as e:
            logger.error(f"计算波动率失败: {str(e)}")
            return 0.0

    def calculate_trend_strength(self, symbol: str) -> float:
        """计算趋势强度，根据���种特性动态调整参数"""
        try:
            if symbol not in self.real_time_cache['1m']:
                return 0.0
                
            klines = self.real_time_cache['1m'][symbol]
            if len(klines) < 20:
                return 0.0
                
            # 获取币种特性
            characteristics = self._get_symbol_characteristics(symbol)
            
            # 根据币种特性动态调整计算参数
            if characteristics['is_major']:
                # 主流币使用更长的均线周期
                ma_periods = [10, 20, 40]
            else:
                # 小币种使用更短的均线周期
                volatility_factor = min(max(characteristics['price_volatility'] * 10, 0.5), 2)
                base_period = 5
                ma_periods = [
                    int(base_period * volatility_factor),
                    int(base_period * 2 * volatility_factor),
                    int(base_period * 4 * volatility_factor)
                ]
            
            # 确保周期有效
            ma_periods = [min(max(p, 5), len(klines)) for p in ma_periods]
            
            # 获取收盘价
            closes = [float(k['close']) for k in klines]
            
            # 计算多个周期的移动平均线
            sma_values = [np.mean(closes[-p:]) for p in ma_periods]
            
            # 趋势得分计算
            trend_score = 0
            
            # 上升趋势
            if all(sma_values[i] > sma_values[i+1] for i in range(len(sma_values)-1)):
                trend_score = 1
            # 下降趋势
            elif all(sma_values[i] < sma_values[i+1] for i in range(len(sma_values)-1)):
                trend_score = -1
                
            # 计算价格相对位置
            current_price = closes[-1]
            price_position = (current_price - sma_values[-1]) / sma_values[-1]
            
            # 根据交易量调整趋势强度
            volume_factor = min(max(characteristics['avg_volume'] / 1000000, 0.5), 2)
            
            # 综合计算趋势强度
            trend_strength = trend_score * (1 + abs(price_position)) * volume_factor
            
            return float(trend_strength)
            
        except Exception as e:
            logger.error(f"计算趋势强���失败: {str(e)}")
            return 0.0

    def get_market_summary(self) -> Dict[str, Any]:
        """获取市场数据摘要"""
        try:
            current_timestamp = int(time.time() * 1000)
            market_data = []
            missing_symbols = []

            for symbol in self.subscribed_symbols:
                if symbol not in self.latest_prices:
                    missing_symbols.append(symbol)
                    continue

                latest_data = self.latest_prices[symbol]
                
                # 计算交易对得分
                score = self._calculate_symbol_score(symbol)
                
                market_data.append({
                    'symbol': symbol,
                    'price': float(latest_data.get('price', 0)),
                    'price_change': float(latest_data.get('price_change', 0)),
                    'volume': float(latest_data.get('volume', 0)),
                    'trades': int(latest_data.get('trades', 0)),
                    'updates': int(latest_data.get('updates', 0)),
                    'high': float(latest_data.get('high', 0)),
                    'low': float(latest_data.get('low', 0)),
                    'quote_volume': float(latest_data.get('quote_volume', 0)),
                    'tick_count': int(latest_data.get('tick_count', 0)),
                    'trend_strength': float(latest_data.get('trend_strength', 0)),
                    'volatility': float(latest_data.get('volatility', 0)),
                    'rsi': float(latest_data.get('rsi', 0)),
                    'score': score,  # 添加综合得分
                    'timestamp': current_timestamp
                })

            if missing_symbols:
                logger.warning(f"数据更新时间: {datetime.fromtimestamp(current_timestamp/1000)} - 以下交易对数据缺失: {', '.join(missing_symbols)}")
            else:
                logger.info(f"数据更新时间: {datetime.fromtimestamp(current_timestamp/1000)} - 全部数据正常")

            return market_data

        except Exception as e:
            logger.error(f"获取市场数据摘要失败: {str(e)}")
            return []

    def _calculate_symbol_score(self, symbol: str) -> float:
        """计算交易对的综合得分"""
        try:
            if symbol not in self.latest_prices:
                return 0.0

            data = self.latest_prices[symbol]
            score = 50.0  # 基础分数

            # 1. 趋势强度评分 (最高30分)
            trend_strength = float(data.get('trend_strength', 0))
            trend_score = abs(trend_strength) * 30
            score += min(trend_score, 30)

            # 2. 波动率评分 (最高20分)
            volatility = float(data.get('volatility', 0))
            if 0.005 <= volatility <= 0.02:  # 适中的波动率
                vol_score = 20
            elif volatility > 0.02:  # 过高的波动率
                vol_score = 10
            else:  # 过低的波动率
                vol_score = 5
            score += vol_score

            # 3. RSI评分 (最高20分)
            rsi = float(data.get('rsi', 50))
            if 40 <= rsi <= 60:  # 中性区域
                score += 20
            elif (30 <= rsi < 40) or (60 < rsi <= 70):  # 接近超买超卖
                score += 15
            elif rsi < 30 or rsi > 70:  # 超买超卖区域
                score += 10

            # 4. 成交量评分 (最高30分)
            volume_change = float(data.get('volume_change', 0))
            if volume_change > 0:
                volume_score = min(volume_change * 30, 30)
                score += volume_score

            # 确保分数在0-100之间
            return max(0, min(100, score))

        except Exception as e:
            logger.error(f"计算{symbol}得分失败: {str(e)}")
            return 0.0

    def get_price_data(self, symbol: str) -> pd.DataFrame:
        """获取价格数据
        
        Args:
            symbol: 交易对符号
            
        Returns:
            包含价格和成交量数据的DataFrame
        """
        try:
            if symbol not in self.real_time_cache['1m']:
                return pd.DataFrame()
                
            data = self.real_time_cache['1m'][symbol]
            if not data:
                return pd.DataFrame()
                
            df = pd.DataFrame(data)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            return df
            
        except Exception as e:
            logger.error(f"获取价格数据失败: {str(e)}")
            return pd.DataFrame()

    def check_database_status(self):
        """检查数据库状态"""
        try:
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                
                # 获取所有交易对的数据统计
                cursor.execute('''
                SELECT symbol, timeframe, 
                       COUNT(*) as count,
                       MIN(timestamp) as min_time,
                       MAX(timestamp) as max_time
                FROM kline_data
                GROUP BY symbol, timeframe
                ''')
                
                stats = cursor.fetchall()
                logger.info("数据库状态:")
                logger.info("=" * 50)
                
                for stat in stats:
                    symbol, timeframe, count, min_time, max_time = stat
                    min_date = datetime.fromtimestamp(min_time/1000)
                    max_date = datetime.fromtimestamp(max_time/1000)
                    logger.info(f"交易对: {symbol}")
                    logger.info(f"  时间周期: {timeframe}")
                    logger.info(f"  数据数: {count}")
                    logger.info(f"  时间范围: {min_date} 至 {max_date}")
                    logger.info("-" * 30)
                
                # 获取数据库文件大小
                cursor.execute("PRAGMA page_size")
                page_size = cursor.fetchone()[0]
                cursor.execute("PRAGMA page_count")
                page_count = cursor.fetchone()[0]
                db_size = page_size * page_count / (1024 * 1024)  # 转换为MB
                
                logger.info(f"数据库大小: {db_size:.2f}MB")
                logger.info("=" * 50)
                
                return True
                
        except Exception as e:
            logger.error(f"检查数据库状态失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    async def check_data_continuity(self):
        """检查数据连续性并补充缺失数据"""
        try:
            current_time = int(time.time() * 1000)
            one_minute = 60 * 1000  # 1分钟的毫秒数
            
            for symbol in self.subscribed_symbols:
                # 获取最新的数据时间戳
                with self._get_db_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                    SELECT MAX(timestamp) FROM kline_data 
                    WHERE symbol = ? AND timeframe = '1m'
                    ''', (symbol,))
                    last_timestamp = cursor.fetchone()[0]
                    
                    if last_timestamp:
                        # 如果最新数据时间与当前时间差距超过2分钟，获取失的数据
                        if current_time - last_timestamp > 2 * one_minute:
                            logger.info(f"检测到 {symbol} 的数据缺失，开始补充...")
                            # 获取缺失的数据
                            klines = await self.get_klines_data(
                                symbol,
                                interval='1m',
                                start_time=last_timestamp + one_minute,
                                limit=500
                            )
                            
                            if klines:
                                # 保存补充的数据
                                await self._save_klines_to_db(symbol, '1m', klines)
                                logger.info(f"已补充 {symbol} 的 {len(klines)} 条缺失数据")
                                
            logger.info("数据完整性检查完成")
            
        except Exception as e:
            logger.error(f"数据完整性检查失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    def calculate_stop_loss(self, symbol: str, entry_price: float, direction: str = 'long') -> dict:
        """计算止损价格和风险比率
        
        Args:
            symbol: 交易对
            entry_price: 入场价格
            direction: 交易方向 ('long' 或 'short')
            
        Returns:
            dict: 包含止损价格和风险比率的字典
        """
        try:
            if symbol not in self.real_time_cache['1m']:
                return {'stop_loss': 0, 'risk_ratio': 0}
                
            klines = self.real_time_cache['1m'][symbol]
            if len(klines) < 20:  # 至少需要20根K线
                return {'stop_loss': 0, 'risk_ratio': 0}
                
            # 获取币种特性
            characteristics = self._get_symbol_characteristics(symbol)
            volatility = self.calculate_volatility(symbol)
            
            # 根据波动率和币种特性动态调整止损比例
            if characteristics['is_major']:
                # 主流币种使用较小的止损比例
                base_stop_percentage = 0.01  # 1%
            else:
                # 小币种使用较的止损比例
                base_stop_percentage = 0.02  # 2%
                
            # 根据波动率调整止损比例
            volatility_multiplier = 1 + (volatility * 10)  # 波动率影响因子
            stop_percentage = base_stop_percentage * volatility_multiplier
            
            # 计算止损价格
            if direction == 'long':
                stop_loss = entry_price * (1 - stop_percentage)
                # 使用近期低点作为参考
                recent_low = min(float(k['low']) for k in klines[-20:])
                stop_loss = max(stop_loss, recent_low * 0.995)  # 略低于近期低点
            else:  # short
                stop_loss = entry_price * (1 + stop_percentage)
                # 使用近期高点作为参考
                recent_high = max(float(k['high']) for k in klines[-20:])
                stop_loss = min(stop_loss, recent_high * 1.005)  # 略高于近期高点
                
            # 计算风险比率 (止损距离/预期目标距离)
            risk_ratio = abs(entry_price - stop_loss) / entry_price
            
            return {
                'stop_loss': round(stop_loss, 8),
                'risk_ratio': round(risk_ratio, 4),
                'stop_percentage': round(stop_percentage * 100, 2)
            }
            
        except Exception as e:
            logger.error(f"计算止损价格失败: {str(e)}")
            return {'stop_loss': 0, 'risk_ratio': 0, 'stop_percentage': 0}

    def update_trailing_stop(self, symbol: str, entry_price: float, current_price: float, 
                            direction: str, initial_stop: float) -> dict:
        """更新跟踪止损价格
        
        Args:
            symbol: 交易对
            entry_price: 入场价格
            current_price: 当前价格
            direction: 交易方向 ('long' 或 'short')
            initial_stop: 初始止损价格
            
        Returns:
            dict: 包含新的止损价格和其他信息
        """
        try:
            # 获取币种特性和波动率
            characteristics = self._get_symbol_characteristics(symbol)
            volatility = self.calculate_volatility(symbol)
            
            # 计算盈利比例
            if direction == 'long':
                profit_ratio = (current_price - entry_price) / entry_price
            else:
                profit_ratio = (entry_price - current_price) / entry_price
                
            # 根据盈利情况动态调整止损
            if profit_ratio > 0:
                # 主流币和小币种使用不同的保护策略
                if characteristics['is_major']:
                    # 主流币使用更保守的保护策略
                    protection_levels = [
                        (0.02, 0.2),   # 盈利2%，保护20%利润
                        (0.05, 0.4),   # 盈利5%，保护40%利润
                        (0.10, 0.6),   # 盈利10%，保护60%利润
                        (0.15, 0.8)    # 盈利15%，保护80%利润
                    ]
                else:
                    # 小币种使用激进的保护策略
                    protection_levels = [
                        (0.03, 0.3),   # 盈利3%，保护30%利润
                        (0.07, 0.5),   # 盈利7%，保护50%利润
                        (0.15, 0.7),   # 盈利15%，保护70%利润
                        (0.20, 0.9)    # 利20%，保护90%利润
                    ]
                
                # 根据波动率调整保护水平
                volatility_factor = 1 + (volatility * 5)  # 波动率影响因子
                
                # 计算新的止损价格
                new_stop = initial_stop
                for profit_level, protection_ratio in protection_levels:
                    if profit_ratio >= profit_level * volatility_factor:
                        if direction == 'long':
                            # 计算保护价格（在盈利点和入场价之间）
                            protection_price = entry_price + (current_price - entry_price) * protection_ratio
                            # 新止损价格取保护价格和当前止损的较高值
                            new_stop = max(new_stop, protection_price)
                        else:
                            # 空单反向计算
                            protection_price = entry_price - (entry_price - current_price) * protection_ratio
                            # 新止损价格取保护价格和当前止损的较低值
                            new_stop = min(new_stop, protection_price)
                
                return {
                    'stop_loss': round(new_stop, 8),
                    'profit_ratio': round(profit_ratio * 100, 2),
                    'protection_level': round(
                        abs(new_stop - entry_price) / abs(current_price - entry_price) * 100 
                        if current_price != entry_price else 0, 
                        2
                    )
                }
            
            # 如果未盈利，返回原止损价格
            return {
                'stop_loss': initial_stop,
                'profit_ratio': round(profit_ratio * 100, 2),
                'protection_level': 0
            }
            
        except Exception as e:
            logger.error(f"更新跟踪止损失败: {str(e)}")
            return {
                'stop_loss': initial_stop,
                'profit_ratio': 0,
                'protection_level': 0
            }

    def analyze_trading_opportunities(self) -> List[dict]:
        """分析当前市场状况，寻找建仓机会"""
        try:
            opportunities = []
            current_timestamp = int(time.time() * 1000)
            
            # 只分析BTC和ETH
            major_symbols = ['BTCUSDT', 'ETHUSDT']
            
            for symbol in major_symbols:
                try:
                    # 获取币种特性
                    characteristics = self._get_symbol_characteristics(symbol)
                    
                    # 计算技术指标
                    volatility = self.calculate_volatility(symbol)
                    trend_strength = self.calculate_trend_strength(symbol)
                    
                    # 获取最新价格数据
                    latest_data = self.latest_prices.get(symbol, {})
                    if not latest_data:
                        continue
                        
                    current_price = float(latest_data.get('price', 0))
                    if not current_price:
                        continue
                    
                    # 建仓条件评分系统 (0-100分)
                    score = 0
                    signals = []
                    
                    # 1. 趋势强度评分 (0-40分)
                    trend_score = abs(trend_strength) * 40
                    direction = 'long' if trend_strength > 0 else 'short'
                    if trend_strength > 0:
                        signals.append(f"上升趋势 (强度: {trend_strength:.2f})")
                    elif trend_strength < 0:
                        signals.append(f"下降趋势 (强度: {abs(trend_strength):.2f})")
                    score += min(trend_score, 40)
                    
                    # 2. 波动率评分 (0-30���)
                    vol_score = min(volatility * 100, 30)
                    score += vol_score
                    signals.append(f"波动率: {volatility:.4f}")
                    
                    # 3. 成交量评分 (0-30分)
                    volume = float(latest_data.get('quote_volume', 0))
                    avg_volume = characteristics['avg_volume']
                    if avg_volume > 0:
                        volume_ratio = volume / avg_volume
                        volume_score = min(volume_ratio * 15, 30)
                        score += volume_score
                        signals.append(f"成交量比: {volume_ratio:.2f}")
                    
                    # 记录分析结果
                    opportunity = {
                        'timestamp': current_timestamp,
                        'symbol': symbol,
                        'score': score,
                        'price': current_price,
                        'signals': signals,
                        'trend_strength': trend_strength,
                        'volatility': volatility,
                        'volume_ratio': volume_ratio if avg_volume > 0 else 0,
                        'direction': direction,
                        'price_change': (current_price - latest_data.get('open', current_price)) / latest_data.get('open', current_price)
                    }
                    opportunities.append(opportunity)
                    
                except Exception as e:
                    logger.error(f"分析 {symbol} 失败: {str(e)}")
                    continue
            
            # 输出分析结果
            if opportunities:
                self._output_opportunities(opportunities)
                # 保存到数据库
                for opp in opportunities:
                    self._save_opportunity(opp)
                
                # 发送数据到前端
                self._send_to_frontend(opportunities)
            
            return opportunities
            
        except Exception as e:
            logger.error(f"分析建仓机会失败: {str(e)}")
            return []

    def _save_opportunity(self, opportunity: dict):
        """保存交易机会到数据库"""
        try:
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                INSERT INTO trading_opportunities 
                (timestamp, symbol, score, price, trend_strength, 
                 volatility, volume_ratio, signals, is_major, direction, stop_loss, risk_ratio, stop_percentage, protection_level, initial_leverage, max_leverage)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    opportunity['timestamp'],
                    opportunity['symbol'],
                    opportunity['score'],
                    opportunity['price'],
                    opportunity['trend_strength'],
                    opportunity['volatility'],
                    opportunity['volume_ratio'],
                    json.dumps(opportunity['signals']),
                    1 if opportunity['is_major'] else 0,
                    opportunity['direction'],
                    opportunity['stop_loss'],
                    opportunity['risk_ratio'],
                    opportunity['stop_percentage'],
                    opportunity.get('protection_level', 0),
                    opportunity['initial_leverage'],
                    opportunity['max_leverage']
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"保存交易机会失败: {str(e)}")

    def _output_opportunities(self, opportunities: List[dict]):
        """输出交易机会分析结果"""
        logger.info("\n=== 建仓机会分析 ===")
        logger.info("-" * 120)
        logger.info(f"{'交易对':^12} {'方向':^6} {'得分':^6} {'价格':^10} {'止损价':^10} "
                   f"{'止损比例%':^8} {'趋势强度':^8} {'波动率':^8} {'成交量比':^8} {'信号'}")
        logger.info("-" * 120)
        
        for opp in opportunities:
            logger.info(
                f"{opp['symbol']:^12} {opp['direction']:^6} {opp['score']:>6.1f} "
                f"{opp['price']:>10.4f} {opp['stop_loss']:>10.4f} {opp['stop_percentage']:>8.2f} "
                f"{opp['trend_strength']:>8.2f} {opp['volatility']:>8.4f} "
                f"{opp['volume_ratio']:>8.2f} | {', '.join(opp['signals'])}"
            )
        logger.info("-" * 120)

    async def _send_to_frontend(self, data: dict):
        """发送数据到前端WebSocket"""
        try:
            message = {
                'type': 'market_data',
                'data': data,
                'timestamp': int(time.time() * 1000)
            }
            
            # 检查是否有websocket_server属性
            if not hasattr(self, 'websocket_server'):
                logger.warning("WebSocket服务器未初始化，无法发送数据")
                return
                
            # 发送到WebSocket服务器
            if self.websocket_server:
                await self.websocket_server.send_message(json.dumps(message))
                logger.debug(f"已发送市场数据到前端: {len(data)} 条记录")
            else:
                logger.warning("WebSocket服务器未就绪，无法发送数据")
                
        except Exception as e:
            logger.error(f"发送数据到前端失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    async def stop(self):
        """停止市场数据管理器"""
        try:
            logger.info("正在停止市场数据管理器...")
            
            # 关闭所有WebSocket连接
            if hasattr(self, 'ws_clients'):
                for ws in self.ws_clients:
                    if ws and not ws.closed:
                        await ws.close()
                self.ws_clients = []
            
            # 关闭HTTP会话
            if hasattr(self, 'session') and self.session:
                await self.session.close()
                
            # 关闭数据库连接
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
                
            logger.info("市场数据管理器已停止")
            
        except Exception as e:
            logger.error(f"停止市场数据管理器失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    def _encode_symbol(self, symbol: str) -> str:
        """对交易对符号进行编码，处理特殊字符"""
        # 如果符号以数字开头，添加特殊处理
        if symbol[0].isdigit():
            return symbol.lower()  # 转换为小写
        return symbol.lower()

    async def _create_ws_subscription_url(self, symbols: List[str]) -> str:
        """创建WebSocket订阅URL"""
        streams = []
        for symbol in symbols:
            encoded_symbol = self._encode_symbol(symbol)
            streams.extend([
                f"{encoded_symbol}@aggTrade",
                f"{encoded_symbol}@kline_1m"
            ])
        return f"{self.FUTURES_WS_URL}/{'/'.join(streams)}"

    def _cleanup_expired_history(self):
        """清理过期的历史数据"""
        try:
            from datetime import datetime, timedelta
            import pandas as pd
            
            history_dir = self.data_paths['history']
            cutoff_date = datetime.now() - timedelta(days=self.data_retention['history_days'])
            
            for file in os.listdir(history_dir):
                if file.endswith('_5m.csv'):
                    file_path = os.path.join(history_dir, file)
                    try:
                        # 读取CSV文件
                        df = pd.read_csv(file_path)
                        if 'timestamp' in df.columns:
                            # 转换时间戳
                            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                            # 只保留截止日期之后的数
                            df = df[df['timestamp'] >= cutoff_date]
                            # 保存回文件
                            df.to_csv(file_path, index=False)
                            logger.debug(f"清理了 {file} 中的过期数据")
                    except Exception as e:
                        logger.error(f"清理 {file} 失败: {str(e)}")
                        continue
            
            logger.info("历史数据清理完成")
            
        except Exception as e:
            logger.error(f"清理过期历史数据失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    def _get_db_connection(self):
        """获取数据库连接"""
        return sqlite3.connect(self.db_path)

    def analyze_correlation_and_next_target(self, current_symbol: str) -> dict:
        """分析与当前交易币种的关联性，寻找下一个目标
        
        Args:
            current_symbol: 当前交易的币种
            
        Returns:
            dict: 包含下一个目标币种和关联信息
        """
        try:
            if current_symbol not in self.real_time_cache['1m']:
                return {}
                
            correlations = {}
            current_prices = []
            
            # 获取当前币种的价格数据
            for kline in self.real_time_cache['1m'][current_symbol][-100:]:  # 使用最近100K线
                current_prices.append(float(kline['close']))
                
            # 计算与其他币种的关联性
            for symbol in self.subscribed_symbols:
                if symbol == current_symbol or symbol not in self.real_time_cache['1m']:
                    continue
                    
                other_prices = []
                for kline in self.real_time_cache['1m'][symbol][-100:]:
                    other_prices.append(float(kline['close']))
                    
                if len(other_prices) == len(current_prices):
                    correlation = np.corrcoef(current_prices, other_prices)[0, 1]
                    correlations[symbol] = {
                        'correlation': correlation,
                        'volatility': self.calculate_volatility(symbol),
                        'trend_strength': self.calculate_trend_strength(symbol)
                    }
            
            # 根据关联性和其他指标排序
            sorted_correlations = sorted(
                correlations.items(),
                key=lambda x: (
                    abs(x[1]['correlation']),  # 关联性绝对值
                    x[1]['trend_strength'],    # 趋势强度
                    x[1]['volatility']         # 波动率
                ),
                reverse=True
            )
            
            if sorted_correlations:
                next_symbol, stats = sorted_correlations[0]
                return {
                    'symbol': next_symbol,
                    'correlation': stats['correlation'],
                    'trend_strength': stats['trend_strength'],
                    'volatility': stats['volatility']
                }
            
            return {}
            
        except Exception as e:
            logger.error(f"分析关联性失败: {str(e)}")
            return {}

    def calculate_position_adjustment(self, symbol: str, current_price: float, 
                                   entry_price: float, current_leverage: int = 20,
                                   is_last_position: bool = False) -> dict:
        """计算仓位调整和杠杆倍数"""
        try:
            # 计算盈利比例
            profit_ratio = (current_price - entry_price) / entry_price
            
            # 获取币种特性
            characteristics = self._get_symbol_characteristics(symbol)
            
            # 获取最大杠杆
            max_leverage = characteristics['max_leverage']
            
            # 当盈利达到2%时进行调整
            if profit_ratio >= 0.02:  # 2%盈利点
                # 计算新的止损价格（在当前盈利的90%位置，保护至少90%的利润）
                new_stop_price = entry_price + (current_price - entry_price) * 0.9
                
                # 根据不同币种设置新的杠杆倍数
                if symbol == 'BTCUSDT':
                    new_leverage = 125  # BTC最大125倍
                elif symbol == 'ETHUSDT':
                    new_leverage = 100  # ETH最大100倍
                else:
                    new_leverage = min(current_leverage * 2.5, max_leverage)  # 其他币种最多提升2.5倍
                
                # 计算可以撤出的保证金比例
                # 如果是最后一个仓位，保留所有保证金
                if is_last_position:
                    margin_release_ratio = 0
                else:
                    margin_release_ratio = 1 - (current_leverage / new_leverage)
                
                return {
                    'can_adjust': True,
                    'current_profit': round(profit_ratio * 100, 2),
                    'suggested_leverage': new_leverage,
                    'margin_release_ratio': round(margin_release_ratio * 100, 2),
                    'new_stop_price': round(new_stop_price, 8),
                    'max_leverage': max_leverage,
                    'is_major': characteristics['is_major']
                }
            
            # 未达到调整条件
            return {
                'can_adjust': False,
                'current_profit': round(profit_ratio * 100, 2),
                'suggested_leverage': current_leverage,
                'margin_release_ratio': 0,
                'new_stop_price': 0,
                'max_leverage': max_leverage,
                'is_major': characteristics['is_major']
            }
            
        except Exception as e:
            logger.error(f"计算仓位调整失败: {str(e)}")
            return {
                'can_adjust': False,
                'current_profit': 0,
                'suggested_leverage': current_leverage,
                'margin_release_ratio': 0,
                'new_stop_price': 0,
                'max_leverage': max_leverage,
                'is_major': characteristics['is_major']
            }
