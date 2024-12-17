import asyncio
import os
if os.name == 'nt':
    from asyncio import WindowsSelectorEventLoopPolicy
    asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())

from typing import Dict, List, Optional, Any, Union
from correlation_manager import CorrelationManager
from market_data_manager import MarketDataManager
from loguru import logger
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import sys
from collections import defaultdict
from config import get_thresholds  # 添加配置导入

# 配置日志
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO"
)
logger.add(
    "logs/strategy_test_{time}.log",
    rotation="500 MB",
    level="DEBUG"
)

async def load_local_data(market_data: MarketDataManager):
    """加载本地数据"""
    try:
        logger.info("检查本地数据...")
        
        # 从history目录读取5分钟K线数据
        history_dir = 'cache/history'
        available_symbols = []
        
        for file in os.listdir(history_dir):
            if not file.endswith('_5m.csv'):
                continue
                
            symbol = file.replace('_5m.csv', '')
            file_path = os.path.join(history_dir, file)
            
            try:
                # 读取CSV文件
                df = pd.read_csv(file_path)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                
                if len(df) == 0:
                    logger.warning(f"{symbol} 没有历史数据")
                    continue
                    
                # 获取数据统计信息
                start_time = df['timestamp'].min()
                end_time = df['timestamp'].max()
                duration = end_time - start_time
                count = len(df)
                avg_volume = df['volume'].mean()
                
                logger.info(f"\n{symbol}:")
                logger.info(f"  数据时间范围: {start_time} 到 {end_time} ({duration.days} 天)")
                logger.info(f"  数据点数量: {count}")
                logger.info(f"  平均成交量: {avg_volume:.2f}")
                
                # 将交易对添加到订阅列表
                market_data.subscribed_symbols.add(symbol)
                available_symbols.append(symbol)
                
                # 获取最新的价格和成交量
                latest = df.iloc[-1]
                market_data.latest_prices[symbol] = {
                    'price': float(latest['close']),
                    'volume': float(latest['volume']),
                    'timestamp': int(latest['timestamp'].timestamp() * 1000)
                }
                
            except Exception as e:
                logger.error(f"处理 {symbol} 的数据失败: {str(e)}")
                continue
                
        if not available_symbols:
            logger.error("没有找到可用的历史数据")
            return False
            
        logger.info(f"\n成功加载 {len(available_symbols)} 个交易对的历史数据")
        return True
        
    except Exception as e:
        logger.error(f"加载本地数据失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def test_strategy():
    """测试策略分析引擎"""
    market_data = None
    try:
        logger.info("开始USDT永续合约策略测试...")
        
        # 确保数据目录存在
        os.makedirs("data", exist_ok=True)
        os.makedirs("logs", exist_ok=True)
        
        # 初始化管理器
        market_data = MarketDataManager()
        correlation_manager = CorrelationManager()
        
        # 加载本地数据
        if not await load_local_data(market_data):
            logger.error("无法加载本地数据，测试终止")
            return
        
        # 初始化数据
        logger.info("\n初始化市场数据...")
        try:
            await correlation_manager.initialize_data()
            
            # 输出相关性矩阵信息
            if correlation_manager.correlation_matrix is not None:
                logger.info("\n相关性矩阵统计:")
                matrix = correlation_manager.correlation_matrix
                high_corr_pairs = []
                for i in range(len(matrix.index)):
                    for j in range(i+1, len(matrix.index)):
                        corr = matrix.iloc[i, j]
                        if abs(corr) > correlation_manager.signal_thresholds['min_correlation']:
                            high_corr_pairs.append((
                                matrix.index[i],
                                matrix.index[j],
                                corr
                            ))
                
                logger.info(f"发现 {len(high_corr_pairs)} 对高相关性交易对:")
                for sym1, sym2, corr in sorted(high_corr_pairs, key=lambda x: abs(x[2]), reverse=True)[:10]:
                    logger.info(f"  {sym1} - {sym2}: {corr:.3f}")
                    
        except Exception as e:
            logger.error(f"初始化数据失败: {str(e)}")
            return
        
        # 获取最近5分钟的数据进行回测
        logger.info("\n开始5分钟数据回测...")
        
        # 初始化1分钟K线数据
        logger.info("初始化1分钟K线数据...")
        for symbol in market_data.subscribed_symbols:
            try:
                klines = await market_data.get_klines_data(symbol, interval='1m', limit=5)
                if klines:
                    await market_data._save_klines_to_db(symbol, '1m', klines)
                    logger.info(f"保存 {symbol} 的1分钟K线数据：{len(klines)} 条")
                await asyncio.sleep(0.5)  # 添加短暂延迟避免请求过快
            except Exception as e:
                logger.error(f"获取 {symbol} 的1分钟数据失败: {str(e)}")
                continue
        
        # 获取回测数据
        with market_data._get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 获取最新时间戳
            cursor.execute('''
            SELECT MAX(timestamp)
            FROM kline_data
            WHERE timeframe = '1m'
            ''')
            latest_timestamp = cursor.fetchone()[0]
            
            if latest_timestamp is None:
                logger.error("没有找到1分钟K线数据")
                return
                
            # 计算5分钟前的时间戳
            five_mins_ago = latest_timestamp - (5 * 60 * 1000)
            
            # 获取这段时间的数据
            cursor.execute('''
            SELECT symbol, timestamp, open, high, low, close, volume, quote_volume
            FROM kline_data
            WHERE timeframe = '1m'
            AND timestamp >= ?
            ORDER BY timestamp ASC
            ''', (five_mins_ago,))
            
            test_data = cursor.fetchall()
            
        if not test_data:
            logger.error("没有找到最近5分钟的数据")
            return
            
        # 按时间戳组织数据
        time_grouped_data = {}
        for row in test_data:
            timestamp = row[1]
            if timestamp not in time_grouped_data:
                time_grouped_data[timestamp] = []
            time_grouped_data[timestamp].append({
                'symbol': row[0],
                'timestamp': timestamp,
                'open': row[2],
                'high': row[3],
                'low': row[4],
                'close': row[5],
                'volume': row[6],
                'quote_volume': row[7]
            })
            
        # 模拟实时交易
        logger.info("\n开始模拟交易...")
        signal_stats = {
            'total': 0,
            'open': 0,
            'close': 0,
            'ignored': 0,
            'by_symbol': defaultdict(int)
        }
        
        # 按时间顺序处理数据
        for timestamp in sorted(time_grouped_data.keys()):
            current_time = datetime.fromtimestamp(timestamp/1000)
            logger.info(f"\n处理时间点: {current_time}")
            
            # 更新市场数据
            for data in time_grouped_data[timestamp]:
                symbol = data['symbol']
                # 更新价格数据
                market_data.latest_prices[symbol] = {
                    'price': data['close'],
                    'volume': data['volume'],
                    'timestamp': timestamp
                }
                
                # 更新相关性管理器的数据
                await correlation_manager.update_realtime_data(
                    symbol,
                    data['close'],
                    data['volume'],
                    timestamp
                )
            
            # 获取交易信号
            signals = await correlation_manager.get_trading_signals()
            
            if signals:
                signal_stats['total'] += len(signals)
                
                for signal in signals:
                    symbol = signal['symbol']
                    signal_stats['by_symbol'][symbol] += 1
                    
                    if signal['action'] == 'open':
                        signal_stats['open'] += 1
                    elif signal['action'] == 'close':
                        signal_stats['close'] += 1
                        
                    logger.info(f"\n交易信号:")
                    logger.info(f"  交易对: {symbol}")
                    logger.info(f"  操作: {signal['action']}")
                    logger.info(f"  价格: {signal['price']}")
                    logger.info(f"  原因: {signal['reason']}")
        
        # 输出测试结果统计
        logger.info("\n回测结果统计:")
        logger.info(f"总信号数量: {signal_stats['total']}")
        logger.info(f"开仓信号: {signal_stats['open']}")
        logger.info(f"平仓信号: {signal_stats['close']}")
        logger.info("\n各交易对信号统计:")
        for symbol, count in sorted(signal_stats['by_symbol'].items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  {symbol}: {count}")
            
    except Exception as e:
        logger.error(f"测试过程中出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # 清理资源
        if market_data is not None:
            await market_data.stop_price_updates()
        logger.info("测试结束")

if __name__ == "__main__":
    if os.name == 'nt':  # Windows系统
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(test_strategy())
    except KeyboardInterrupt:
        logger.info("用户中断测试")
    except Exception as e:
        logger.error(f"程序异常退出: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())