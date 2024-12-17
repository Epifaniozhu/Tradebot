from typing import Dict, Any, List, Optional
import ccxt
import pandas as pd
import numpy as np
from loguru import logger
import ta
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

class MarketAnalyzer:
    def __init__(self, exchange_id: str = 'binance'):
        """
        市场分析模块初始化
        :param exchange_id: 交易所ID，默认使用币安
        """
        # 从环境变量读取代理配置
        use_proxy = os.getenv('USE_PROXY', 'false').lower() == 'true'
        proxy_host = os.getenv('PROXY_HOST')
        proxy_port = os.getenv('PROXY_PORT')
        
        # 配置交易所连接
        exchange_config = {
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',  # 使用U本位合约
                'defaultMarginMode': 'isolated',  # 使用逐仓模式
                'defaultLeverage': int(os.getenv('MAX_LEVERAGE', '2')),  # 从环境变量读取杠杆倍数
                'adjustForTimeDifference': True
            }
        }
        
        # 添加代理配��
        if use_proxy and proxy_host and proxy_port:
            proxy_url = f'http://{proxy_host}:{proxy_port}'
            exchange_config['proxies'] = {
                'http': proxy_url,
                'https': proxy_url
            }
            logger.info(f"使用代理服务器: {proxy_url}")
        
        # 添加API密钥配置
        api_key = os.getenv('BINANCE_API_KEY')
        api_secret = os.getenv('BINANCE_API_SECRET')
        if api_key and api_secret:
            exchange_config['apiKey'] = api_key
            exchange_config['secret'] = api_secret
            
        self.exchange = getattr(ccxt, exchange_id)(exchange_config)
        self.cached_data = {}  # 缓存数据
        
        logger.info(f"MarketAnalyzer 初始化完成，交易所: {exchange_id}")
        
    def get_market_data(self, symbol: str, timeframe: str = '5m', limit: int = 100) -> pd.DataFrame:
        """
        获取市场数据
        :param symbol: 交易对符号
        :param timeframe: K线时间周期 ('5m' 或 '1m')
        :param limit: 获取的K线数量
        :return: DataFrame包含OHLCV数据
        """
        try:
            if timeframe == '5m':
                # 从history目录读取5分钟K线数据
                file_path = os.path.join('cache/history', f"{symbol}_5m.csv")
                if os.path.exists(file_path):
                    df = pd.read_csv(file_path)
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df.set_index('timestamp', inplace=True)
                    
                    # 只返回最近的limit条数据
                    if len(df) > limit:
                        df = df.tail(limit)
                    
                    # 计算基本技术指标
                    self._calculate_indicators(df)
                    
                    # 缓存数据
                    self.cached_data[symbol] = df
                    return df
                    
            elif timeframe == '1m':
                # 从minK目录读取1分钟K线数据
                file_path = os.path.join('cache/history/minK', f"{symbol}_1m.csv")
                if os.path.exists(file_path):
                    df = pd.read_csv(file_path)
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df.set_index('timestamp', inplace=True)
                    
                    # 只返回最近的limit条数据
                    if len(df) > limit:
                        df = df.tail(limit)
                    
                    # 计算基本技术指标
                    self._calculate_indicators(df)
                    
                    # 缓存数据
                    self.cached_data[symbol] = df
                    return df
                    
            logger.warning(f"找不到 {symbol} 的 {timeframe} 数据文件")
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"获取市场数据失败: {str(e)}")
            return pd.DataFrame()
            
    def _calculate_indicators(self, df: pd.DataFrame) -> None:
        """
        计算技术指标
        :param df: 包含OHLCV数据的DataFrame
        """
        try:
            # 移动平均线
            df['MA5'] = ta.trend.sma_indicator(df['close'], window=5)
            df['MA10'] = ta.trend.sma_indicator(df['close'], window=10)
            df['MA20'] = ta.trend.sma_indicator(df['close'], window=20)
            
            # MACD
            macd = ta.trend.MACD(df['close'])
            df['MACD'] = macd.macd()
            df['MACD_signal'] = macd.macd_signal()
            df['MACD_hist'] = macd.macd_diff()
            
            # RSI
            df['RSI'] = ta.momentum.rsi(df['close'], window=14)
            
            # 布林带
            bollinger = ta.volatility.BollingerBands(df['close'])
            df['BB_upper'] = bollinger.bollinger_hband()
            df['BB_middle'] = bollinger.bollinger_mavg()
            df['BB_lower'] = bollinger.bollinger_lband()
            
            # 成交量指标
            df['Volume_MA5'] = ta.trend.sma_indicator(df['volume'], window=5)
            
        except Exception as e:
            logger.error(f"计算技术指标失败: {str(e)}")
            
    def _calculate_support_resistance(self, data: pd.DataFrame, window: int = 20) -> Dict[str, float]:
        """
        计算支撑位和阻力位
        :param data: OHLCV数据
        :param window: 计算窗口
        :return: 支撑位和阻力位
        """
        recent_data = data.tail(window)
        
        # 使用布林带作为动态支撑和阻力
        latest = recent_data.iloc[-1]
        
        # 计算近期高点和低点
        recent_high = recent_data['high'].max()
        recent_low = recent_data['low'].min()
        
        return {
            'current_price': latest['close'],
            'immediate_support': latest['BB_lower'],
            'immediate_resistance': latest['BB_upper'],
            'strong_support': recent_low,
            'strong_resistance': recent_high
        }
        
    def analyze_trend(self, data: pd.DataFrame, lookback_periods: int = 5) -> Dict[str, Any]:
        """
        分析市场趋势
        :param data: OHLCV数据
        :param lookback_periods: 回溯周期数
        :return: 包含分析结果的字典
        """
        try:
            latest = data.iloc[-1]
            prev = data.iloc[-lookback_periods]
            
            # 趋势强度分析
            trend_strength = {
                'price_change_pct': (latest['close'] - prev['close']) / prev['close'] * 100,
                'volume_change_pct': (latest['volume'] - prev['volume']) / prev['volume'] * 100,
                'ma_alignment': latest['MA5'] > latest['MA10'] > latest['MA20'],
                'rsi': latest['RSI'],
                'macd_hist': latest['MACD_hist']
            }
            
            # 趋势判断
            trend = self._determine_trend(latest, trend_strength)
            
            # 支撑和阻力位
            support_resistance = self._calculate_support_resistance(data)
            
            return {
                'trend': trend,
                'trend_strength': trend_strength,
                'support_resistance': support_resistance,
                'indicators': {
                    'rsi': latest['RSI'],
                    'macd': latest['MACD'],
                    'macd_signal': latest['MACD_signal'],
                    'bb_position': (latest['close'] - latest['BB_lower']) / (latest['BB_upper'] - latest['BB_lower'])
                }
            }
            
        except Exception as e:
            logger.error(f"分析趋势失败: {str(e)}")
            return None
            
    def _determine_trend(self, latest: pd.Series, strength: Dict) -> str:
        """
        确定趋势方向
        :param latest: 最新的��场数据
        :param strength: 趋势强度指标
        :return: 趋势方向 ('strong_up', 'up', 'neutral', 'down', 'strong_down')
        """
        score = 0
        
        # MA排列得分
        if strength['ma_alignment']:
            score += 2
        elif latest['MA5'] > latest['MA10']:
            score += 1
            
        # RSI得分
        if strength['rsi'] > 70:
            score += 2
        elif strength['rsi'] > 60:
            score += 1
        elif strength['rsi'] < 30:
            score -= 2
        elif strength['rsi'] < 40:
            score -= 1
            
        # MACD得分
        if strength['macd_hist'] > 0:
            score += 1
        else:
            score -= 1
            
        # 价格变化得分
        if strength['price_change_pct'] > 5:
            score += 2
        elif strength['price_change_pct'] > 2:
            score += 1
        elif strength['price_change_pct'] < -5:
            score -= 2
        elif strength['price_change_pct'] < -2:
            score -= 1
            
        # 根据总分判断趋势
        if score >= 4:
            return 'strong_up'
        elif score >= 2:
            return 'up'
        elif score <= -4:
            return 'strong_down'
        elif score <= -2:
            return 'down'
        else:
            return 'neutral'
            
    def get_trading_signals(self, symbol: str, timeframe: str = '1h') -> Dict[str, Any]:
        """
        获取交易信号
        :param symbol: 交易对
        :param timeframe: 时间周期
        :return: 交易信号
        """
        try:
            # 获取最新数据
            data = self.get_market_data(symbol, timeframe)
            if data is None:
                return None
                
            # 分析趋势
            analysis = self.analyze_trend(data)
            if analysis is None:
                return None
                
            # 生成交易信号
            latest = data.iloc[-1]
            signals = {
                'symbol': symbol,
                'timestamp': datetime.now(),
                'price': latest['close'],
                'trend': analysis['trend'],
                'signals': []
            }
            
            # 添加具体信号
            if analysis['trend'] in ['strong_up', 'up']:
                signals['signals'].append({
                    'type': 'buy',
                    'strength': 'strong' if analysis['trend'] == 'strong_up' else 'normal',
                    'reason': f"上升趋势，RSI={analysis['indicators']['rsi']:.2f}，MACD柱状={analysis['trend_strength']['macd_hist']:.2f}"
                })
                
            elif analysis['trend'] in ['strong_down', 'down']:
                signals['signals'].append({
                    'type': 'sell',
                    'strength': 'strong' if analysis['trend'] == 'strong_down' else 'normal',
                    'reason': f"下降趋势，RSI={analysis['indicators']['rsi']:.2f}，MACD柱状={analysis['trend_strength']['macd_hist']:.2f}"
                })
                
            # 添加支撑位和阻力位
            signals['support_resistance'] = analysis['support_resistance']
            
            return signals
            
        except Exception as e:
            logger.error(f"获取交易信号失败: {str(e)}")
            return None