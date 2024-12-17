from typing import Dict, List, Optional, Tuple, Any
import numpy as np
import pandas as pd
from loguru import logger
from datetime import datetime, timedelta
from market_data_manager import MarketDataManager
from collections import deque
import time
import asyncio
import os

class CorrelationManager:
    def __init__(self, market_data):
        self.market_data = market_data
        self.correlation_matrix = None
        self.returns_data = {}
        self._initialized = False
        
        # 添加实时数据相关的属性
        self.realtime_data = {}
        self.realtime_correlation = {}
        self.last_realtime_update = {}
        self.last_update = None
        self.update_interval = 60  # 60秒更新一次
        
        # 信号阈值配置
        self.signal_thresholds = {
            'price_change': 0.005,  # 0.5%的价格变化
            'volume_spike': 1.5,    # 1.5倍于平均成交量
            'min_correlation': 0.5,  # 最小相关系数
            'trend_confirmation': 0.6  # 趋势确认阈值
        }
        
        # 风险参数配置
        self.risk_params = {
            'btc_eth': {
                'initial_position_size': 0.1,  # 初始仓位大小
                'max_position_size': 0.3,     # 最大仓位大小
                'stop_loss_pct': 0.02,        # 止损百分比
                'take_profit_pct': 0.04       # 止盈百分比
            },
            'others': {
                'initial_position_size': 0.05,
                'max_position_size': 0.2,
                'stop_loss_pct': 0.03,
                'take_profit_pct': 0.06
            }
        }
        
        # 统计数据
        self.stats = {
            'total_signals': 0,
            'signals_by_symbol': {},
            'volume_spikes': 0,
            'trend_signals': 0
        }
        
        # 仓位管理
        self.positions = {}
        self.position_history = {}
        self.last_signal_time = {}
        
    async def initialize(self):
        """初始化相关性管理器"""
        try:
            if self._initialized:
                logger.info("相关性管理器已经初始化过")
                return True
                
            logger.info("开始初始化相关性管理器...")
            
            # 初始化数据
            await self.initialize_data()
            
            # 启动数据更新任务
            asyncio.create_task(self._periodic_update())
            
            self._initialized = True
            logger.info("相关性管理器初始化完成")
            return True
            
        except Exception as e:
            logger.error(f"初始化相关性管理器失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False
            
    async def _periodic_update(self):
        """定期更新数据的任务"""
        while True:
            try:
                await self.update_data()
                await asyncio.sleep(self.update_interval)
            except Exception as e:
                logger.error(f"定期更新数据失败: {str(e)}")
                await asyncio.sleep(5)  # 出错后等待5秒再试

    def _calculate_rsi(self, prices: np.ndarray, period: int = 14) -> float:
        """计算RSI指标"""
        try:
            if len(prices) < period + 1:
                return 50.0  # 默认值
                
            # 计算价格变化
            deltas = np.diff(prices)
            
            # 分离上涨和下跌
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)
            
            # 计算平均值
            avg_gain = np.mean(gains[-period:])
            avg_loss = np.mean(losses[-period:])
            
            if avg_loss == 0:
                return 100.0
                
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            
            return float(rsi)
            
        except Exception as e:
            logger.error(f"计算RSI失败: {str(e)}")
            return 50.0

    def _calculate_bollinger_bands(self, prices: np.ndarray, period: int = 20, num_std: float = 2.0) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """计算布林带"""
        try:
            if len(prices) < period:
                return np.array([]), np.array([]), np.array([])
                
            # 计算移动平均线
            middle_band = np.convolve(prices, np.ones(period)/period, mode='valid')
            
            # 计算标准差
            rolling_std = np.array([np.std(prices[i:i+period]) for i in range(len(prices)-period+1)])
            
            # 计算上下轨
            upper_band = middle_band + (rolling_std * num_std)
            lower_band = middle_band - (rolling_std * num_std)
            
            return middle_band, upper_band, lower_band
            
        except Exception as e:
            logger.error(f"计算布林带失败: {str(e)}")
            return np.array([]), np.array([]), np.array([])

    def _calculate_volatility(self, prices: np.ndarray, period: int = 20) -> float:
        """计算波动率"""
        try:
            if len(prices) < 2:
                return 0.0
                
            # 计算收益率
            returns = np.diff(prices) / prices[:-1]
            
            # 计算标准差
            volatility = float(np.std(returns[-period:]) if len(returns) >= period else np.std(returns))
            
            return volatility
            
        except Exception as e:
            logger.error(f"计算波动率失败: {str(e)}")
            return 0.0

    async def initialize_data(self):
        """初始化相关性数据"""
        try:
            if self._initialized:
                logger.info("相关性管理器已经初始化过，跳过初始化")
                return
                
            # 初始化市场数据管理器
            await self.market_data.initialize()
            logger.info("市场数据管理器初始化完成")
            
            # 获取可用的交易对
            symbols = list(self.market_data.correlation_data.keys())
            logger.info(f"找到 {len(symbols)} 个可用的交易对")
            
            if not symbols:
                logger.warning("没有可用的交易对")
                return
                
            # 更新收益率数据
            await self._update_returns_data()
            
            # 初始化实时数据结构
            for symbol in symbols:
                self.realtime_data[symbol] = {
                    'prices': deque(maxlen=3600),  # 保存1小时的数据
                    'volumes': deque(maxlen=3600),
                    'timestamps': deque(maxlen=3600)
                }
            
            # 标记为已初始化
            self._initialized = True
            logger.info("相关性管理器初始化完成")
            
        except Exception as e:
            logger.error(f"初始化相关性数据失败: {str(e)}")
            raise

    async def _update_returns_data(self, symbols=None):
        """更新收益率数据
        Args:
            symbols: 可选交易对列表，如果为None则更新所有交易对
        """
        try:
            # 如果没有指定交易对，使用所有可用的交易对
            if symbols is None:
                symbols = list(self.market_data.correlation_data.keys())
                
            # 获取每个交易对的价格数据
            for symbol in symbols:
                # 从market_data获取K线数据
                klines = await self.market_data.get_klines_data(symbol, interval='1h', limit=100)
                if klines and len(klines) > 0:
                    # 将K线数据转换为DataFrame
                    df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trades', 'taker_buy_base', 'taker_buy_quote', 'ignore'])
                    df['close'] = df['close'].astype(float)
                    
                    # 计算收益率
                    returns = df['close'].pct_change()
                    self.returns_data[symbol] = returns.dropna()
                    logger.debug(f"更新 {symbol} 的收益率数据：{len(self.returns_data[symbol])} 个数据点")
            
            # 更新相关性���阵
            if len(self.returns_data) >= 2:
                returns_df = pd.DataFrame(self.returns_data)
                self.correlation_matrix = returns_df.corr()
                logger.debug(f"更新相关性矩阵完成，包含 {len(self.correlation_matrix)} 个交易对")
            else:
                logger.warning("没有足够的数据来计算相关性矩阵")
                
        except Exception as e:
            logger.error(f"更新收益率数据失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    async def update_data(self, top_symbols: Optional[List[str]] = None):
        """更新数据"""
        try:
            current_time = time.time()
            
            # 检查是否需要更新
            if self.last_update and current_time - self.last_update < self.update_interval:
                return
            
            # 如果没有指定交易对，使用所有已订阅的交易对
            if top_symbols is None:
                top_symbols = self.market_data.subscribed_symbols
            
            if not top_symbols:
                logger.warning("没有可用的交易对数据")
                return
            
            logger.info(f"开始更新 {len(top_symbols)} 个交易对的数据")
            
            # 更新每个交易对的数据
            for symbol in top_symbols:
                try:
                    # 获取新价格数据
                    latest_price = self.market_data.latest_prices.get(symbol)
                    if latest_price:
                        price = latest_price.get('price', 0)
                        volume = latest_price.get('volume', 0)
                        timestamp = int(current_time * 1000)
                        
                        # 更新实时数据
                        await self.update_realtime_data(symbol, price, volume, timestamp)
                except Exception as e:
                    logger.error(f"更新 {symbol} 数据失败: {str(e)}")
                    continue
            
            # 更新收益率数据和相关性矩阵
            await self._update_returns_data(top_symbols)
            
            self.last_update = current_time
            
        except Exception as e:
            logger.error(f"更新数据失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    async def _update_correlation_matrix(self):
        """更新相关性矩阵"""
        try:
            if not self.returns_history:
                logger.warning("没有可用的收益率数据")
                return
                
            # 创建收益DataFrame
            returns_data = {}
            for symbol, returns in self.returns_history.items():
                if returns:  # 确保有数据
                    timestamps, values = zip(*returns)
                    returns_data[symbol] = pd.Series(values, index=timestamps)
                    
            if not returns_data:
                logger.warning("没有足够的数据来计算相关性")
                return
                
            # 计算相关性矩阵
            returns_df = pd.DataFrame(returns_data)
            self.correlation_matrix = returns_df.corr()
            
            logger.info("相关性矩阵更新成功")
            
        except Exception as e:
            logger.error(f"更新相关性矩阵失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    def _update_price_changes(self, symbol: str, price: float, timestamp: float):
        """更新价格变化据"""
        if symbol not in self.price_changes:
            self.price_changes[symbol] = deque(maxlen=60)  # 保存60秒的数据
            
        # 添加新的价格数据
        self.price_changes[symbol].append((timestamp, price))
        
        # 清理超���60秒的旧数据
        current_time = time.time()
        while self.price_changes[symbol] and current_time - self.price_changes[symbol][0][0] > 60:
            self.price_changes[symbol].popleft()

    def _calculate_price_change(self, symbol: str) -> Optional[float]:
        """计算价格变化率"""
        try:
            if symbol not in self.realtime_data:
                return None
            
            data = self.realtime_data[symbol]
            if len(data['prices']) < 2:  # 至少需要2个价格点
                return None
            
            # 使用最近的价格数据计算变化率
            latest_price = data['prices'][-1]
            start_price = data['prices'][0]  # 使用第一个价格作为基准
            
            if start_price > 0:  # 避免除以0
                price_change = (latest_price - start_price) / start_price
                return price_change  # 返回原始比率，乘以100
            
            return None
            
        except Exception as e:
            logger.error(f"计算价格变化率失败: {str(e)}")
            return None

    def _detect_volume_spike(self, symbol: str, current_volume: float) -> bool:
        """检测成交量异常"""
        try:
            # 获取最近的成交量数据
            df = self.market_data.get_price_data(symbol)
            if df.empty:
                return False
                
            # 计算平均成交量
            avg_volume = df['volume'].mean()
            
            # 检查是否出现成交量异常
            return current_volume > avg_volume * self.signal_thresholds['volume_spike']
            
        except Exception as e:
            logger.error(f"检测成交量异常失败: {str(e)}")
            return False

    async def update_realtime_data(self, symbol: str, price: float, volume: float, timestamp: int):
        """更新实时数据"""
        if symbol not in self.realtime_data:
            self.realtime_data[symbol] = {
                'prices': [],
                'volumes': [],
                'timestamps': []
            }
            
        data = self.realtime_data[symbol]
        data['prices'].append(price)
        data['volumes'].append(volume)
        data['timestamps'].append(timestamp)
        
        # 保持最近3600个数据点（1小时）
        if len(data['prices']) > 3600:
            data['prices'] = data['prices'][-3600:]
            data['volumes'] = data['volumes'][-3600:]
            data['timestamps'] = data['timestamps'][-3600:]
            
        # 更新实时相关性
        await self.update_realtime_correlation(symbol)
        
    async def update_realtime_correlation(self, symbol: str):
        """更新实时相关性"""
        try:
            current_time = time.time() * 1000  # 转换为毫秒
            
            # 每秒最多更新一次
            if symbol in self.last_realtime_update and \
               current_time - self.last_realtime_update[symbol] < 1000:
                return
                
            # 取相关交易对
            if self.correlation_matrix is not None:
                correlated_pairs = self.correlation_matrix[symbol][
                    self.correlation_matrix[symbol] > self.signal_thresholds['min_correlation']
                ].index.tolist()
                
                # 计算实时相关性
                for pair in correlated_pairs:
                    if pair in self.realtime_data:
                        # 最近60秒数据
                        symbol_data = self.realtime_data[symbol]
                        pair_data = self.realtime_data[pair]
                        
                        if len(symbol_data['prices']) >= 60 and len(pair_data['prices']) >= 60:
                            symbol_returns = np.diff(symbol_data['prices'][-60:]) / symbol_data['prices'][-61:-1]
                            pair_returns = np.diff(pair_data['prices'][-60:]) / pair_data['prices'][-61:-1]
                            
                            # 计算相关系数
                            correlation = np.corrcoef(symbol_returns, pair_returns)[0, 1]
                            
                            if symbol not in self.realtime_correlation:
                                self.realtime_correlation[symbol] = {}
                            self.realtime_correlation[symbol][pair] = correlation
                            
            self.last_realtime_update[symbol] = current_time
            
        except Exception as e:
            logger.error(f"更新实时相关性失败: {str(e)}")
            
    def _calculate_position_points(self, symbol: str, entry_price: float, trend_strength: float, 
                               volatility: float, position_type: str = 'long') -> Dict[str, float]:
        """
        计算止盈止损点
        :param symbol: 交易对
        :param entry_price: 入价格
        :param trend_strength: 趋势强度
        :param volatility: 
        :param position_type: 仓位类型 ('long' or 'short')
        :return: 止盈止损点信息
        """
        try:
            # 选择参数集
            params = self.risk_params['btc_eth'] if symbol in ['BTCUSDT', 'ETHUSDT'] else self.risk_params['others']
            
            # 根据动率和趋势强度整止盈止损比例
            volatility_adjusted = volatility * params['volatility_multiplier']
            trend_adjusted = trend_strength * params['trend_factor']
            
            # 计算基础止盈止损比例
            base_profit_ratio = max(params['min_profit_ratio'], volatility_adjusted * trend_adjusted)
            base_loss_ratio = min(params['max_loss_ratio'], volatility_adjusted * 0.5)
            
            # 根据仓位类型计算体价格点
            if position_type == 'long':
                take_profit = entry_price * (1 + base_profit_ratio)
                stop_loss = entry_price * (1 - base_loss_ratio)
                protection_point = entry_price * (1 + base_profit_ratio * 0.3)  # 保本位
            else:  # short
                take_profit = entry_price * (1 - base_profit_ratio)
                stop_loss = entry_price * (1 + base_loss_ratio)
                protection_point = entry_price * (1 - base_profit_ratio * 0.3)  # 保本位
            
            return {
                'take_profit': take_profit,
                'stop_loss': stop_loss,
                'protection_point': protection_point,
                'profit_ratio': base_profit_ratio,
                'loss_ratio': base_loss_ratio
            }
            
        except Exception as e:
            logger.error(f"计算止盈止损点失败: {str(e)}")
            return None

    def _has_btc_eth_position(self) -> Dict[str, bool]:
        """
        检查BTC和ETH的仓位状态
        :return: {'BTC': bool, 'ETH': bool} 表示是否有仓位
        """
        return {
            'BTC': bool(self.positions.get('BTCUSDT')),
            'ETH': bool(self.positions.get('ETHUSDT'))
        }

    def _get_btc_eth_position_direction(self) -> Dict[str, Optional[str]]:
        """
        获取BTC和ETH仓位的方向
        :return: {'BTC': direction, 'ETH': direction} direction可能是'long', 'short' 或 None
        """
        directions = {}
        for symbol, name in [('BTCUSDT', 'BTC'), ('ETHUSDT', 'ETH')]:
            if symbol in self.positions and self.positions[symbol]:
                directions[name] = self.positions[symbol][0]['type']
            else:
                directions[name] = None
        return directions

    def _adjust_stop_loss(self, symbol: str, current_price: float) -> None:
        """
        根据价格变动动态调整止损点
        :param symbol: 交易对
        :param current_price: 当前价格
        """
        if symbol not in self.positions or not self.positions[symbol]:
            return

        position = self.positions[symbol][0]  # 获取最新仓位
        entry_price = position['entry_price']
        position_type = position['type']

        # 计算价格变动百分比
        price_change = (current_price - entry_price) / entry_price
        
        # 根据不同币种使用不同的数
        params = self.risk_params['btc_eth'] if symbol in ['BTCUSDT', 'ETHUSDT'] else self.risk_params['others']
        
        if position_type == 'long' and price_change > 0:
            # 多仓盈利，动态调整止损点
            min_profit_ratio = params['min_profit_ratio']
            if price_change > min_profit_ratio * 2:  # 盈利超过2倍最小止盈点
                new_stop_loss = current_price * (1 - min_profit_ratio * 0.5)  # 将止损设为当前价格的一半最小止盈点
                if 'stop_loss' not in position or new_stop_loss > position['stop_loss']:
                    position['stop_loss'] = new_stop_loss
                    logger.info(f"{symbol} 上调止损点至 {new_stop_loss:.4f}")
                    
        elif position_type == 'short' and price_change < 0:
            # 空仓盈利时，动态调整止损点
            min_profit_ratio = params['min_profit_ratio']
            if abs(price_change) > min_profit_ratio * 2:
                new_stop_loss = current_price * (1 + min_profit_ratio * 0.5)
                if 'stop_loss' not in position or new_stop_loss < position['stop_loss']:
                    position['stop_loss'] = new_stop_loss
                    logger.info(f"{symbol} 下调止损点至 {new_stop_loss:.4f}")

    def _get_correlated_symbols(self, major_symbol: str) -> List[str]:
        """
        获取与主流币相关的交易对，按相关强度排序
        :param major_symbol: 主流币交易对
        :return: 相关币种列表（按相关性从强到弱排序）
        """
        try:
            if major_symbol not in self.correlation_matrix:
                return []
                
            # 获取与主流币的相关性
            correlations = self.correlation_matrix[major_symbol]
            
            # 过滤掉主流币（BTC和ETH），保留所有其他币种
            filtered_correlations = {
                symbol: corr for symbol, corr in correlations.items()
                if symbol not in ['BTCUSDT', 'ETHUSDT']
            }
            
            # 按相关性绝对值排序（从强到弱）
            sorted_symbols = sorted(
                filtered_correlations.items(),
                key=lambda x: abs(x[1]),
                reverse=True
            )
            
            # 返回所有相关币种（按相关性排序）
            return [symbol for symbol, _ in sorted_symbols]
            
        except Exception as e:
            logger.error(f"获取相关币种失败: {str(e)}")
            return []

    def _analyze_major_opportunities(self) -> Dict[str, Any]:
        """
        分析BTC和ETH的交易机会，选择更好的一个
        :return: 选中的交易机会信
        """
        try:
            opportunities = {}
            
            # 分析BTC和ETH
            for symbol in ['BTCUSDT', 'ETHUSDT']:
                if symbol not in self.realtime_data:
                    continue
                    
                data = self.realtime_data[symbol]
                if not data['prices'] or len(data['prices']) < 2:
                    continue
                    
                current_price = data['prices'][-1]
                
                # 计算各项指标
                trend_strength = self._calculate_trend_strength(np.array(data['prices']))
                volatility = self.calculate_volatility(symbol)
                volume_change = data.get('volume_change', 0)
                price_change = self._calculate_price_change(symbol)
                
                # 评分系统 (0-100分)
                score = 0
                
                # 1. 趋势强度 (0-40分)
                trend_score = abs(trend_strength) * 40
                score += min(trend_score, 40)
                
                # 2. 波动率评分 (0-30分)
                vol_score = min(volatility * 100, 30)
                score += vol_score
                
                # 3. 成交量评分 (0-30分)
                if volume_change > 0:
                    volume_score = min(volume_change * 30, 30)
                    score += volume_score
                
                opportunities[symbol] = {
                    'symbol': symbol,
                    'price': current_price,
                    'trend_strength': trend_strength,
                    'volatility': volatility,
                    'volume_change': volume_change,
                    'price_change': price_change,
                    'score': score,
                    'direction': 'long' if trend_strength > 0 else 'short'  # 记录趋势方向
                }
            
            # 选择得分最高的机会
            if opportunities:
                best_symbol = max(opportunities.items(), key=lambda x: x[1]['score'])
                return best_symbol[1]
            
            return None
            
        except Exception as e:
            logger.error(f"分析主流币机会失败: {str(e)}")
            return None

    def _analyze_correlated_opportunities(self, major_symbol: str, major_trend: float) -> List[Dict[str, Any]]:
        """
        分析与主流币相关的交易机会
        :param major_symbol: 主流币交易对
        :param major_trend: 主流币趋势强度
        :return: 相关币种的交易信号列表
        """
        try:
            signals = []
            # 取按相关性排序的币种列表
            correlated_symbols = self._get_correlated_symbols(major_symbol)
            
            # 获取主流币的方向
            major_direction = 'long' if major_trend > 0 else 'short'
            
            for symbol in correlated_symbols:
                if symbol not in self.realtime_data:
                    continue
                    
                data = self.realtime_data[symbol]
                if not data['prices'] or len(data['prices']) < 2:
                    continue
                    
                current_price = data['prices'][-1]
                trend_strength = self._calculate_trend_strength(np.array(data['prices']))
                correlation = self.get_correlation(major_symbol, symbol)
                
                # 检查趋势是否与主流币一致
                if np.sign(trend_strength) == np.sign(major_trend) and abs(trend_strength) >= 0.3:
                    signals.append({
                        'symbol': symbol,
                        'action': f'open_{major_direction}',  # 跟随主流币方向
                        'price': current_price,
                        'trend_strength': trend_strength,
                        'correlation': correlation,
                        'reason': f"与{major_symbol}相关性排名靠前，关系数: {correlation:.2f}, 趋势一致"
                    })
            
            # 信号已经按相关性排序（因为输入的币种列表就是按相关性排序的）
            return signals
            
        except Exception as e:
            logger.error(f"分析相关币种机会失败: {str(e)}")
            return []

    def _calculate_trend_strength(self, prices: np.ndarray) -> float:
        """计算趋势强度"""
        try:
            if len(prices) < 2:
                return 0.0

            # 计算价格变化
            price_changes = np.diff(prices) / prices[:-1]
            
            # 计算趋势方向
            up_moves = np.sum(price_changes > 0)
            down_moves = np.sum(price_changes < 0)
            total_moves = len(price_changes)
            
            # 计算主导趋势的强度 (-1 到 1 之间)
            if total_moves > 0:
                trend_direction = (up_moves - down_moves) / total_moves
            else:
                trend_direction = 0
                
            # 计算价格变化的标准差作为波动性指标
            volatility = np.std(price_changes) if len(price_changes) > 0 else 0
            
            # 计算价格变化的累积效应
            cumulative_change = (prices[-1] - prices[0]) / prices[0]
            
            # 综合计算趋势强度
            # 1. trend_direction 反映趋势一致性 (-1 到 1)
            # 2. volatility 反映波动性
            # 3. cumulative_change 反映整体变化幅度
            trend_strength = (
                abs(trend_direction) * 0.4 +  # 趋势方向权重
                min(abs(cumulative_change), 0.1) * 5 +  # 累积变化权重
                min(volatility, 0.1) * 2  # 波动性权重
            ) * np.sign(trend_direction)  # 保持趋势方向
            
            return float(trend_strength)
            
        except Exception as e:
            logger.error(f"计算趋势强度失败: {str(e)}")
            return 0.0

    def _determine_signal_type(self, price_change: float, volume_change: float, trend_strength: float) -> str:
        """根据价格变化、成交量变化和趋势强度确定信号类型"""
        try:
            # 设置信号阈值
            PRICE_THRESHOLD = 0.02  # 2%
            VOLUME_THRESHOLD = 1.5  # 150%
            TREND_THRESHOLD = 0.3   # 趋势强阈值
            
            if abs(trend_strength) < TREND_THRESHOLD:
                return 'neutral'
                
            if price_change > PRICE_THRESHOLD and volume_change > VOLUME_THRESHOLD:
                return 'strong_buy' if trend_strength > 0 else 'strong_sell'
            elif price_change > PRICE_THRESHOLD/2 or volume_change > VOLUME_THRESHOLD/2:
                return 'weak_buy' if trend_strength > 0 else 'weak_sell'
            else:
                return 'neutral'
                
        except Exception as e:
            logger.error(f"确定信号类型失败: {str(e)}")
            return 'neutral'

    async def get_no_signal_reasons(self) -> str:
        """获取未成交易信号的原因"""
        reasons = []
        
        # 检查数据是否足够
        if not self.returns_data or len(self.returns_data) < 2:
            reasons.append("历史数据不足")
            return "; ".join(reasons)
            
        # 检查价格变化
        for symbol in self.returns_data.keys():
            latest_price = self.market_data.latest_prices.get(symbol)
            if not latest_price:
                continue
                
            # 计算价格变化
            price_change = self._calculate_price_change(symbol)
            if price_change is None:
                continue
                
            # 检查成交量
            volume_spike = self._detect_volume_spike(symbol, latest_price.get('volume', 0))
            
            # 获取相关性趋势
            corr_trends = self._get_correlated_trends(symbol)
            avg_corr_trend = sum(t[1] for t in corr_trends) / len(corr_trends) if corr_trends else 0
            
            # 添加不满足条件的原因
            if abs(price_change) < self.signal_thresholds['price_change']:
                reasons.append(f"{symbol} 价格变化({price_change*100:.2f}%)未达到阈值({self.signal_thresholds['price_change']*100}%)")
            
            if not volume_spike:
                reasons.append(f"{symbol} 成交量未达到异常水平")
            
            if abs(avg_corr_trend) < self.signal_thresholds['trend_strength']:
                reasons.append(f"{symbol} 趋势强度({abs(avg_corr_trend):.2f})未达到阈值({self.signal_thresholds['trend_strength']})")
        
        if not reasons:
            reasons.append("所有交易对均未满足交易条件")
            
        return "; ".join(reasons)

    def _is_first_position(self, symbol: str) -> bool:
        """
        检查是否是首次开仓
        :param symbol: 交易对
        :return: 是否是首次开仓
        """
        return symbol not in self.positions or not self.positions[symbol]

    def update_position(self, symbol: str, action: str, price: float, size: float) -> None:
        """
        更新仓位信息
        :param symbol: 交易对
        :param action: 操作类型 ('open_long', 'open_short', 'close')
        :param price: 价格
        :param size: 数量
        """
        if symbol not in self.positions:
            self.positions[symbol] = []
            self.position_history[symbol] = []
        
        if action == 'close':
            if self.positions[symbol]:
                closed_position = self.positions[symbol].pop()
                closed_position['close_price'] = price
                closed_position['close_time'] = time.time()
                closed_position['pnl'] = (price - closed_position['entry_price']) * size * (1 if closed_position['type'] == 'long' else -1)
                self.position_history[symbol].append(closed_position)
                
                # 如果是BTC/ETH仓位被平掉，同时平掉其相关币种的仓位
                if symbol in ['BTCUSDT', 'ETHUSDT']:
                    self._close_correlated_positions(symbol, price)
        else:
            position = {
                'type': 'long' if action == 'open_long' else 'short',
                'entry_price': price,
                'size': size,
                'entry_time': time.time(),
                'leverage': 20,  # 初始20倍杠杆
                'is_major': symbol in ['BTCUSDT', 'ETHUSDT'],
                'profit_protected': False,  # 是否已经保护了利润
                'released_margin': 0,  # 已释放的保证金
                'major_type': 'BTC' if symbol == 'BTCUSDT' else ('ETH' if symbol == 'ETHUSDT' else None)  # 标记主流币类型
            }
            self.positions[symbol].append(position)
            
    def _close_correlated_positions(self, major_symbol: str, current_price: float) -> None:
        """
        平掉与主流币相关的所有仓位
        :param major_symbol: 主流币交易对
        :param current_price: 当前价格
        """
        # 获取相关币种
        correlated_symbols = self._get_correlated_symbols(major_symbol)
        major_type = 'BTC' if major_symbol == 'BTCUSDT' else 'ETH'
        
        # 平掉所有相关币种的仓位（只平掉与该主流币相关的仓位）
        for symbol in correlated_symbols:
            if symbol in self.positions and self.positions[symbol]:
                position = self.positions[symbol][0]
                # 只平掉与当前主流币关联的仓位
                if position.get('major_type') == major_type:
                    self.update_position(symbol, 'close', current_price, position['size'])
                    logger.info(f"由于{major_symbol}仓位平仓，关闭平仓{symbol}")
                
    def adjust_position_leverage(self, symbol: str, current_price: float) -> Dict[str, Any]:
        """
        调整仓位杠杆
        :param symbol: 交易对
        :param current_price: 当前价格
        :return: 调整结果
        """
        if symbol not in self.positions or not self.positions[symbol]:
            return {'adjusted': False, 'reason': '无持仓'}
            
        position = self.positions[symbol][0]
        entry_price = position['entry_price']
        
        # 计算盈利比例
        profit_ratio = (current_price - entry_price) / entry_price
        
        # 如果盈利达到2%且未保护利润
        if profit_ratio >= 0.02 and not position['profit_protected']:
            old_leverage = position['leverage']
            
            # 根据不同币种设置新的杠杆倍数
            if symbol == 'BTCUSDT':
                new_leverage = 125  # BTC最大125倍
            elif symbol == 'ETHUSDT':
                new_leverage = 100  # ETH最大100倍
            else:
                new_leverage = min(old_leverage * 2.5, 50)  # 其他币种最多提升2.5倍，上限50倍
                
            # 计算可以释放的保证金
            position_value = position['size'] * current_price
            old_margin = position_value / old_leverage
            new_margin = position_value / new_leverage
            released_margin = old_margin - new_margin
            
            # 更新仓位信息
            position['leverage'] = new_leverage
            position['profit_protected'] = True
            position['released_margin'] = released_margin
            
            return {
                'adjusted': True,
                'old_leverage': old_leverage,
                'new_leverage': new_leverage,
                'released_margin': released_margin,
                'profit_ratio': profit_ratio,
                'major_type': position.get('major_type')  # 返回主流币类型
            }
            
        return {'adjusted': False, 'reason': '未达到调整条件'}

    def get_position_status(self, symbol: str) -> Dict[str, Any]:
        """
        获取仓位状态
        :param symbol: 交易对
        :return: 仓位状态信息
        """
        if symbol not in self.positions:
            return {'has_position': False, 'first_position_active': False}
        
        positions = self.positions[symbol]
        return {
            'has_position': bool(positions),
            'first_position_active': any(p['is_first'] for p in positions),
            'total_positions': len(positions),
            'current_positions': positions
        }

    def get_market_analysis(self, market_data: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """分析市场数据，返回分析结果"""
        try:
            analysis_results = []
            current_timestamp = int(time.time() * 1000)
            
            # 从CSV文件读取5分钟K线数据
            history_dir = 'cache/history'
            for file in os.listdir(history_dir):
                if not file.endswith('_5m.csv'):
                    continue
                    
                symbol = file.replace('_5m.csv', '')
                file_path = os.path.join(history_dir, file)
                
                try:
                    # 读取CSV数据
                    df = pd.read_csv(file_path)
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    
                    if len(df) < 2:
                        continue
                        
                    # 获取最新数据
                    latest = df.iloc[-1]
                    prev = df.iloc[-2]
                    
                    # 计算指标
                    price = float(latest['close'])
                    price_change = (price - float(prev['close'])) / float(prev['close'])
                    volume = float(latest['volume'])
                    
                    # 计算趋势强度
                    trend_strength = self._calculate_trend_strength(df['close'].values)
                    
                    # 计算RSI
                    rsi = self._calculate_rsi(df['close'].values)
                    
                    # 计算布林带
                    bb_middle, bb_upper, bb_lower = self._calculate_bollinger_bands(df['close'].values)
                    
                    # 计算波动率 - 使用最近20个周期的数据
                    prices = df['close'].values[-20:]
                    returns = np.diff(prices) / prices[:-1]
                    volatility = float(np.std(returns) * np.sqrt(288))  # 年化波动率 (288 = 24小时 * 12个5分钟)
                    
                    # 生成分析结果
                    analysis = {
                        'symbol': symbol,
                        'price': price,
                        'price_change': price_change,
                        'volume': volume,
                        'trades': int(latest.get('trades', 0)),
                        'updates': int(latest.get('updates', 0)),
                        'high': float(latest['high']),
                        'low': float(latest['low']),
                        'quote_volume': float(latest.get('quote_volume', 0)),
                        'tick_count': int(latest.get('tick_count', 0)),
                        'trend_strength': trend_strength,
                        'volatility': volatility,  # 年化波动率
                        'rsi': rsi,
                        'bollinger_middle': bb_middle[-1] if len(bb_middle) > 0 else None,
                        'bollinger_upper': bb_upper[-1] if len(bb_upper) > 0 else None,
                        'bollinger_lower': bb_lower[-1] if len(bb_lower) > 0 else None,
                        'timestamp': current_timestamp
                    }
                    
                    analysis_results.append(analysis)
                    
                except Exception as e:
                    logger.error(f"分析 {symbol} 数据失败: {str(e)}")
                    continue
            
            return {
                'timestamp': current_timestamp,
                'data': analysis_results
            }
            
        except Exception as e:
            logger.error(f"获取市场分析数据失败: {str(e)}")
            return {
                'timestamp': int(time.time() * 1000),
                'data': []
            }

    def _get_correlated_trends(self, symbol: str) -> List[Tuple[str, float]]:
        """获取相关交易对的趋势
        
        Args:
            symbol: 交易对符号
            
        Returns:
            [(相关交易对, 趋势得分)]
        """
        try:
            if self.correlation_matrix is None or symbol not in self.correlation_matrix.index:
                return []
                
            # 获取相关性高的交易对
            correlated_pairs = self.correlation_matrix[symbol][
                self.correlation_matrix[symbol] > self.signal_thresholds['min_correlation']
            ].index.tolist()
            
            trends = []
            for pair in correlated_pairs:
                if pair in self.market_data.latest_prices:
                    pair_change = self._calculate_price_change(pair)
                    if pair_change is not None:
                        correlation = self.correlation_matrix.loc[symbol, pair]
                        trend_score = pair_change * np.sign(correlation)
                        trends.append((pair, trend_score))
                        
            return trends
            
        except Exception as e:
            logger.error(f"获取相关趋势失败: {str(e)}")
            return []

    async def market_analysis_loop(self):
        """市场分析循环"""
        while True:
            try:
                # 确保初始化
                if not self._initialized:
                    logger.warning("相关性管理器尚未初始化，等待初始化完成")
                    await asyncio.sleep(5)
                    continue

                # 获取市场分析结果
                market_analysis = self.get_market_analysis()  # 不需要传递参数，使用内部数据

                # 输出分析结果
                if market_analysis:
                    total_volume = sum(data.get('volume_change', 0) for data in market_analysis)
                    avg_price_change = np.mean([data['price_change'] for data in market_analysis])
                    avg_trend_strength = np.mean([data['trend_strength'] for data in market_analysis])
                    
                    logger.info("\n=== 市场摘要 ===")
                    logger.info(f"总成交量: {total_volume:,.2f}")
                    logger.info(f"平均涨跌幅: {avg_price_change*100:.2f}%")
                    logger.info(f"平均趋势强度: {avg_trend_strength:.2f}")
                    logger.info(f"接收到信号 {len(market_analysis)}")
                    
                    logger.info("\n=== 市场分析结果 ===")
                    logger.info(f"{'交易对':^12} {'价格':^10} {'价格变化':^10} {'成交量变化':^10} {'趋势强度':^10} {'信号类型'}")
                    logger.info("-" * 80)
                    
                    for data in market_analysis:
                        logger.info(
                            f"{data['symbol']:^12} {data['price']:>10.4f} "
                            f"{data['price_change']*100:>9.2f}% {data.get('volume_change', 0)*100:>9.2f}% "
                            f"{data['trend_strength']:>10.2f} {data.get('signal_type', 'neutral')}"
                        )
                else:
                    logger.debug("没有可用的市场数据进行分析")

                # 更新相关性矩阵
                await self.update_correlation_matrix()
                
                # 等待下一次���析
                await asyncio.sleep(60)  # 每分钟分析一次
                
            except Exception as e:
                logger.error(f"市场分析循环出错: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                await asyncio.sleep(5)  # 出错后等待5秒再继续

    async def update_correlation_matrix(self):
        """更新相关性矩阵"""
        try:
            if not self.realtime_data:
                logger.warning("没有可用的实时数据")
                return
                
            # 创建价格变化率数据
            returns_data = {}
            for symbol, data in self.realtime_data.items():
                if len(data['prices']) >= 2:
                    # 计算收益率
                    prices = np.array(data['prices'])
                    returns = np.diff(prices) / prices[:-1]
                    returns_data[symbol] = returns
                    
            if len(returns_data) < 2:
                logger.warning("没有足够的数据来计算相关性")
                return
                
            # 创建DataFrame并计算相关性
            returns_df = pd.DataFrame(returns_data)
            self.correlation_matrix = returns_df.corr()
            
            # 记录更��时间
            self.last_update = time.time()
            
            logger.debug(f"相关性矩阵更新成功，包含 {len(self.correlation_matrix)} 个交易对")
            
        except Exception as e:
            logger.error(f"更新相关性矩阵失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    def get_correlated_pairs(self, symbol: str, min_correlation: float = 0.5) -> List[Dict[str, Any]]:
        """获取与指定交易对相关性高的其他交易对
        
        Args:
            symbol: 交易对
            min_correlation: 最小相关系数
            
        Returns:
            List[Dict]: 相关交易对列表，每个字典包含 symbol 和 correlation
        """
        try:
            if self.correlation_matrix is None or symbol not in self.correlation_matrix.index:
                return []
                
            # 获取相关系数
            correlations = self.correlation_matrix[symbol]
            
            # 筛选高相关性的交易对
            high_corr = correlations[correlations.abs() > min_correlation]
            
            # 转换为列表格式
            correlated_pairs = []
            for pair, corr in high_corr.items():
                if pair != symbol:  # 排除自身
                    correlated_pairs.append({
                        'symbol': pair,
                        'correlation': float(corr)
                    })
                    
            # 按相关系数绝对值排序
            correlated_pairs.sort(key=lambda x: abs(x['correlation']), reverse=True)
            
            return correlated_pairs
            
        except Exception as e:
            logger.error(f"获取相关交易对失败: {str(e)}")
            return []

    def get_market_status(self) -> Dict[str, Any]:
        """获取市场状态概览"""
        try:
            if not self.realtime_data:
                return {'status': 'no_data'}
                
            # 算整体市场趋势
            trends = []
            volumes = []
            volatilities = []
            
            for symbol, data in self.realtime_data.items():
                if len(data['prices']) >= 2:
                    # 计算趋势
                    prices = np.array(data['prices'])
                    trend = self._calculate_trend_strength(prices)
                    trends.append(trend)
                    
                    # 计算成交量变化
                    volumes_arr = np.array(data['volumes'])
                    volume_change = (volumes_arr[-1] - volumes_arr[0]) / volumes_arr[0]
                    volumes.append(volume_change)
                    
                    # 计算波动率
                    returns = np.diff(prices) / prices[:-1]
                    volatility = np.std(returns)
                    volatilities.append(volatility)
            
            if not trends:
                return {'status': 'insufficient_data'}
                
            # 计算市场指标
            market_status = {
                'status': 'active',
                'timestamp': int(time.time() * 1000),
                'overall_trend': float(np.mean(trends)),
                'trend_strength': float(np.std(trends)),
                'volume_activity': float(np.mean(volumes)),
                'market_volatility': float(np.mean(volatilities)),
                'active_pairs': len(self.realtime_data)
            }
            
            # 添加市场状态描述
            market_status['market_condition'] = self._determine_market_condition(market_status)
            
            return market_status
            
        except Exception as e:
            logger.error(f"获取市场状态失败: {str(e)}")
            return {'status': 'error', 'error': str(e)}
            
    def _determine_market_condition(self, status: Dict[str, Any]) -> str:
        """根据市场指标确定市场状态"""
        try:
            trend = status['overall_trend']
            volatility = status['market_volatility']
            volume = status['volume_activity']
            
            # 定义阈值
            HIGH_VOLATILITY = 0.002  # 0.2%
            HIGH_VOLUME = 1.5        # 150%
            TREND_THRESHOLD = 0.3    # 趋势强度阈值
            
            conditions = []
            
            # 判断趋势
            if abs(trend) > TREND_THRESHOLD:
                conditions.append('强势' if trend > 0 else '弱势')
            else:
                conditions.append('盘整')
                
            # 判断波动性
            if volatility > HIGH_VOLATILITY:
                conditions.append('高波动')
            else:
                conditions.append('低波动')
                
            # 判断成交量
            if abs(volume) > HIGH_VOLUME:
                conditions.append('放量')
            else:
                conditions.append('缩量')
                
            return ' '.join(conditions)
            
        except Exception as e:
            logger.error(f"判断市场状态失败: {str(e)}")
            return '未知'