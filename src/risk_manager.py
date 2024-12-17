from typing import Dict, Any, Optional, List, Tuple
from loguru import logger
import pandas as pd
import numpy as np
from datetime import datetime

class RiskManager:
    def __init__(self, initial_balance: float = 1000.0, max_position_size: float = 0.5):
        """
        风险管理模块初始化（专注于USDT永续合约）
        :param initial_balance: 初始资金
        :param max_position_size: 最大仓位比例（占总资金的比例）
        """
        self.initial_balance = initial_balance
        self.base_position_size = max_position_size  # 基础仓位比例
        self.max_position_size = max_position_size  # 当前最大仓位比例
        self.base_max_drawdown = 0.2  # 基础最大回撤限制
        self.max_drawdown = 0.2  # 当前最大回撤限制
        self.base_risk_percentage = 0.01  # 基础风险比例
        self.risk_percentage = 0.01  # 当前风险比例
        self.stop_loss_points = {}  # 止损点
        self.take_profit_points = {}  # 止盈点
        self.position_sizes = {}  # 仓位大小
        self.total_position_limit = 0.8  # 总仓位限制
        self.correlation_threshold = 0.7  # 相关性阈值
        self.position_history = []  # 仓位历史记录
        self.volatility_data = {}  # 波动率数据
        self.market_state = {}  # 市场状态数据
        
        # 期货合约特定的风险控制参数
        self.max_concentration_ratio = 0.3  # 单个合约最大集中度
        self.min_liquidity_threshold = 1000000  # 最小流动性阈值（以USDT计）
        self.max_volatility_threshold = 0.5  # 最大可接受波动率
        self.max_leverage = 5  # 最大杠杆倍数
        self.volume_impact_threshold = 0.1  # 成交量影响阈值
        self.funding_rate_threshold = 0.001  # 资金费率阈值
        self.liquidation_buffer = 0.2  # 清算缓冲区（20%）
        
    def evaluate_risks(self, symbol: str, current_price: float, volume_24h: float) -> Dict[str, Any]:
        """
        评估期货合约风险
        :param symbol: 合约交易对
        :param current_price: 当前价格
        :param volume_24h: 24小时成交量
        :return: 风险评估结果
        """
        try:
            if not symbol.endswith('USDT'):
                return {'risk_level': 'high', 'reason': '只支持USDT永续合约'}
                
            # 获取波动率
            volatility = self.get_volatility(symbol)
            
            # 计算流动性风险
            liquidity_risk = volume_24h < self.min_liquidity_threshold
            
            # 计算波动率风险
            volatility_risk = volatility > self.max_volatility_threshold if volatility else True
            
            # 评估综合风险
            risk_factors = []
            
            if liquidity_risk:
                risk_factors.append('流动性不足')
            if volatility_risk:
                risk_factors.append('波动率过高')
                
            # 确定风险等级
            if len(risk_factors) >= 2:
                risk_level = 'high'
            elif len(risk_factors) == 1:
                risk_level = 'medium'
            else:
                risk_level = 'low'
                
            return {
                'risk_level': risk_level,
                'factors': risk_factors,
                'metrics': {
                    'volatility': volatility,
                    'volume_24h': volume_24h,
                    'current_price': current_price
                }
            }
            
        except Exception as e:
            logger.error(f"风险评估失败: {str(e)}")
            return {'risk_level': 'high', 'reason': f'评估失败: {str(e)}'}
        
    async def adjust_risk_parameters(self, symbol: str) -> None:
        """
        根据波动率自动调整风险参数
        :param symbol: 交易对
        """
        try:
            volatility = self.get_volatility(symbol)
            if not volatility:
                return
                
            # 记录市场状态
            self.market_state[symbol] = {
                'volatility': volatility,
                'timestamp': datetime.now()
            }
            
            # 根据波动率调整参数
            if volatility > 0.5:  # 高波动率
                self.max_position_size = self.base_position_size * 0.2  # 降低到20%
                self.max_drawdown = self.base_max_drawdown * 0.5  # 降低到10%
                self.risk_percentage = self.base_risk_percentage * 0.2  # 降低到0.2%
                logger.info(f"高波动率({volatility:.2f})，调整风险参数：仓位{self.max_position_size:.2f}，回撤{self.max_drawdown:.2f}")
                
            elif volatility > 0.25:  # 中等波动率
                self.max_position_size = self.base_position_size * 0.5  # 降低到50%
                self.max_drawdown = self.base_max_drawdown * 0.75  # 降低到15%
                self.risk_percentage = self.base_risk_percentage * 0.5  # 降低到0.5%
                logger.info(f"中等波动率({volatility:.2f})，调整风险参数：仓位{self.max_position_size:.2f}，回撤{self.max_drawdown:.2f}")
                
            else:  # 低波动率 (volatility <= 0.25)
                self.max_position_size = self.base_position_size  # 使用100%
                self.max_drawdown = self.base_max_drawdown  # 使用默认
                self.risk_percentage = self.base_risk_percentage  # 使用默认
                logger.info(f"低波动率({volatility:.2f})，使用默认风险参数")
                
        except Exception as e:
            logger.error(f"调整风险参数失败: {str(e)}")
            
    async def calculate_position_size(self, 
                                   symbol: str, 
                                   current_balance: float,
                                   current_price: float,
                                   risk_percentage: float = None,
                                   leverage: int = 1) -> float:
        """
        计算建仓数量
        :param symbol: 交易对
        :param current_balance: 当前余额
        :param current_price: 当前价格
        :param risk_percentage: 风险比例（可选，如果不提供则使用当前风险比例）
        :param leverage: 杠杆倍数
        :return: 建议的建仓数量
        """
        try:
            # 首先调整风险参数
            await self.adjust_risk_parameters(symbol)
            
            # 使用当前风险比例（如果没有提供）
            if risk_percentage is None:
                risk_percentage = self.risk_percentage
                
            # 检查杠杆是否超过限制
            if leverage > self.max_leverage:
                logger.warning(f"杠杆倍数 {leverage} 超过最大限制 {self.max_leverage}，将使用最大限制")
                leverage = self.max_leverage
                
            # 检查总仓位限制
            total_position_value = sum(size * price for symbol, (size, price) in self.position_sizes.items())
            available_position_value = current_balance * self.total_position_limit - total_position_value
            
            if available_position_value <= 0:
                logger.warning(f"达到总仓位限制，无法开新仓位")
                return 0
                
            # 获取波动率和其他风险指标
            volatility = self.get_volatility(symbol)
            if volatility is None:
                volatility = 0.5  # 使用保守的默认值
                
            # 计算风险调整系数
            risk_adjustments = {
                'volatility': 1.0 / (1.0 + volatility * 2),  # 波动率越高，系数越小
                'leverage': 1.0 / leverage,  # 杠杆越高，系数越小
                'correlation': 1.0  # 默认值，后面会根据相关性调整
            }
            
            # 相关性调整
            correlated_pairs = await self.get_correlated_pairs(symbol)
            if correlated_pairs:
                max_correlation = max(abs(corr) for _, corr in correlated_pairs)
                risk_adjustments['correlation'] = 1.0 - (max_correlation * 0.5)
                
            # 计算综合风险调整系数
            combined_risk_factor = min(
                risk_adjustments['volatility'],
                risk_adjustments['leverage'],
                risk_adjustments['correlation']
            )
            
            # 计算可用于该仓位的最大资金
            max_position_value = min(
                current_balance * self.max_position_size * combined_risk_factor,
                available_position_value
            )
            
            # 考虑杠杆后的实际可用资金
            effective_balance = max_position_value * leverage
            
            # 基于资金限制的仓位大小
            position_size_by_balance = effective_balance / current_price
            
            # 基于风险限制的仓位大小
            risk_amount = current_balance * risk_percentage * combined_risk_factor
            max_loss_percentage = 0.1  # 假设最大允许损失为10%
            position_size_by_risk = risk_amount / (current_price * max_loss_percentage)
            
            # 取两者中的较小值
            position_size = min(position_size_by_balance, position_size_by_risk)
            
            # 应用最小交易量限制（假设为0.001）
            if position_size < 0.001:
                logger.warning(f"计算的仓位 {position_size:.6f} 小于最小交易量限制 0.001")
                return 0
                
            # 记录仓位信息
            self.position_sizes[symbol] = (position_size, current_price)
            
            # 记录详细的计算过程
            logger.info(
                f"仓位计算详情:\n"
                f"- 可用资金: {available_position_value:.2f}\n"
                f"- 风险调整系数: {combined_risk_factor:.4f}\n"
                f"- 波动率调整: {risk_adjustments['volatility']:.4f}\n"
                f"- 杠杆调整: {risk_adjustments['leverage']:.4f}\n"
                f"- 相关性调整: {risk_adjustments['correlation']:.4f}\n"
                f"- 最终仓位: {position_size:.4f}"
            )
            
            return position_size
            
        except Exception as e:
            logger.error(f"计算建仓数量失败: {str(e)}")
            return 0
            
    def get_volatility(self, symbol: str, window: int = 20) -> float:
        """
        获取波动率
        :param symbol: 交易对
        :param window: 计算窗口
        :return: 波动率
        """
        try:
            # 如果没有历史数据，使用默认值
            if not hasattr(self, 'price_history'):
                self.price_history = {}
                
            if symbol not in self.price_history:
                self.price_history[symbol] = []
                
            # 计算波动率（如果有足够的历史数据）
            if len(self.price_history[symbol]) >= window:
                prices = np.array(self.price_history[symbol][-window:])
                returns = np.log(prices[1:] / prices[:-1])
                # 使用指数加权移动平均来增加对最近波动率的敏感度
                weights = np.exp(np.linspace(-1., 0., len(returns)))
                weights /= weights.sum()
                # 计算加权标准差
                variance = np.average((returns - returns.mean()) ** 2, weights=weights)
                volatility = np.sqrt(variance) * np.sqrt(365)  # 年化波动率
                
                # 更新波动率数据
                self.volatility_data[symbol] = volatility
                
                # 确保波动率数据被正确存储和使用
                self.market_state[symbol] = {
                    'volatility': volatility,
                    'timestamp': datetime.now()
                }
                
                return volatility
                
            return 0.2  # 默认波动率20%
            
        except Exception as e:
            logger.error(f"计算波动率失败: {str(e)}")
            return 0.2  # 出错时使用默认波动率
            
    def update_price_history(self, symbol: str, price: float) -> None:
        """
        更新价格历史
        :param symbol: 交易对
        :param price: 价格
        """
        if not hasattr(self, 'price_history'):
            self.price_history = {}
            
        if symbol not in self.price_history:
            self.price_history[symbol] = []
            
        self.price_history[symbol].append(price)
        
        # 保持最近1000个价格点
        if len(self.price_history[symbol]) > 1000:
            self.price_history[symbol] = self.price_history[symbol][-1000:]
            
        # 更新波动率
        if len(self.price_history[symbol]) >= 20:
            self.get_volatility(symbol)
        
    def check_correlation_limit(self, symbol: str, new_position_value: float) -> bool:
        """
        检查相关性限制
        :param symbol: 交易对
        :param new_position_value: 新仓位价值
        :return: 是否通过相关性检查
        """
        try:
            if not hasattr(self, 'price_history') or symbol not in self.price_history:
                return True
                
            # 检查是否有足够的历史数据
            if len(self.price_history[symbol]) < 20:
                return True
                
            # 计算与现有仓位的相关性
            current_positions = list(self.position_sizes.keys())
            if not current_positions:
                return True
                
            for pos_symbol in current_positions:
                if pos_symbol == symbol:
                    continue
                    
                if pos_symbol not in self.price_history:
                    continue
                    
                # 确保两个价格序列长度相同
                min_length = min(len(self.price_history[symbol]), len(self.price_history[pos_symbol]))
                if min_length < 20:
                    continue
                    
                # 计算相关系数
                prices1 = np.array(self.price_history[symbol][-min_length:])
                prices2 = np.array(self.price_history[pos_symbol][-min_length:])
                returns1 = np.log(prices1[1:] / prices1[:-1])
                returns2 = np.log(prices2[1:] / prices2[:-1])
                correlation = np.corrcoef(returns1, returns2)[0, 1]
                
                # 如果相关性过高，返回False
                if abs(correlation) > self.correlation_threshold:
                    logger.warning(f"{symbol}与{pos_symbol}的相关性为{correlation:.2f}，超过阈值{self.correlation_threshold}")
                    return False
                    
            return True
            
        except Exception as e:
            logger.error(f"检查相关性失败: {str(e)}")
            return True  # 出错时默认通过检查
        
    def set_dynamic_stop_loss(self,
                            symbol: str,
                            entry_price: float,
                            current_price: float,
                            position_type: str = 'long',
                            initial_risk: float = 0.02,  # 初始止损比例
                            trailing_callback: float = 0.01  # 移动止损回调比例
                            ) -> None:
        """
        设置动态止损
        :param symbol: 交易对
        :param entry_price: 入场价格
        :param current_price: 当前价格
        :param position_type: 仓位类型 ('long' or 'short')
        :param initial_risk: 初始止损比例
        :param trailing_callback: 移动止损回调比例
        """
        try:
            # 获取波动率
            volatility = self.get_volatility(symbol)
            if volatility:
                # 根据波动率调整止损比例
                initial_risk = max(initial_risk, volatility * 1.5)
                trailing_callback = max(trailing_callback, volatility * 0.5)
                
            if position_type == 'long':
                # 多仓止损点
                initial_stop = entry_price * (1 - initial_risk)
                trailing_stop = current_price * (1 - trailing_callback)
                stop_price = max(initial_stop, trailing_stop)
            else:
                # 空仓止损点
                initial_stop = entry_price * (1 + initial_risk)
                trailing_stop = current_price * (1 + trailing_callback)
                stop_price = min(initial_stop, trailing_stop)
                
            self.stop_loss_points[symbol] = {
                'price': stop_price,
                'type': 'trailing',
                'callback': trailing_callback,
                'entry_time': datetime.now(),
                'update_count': 0
            }
            
            logger.info(f"设置动态止损: {symbol} -> {stop_price:.2f}")
            
        except Exception as e:
            logger.error(f"设置动态止损失败: {str(e)}")
            
    def set_dynamic_take_profit(self,
                              symbol: str,
                              entry_price: float,
                              position_type: str = 'long',
                              risk_reward_ratio: float = 2.0  # 风险收益比
                              ) -> None:
        """
        设置动态止盈
        :param symbol: 交易对
        :param entry_price: 入场价格
        :param position_type: 仓位类型 ('long' or 'short')
        :param risk_reward_ratio: 风险收益比
        """
        try:
            if symbol not in self.stop_loss_points:
                logger.error(f"未设置止损点，无法设置止盈点: {symbol}")
                return
                
            stop_loss = self.stop_loss_points[symbol]['price']
            risk = abs(entry_price - stop_loss)
            
            # 根据波动率调整风险收益比
            volatility = self.get_volatility(symbol)
            if volatility:
                risk_reward_ratio = max(risk_reward_ratio, 1 + volatility * 3)
                
            target_profit = risk * risk_reward_ratio
            
            if position_type == 'long':
                take_profit = entry_price + target_profit
            else:
                take_profit = entry_price - target_profit
                
            self.take_profit_points[symbol] = {
                'price': take_profit,
                'type': 'dynamic',
                'levels': [  # 分批止盈
                    {'price': entry_price + target_profit * 0.5, 'percentage': 0.3},
                    {'price': entry_price + target_profit * 0.8, 'percentage': 0.3},
                    {'price': take_profit, 'percentage': 0.4}
                ]
            }
            
            logger.info(f"设置动态止盈: {symbol} -> {take_profit:.2f}")
            
        except Exception as e:
            logger.error(f"设置止盈点失败: {str(e)}")
            
    def update_stop_loss(self, symbol: str, current_price: float) -> None:
        """
        更新移动止损
        :param symbol: 交易对
        :param current_price: 当前价格
        """
        if symbol not in self.stop_loss_points:
            return
            
        stop_loss = self.stop_loss_points[symbol]
        if stop_loss['type'] != 'trailing':
            return
            
        # 检查持仓时间
        holding_time = datetime.now() - stop_loss['entry_time']
        callback = stop_loss['callback']
        current_stop = stop_loss['price']
        
        # 根据持仓时间调整回调比例
        if holding_time.days >= 1:
            callback = max(callback * 0.8, 0.005)  # 最小0.5%的回调
            
        # 更新移动止损
        new_stop = current_price * (1 - callback)
        if new_stop > current_stop:
            self.stop_loss_points[symbol]['price'] = new_stop
            self.stop_loss_points[symbol]['update_count'] += 1
            logger.info(f"更新移动止损: {symbol} -> {new_stop:.2f}")
            
    def check_risk_levels(self, current_prices: Dict[str, float]) -> Dict[str, Any]:
        """
        检查风险水平
        :param current_prices: 当前价格字典
        :return: 风险评估结果
        """
        risk_assessment = {}
        
        try:
            # 计算当前持仓市值
            current_position_value = sum(
                size * current_prices.get(symbol, price) 
                for symbol, (size, price) in self.position_sizes.items()
            )
            
            # 计算持仓成本
            position_cost = sum(
                size * price 
                for symbol, (size, price) in self.position_sizes.items()
            )
            
            # 计算当前权益
            current_equity = self.initial_balance + (current_position_value - position_cost)
            
            # 计算回撤
            drawdown = max(0, (self.initial_balance - current_equity) / self.initial_balance)
            
            # 检查最大回撤限制
            if drawdown > self.max_drawdown:
                logger.warning(f"达到最大回撤限制: {drawdown:.2%}")
                # 强制平仓所有仓位
                for symbol in self.position_sizes.keys():
                    if symbol in current_prices:
                        risk_assessment[symbol] = {
                            'symbol': symbol,
                            'current_price': current_prices[symbol],
                            'actions': [{
                                'type': 'stop_loss',
                                'trigger_price': current_prices[symbol],
                                'reason': f'触发最大回撤限制 ({drawdown:.2%})'
                            }]
                        }
                return risk_assessment
                
            # 检查各个交易对的风险
            for symbol, price in current_prices.items():
                assessment = {
                    'symbol': symbol,
                    'current_price': price,
                    'actions': []
                }
                
                # 检查止损
                if symbol in self.stop_loss_points:
                    stop_loss = self.stop_loss_points[symbol]
                    if price <= stop_loss['price']:
                        assessment['actions'].append({
                            'type': 'stop_loss',
                            'trigger_price': stop_loss['price'],
                            'reason': '触发止损'
                        })
                        
                # 检查分批止盈
                if symbol in self.take_profit_points:
                    take_profit = self.take_profit_points[symbol]
                    if take_profit['type'] == 'dynamic':
                        for level in take_profit['levels']:
                            if price >= level['price']:
                                assessment['actions'].append({
                                    'type': 'take_profit_partial',
                                    'trigger_price': level['price'],
                                    'percentage': level['percentage'],
                                    'reason': f"触发{level['percentage']*100}%止盈"
                                })
                    else:
                        if price >= take_profit['price']:
                            assessment['actions'].append({
                                'type': 'take_profit',
                                'trigger_price': take_profit['price'],
                                'reason': '触发止盈'
                            })
                        
                # 如果有任何需要采取的行动，添加到评估结果中
                if assessment['actions']:
                    risk_assessment[symbol] = assessment
                    
            return risk_assessment
            
        except Exception as e:
            logger.error(f"检查风险水平失败: {str(e)}")
            return risk_assessment
        
    def get_position_info(self, symbol: str) -> Dict[str, Any]:
        """
        获取仓位信息
        :param symbol: 交易对
        :return: 仓位信息
        """
        return {
            'size': self.position_sizes.get(symbol, (0, 0))[0],
            'stop_loss': self.stop_loss_points.get(symbol, None),
            'take_profit': self.take_profit_points.get(symbol, None)
        }
        
    def record_position(self, symbol: str, action: str, price: float, size: float) -> None:
        """
        记录仓位变动
        :param symbol: 交易对
        :param action: 操作类型 ('open', 'close')
        :param price: 价格
        :param size: 数量
        """
        self.position_history.append({
            'timestamp': datetime.now(),
            'symbol': symbol,
            'action': action,
            'price': price,
            'size': size
        })
        
    def _get_default_risk_assessment(self, symbol: str) -> Dict[str, Any]:
        """
        获取默认的风险评估结果
        :param symbol: 交易对
        :return: 默认风险评估结果
        """
        return {
            'symbol': symbol,
            'timestamp': datetime.now(),
            'risk_factors': {
                'volatility': {'value': 0.2, 'risk_level': 'low'},
                'liquidity': {'value': 0.0, 'risk_level': 'high'},
                'concentration': {'value': 0.0, 'risk_level': 'low'},
                'correlation': {'value': 0.0, 'risk_level': 'low'},
                'volume_impact': {'value': 0.0, 'risk_level': 'low'}
            },
            'overall_risk': 'high',
            'warnings': ['使用默认风险评估，建议谨慎操作']
        }

    async def evaluate_risks(self, symbol: str, current_price: float, volume_24h: float) -> Dict[str, Any]:
        """
        全面评估交易对风险
        :param symbol: 交易对
        :param current_price: 当前价格
        :param volume_24h: 24小时成交量
        :return: 风险评估结果
        """
        try:
            risk_assessment = await self._evaluate_risks_internal(symbol, current_price, volume_24h)
            if risk_assessment is None:
                logger.warning(f"风险评估失败，使用默认评估: {symbol}")
                return self._get_default_risk_assessment(symbol)
            return risk_assessment
        except Exception as e:
            logger.error(f"风险评估失败: {str(e)}")
            return self._get_default_risk_assessment(symbol)

    async def _evaluate_risks_internal(self, symbol: str, current_price: float, volume_24h: float) -> Optional[Dict[str, Any]]:
        """
        内部风险评估实现
        :param symbol: 交易对
        :param current_price: 当前价格
        :param volume_24h: 24小时成交量
        :return: 风险评估结果
        """
        try:
            risk_assessment = {
                'symbol': symbol,
                'timestamp': datetime.now(),
                'risk_factors': {},
                'overall_risk': 'low',
                'warnings': []
            }
            
            # 1. 波动率风险评估
            volatility = self.get_volatility(symbol)
            if volatility:
                risk_assessment['risk_factors']['volatility'] = {
                    'value': volatility,
                    'risk_level': 'high' if volatility > self.max_volatility_threshold else
                                'medium' if volatility > self.max_volatility_threshold * 0.5 else 'low'
                }
                if volatility > self.max_volatility_threshold:
                    risk_assessment['warnings'].append(f"波动率过高: {volatility:.2%}")
            
            # 2. 流动性风险评估
            liquidity_score = volume_24h * current_price
            risk_assessment['risk_factors']['liquidity'] = {
                'value': liquidity_score,
                'risk_level': 'high' if liquidity_score < self.min_liquidity_threshold else
                            'medium' if liquidity_score < self.min_liquidity_threshold * 2 else 'low'
            }
            if liquidity_score < self.min_liquidity_threshold:
                risk_assessment['warnings'].append(f"流动性不足: {liquidity_score:,.0f} USDT")
            
            # 3. 集中度风险评估
            if symbol in self.position_sizes:
                position_size, position_price = self.position_sizes[symbol]
                position_value = position_size * current_price
                total_position_value = sum(size * price for size, price in self.position_sizes.values())
                concentration_ratio = position_value / total_position_value if total_position_value > 0 else 0
                
                risk_assessment['risk_factors']['concentration'] = {
                    'value': concentration_ratio,
                    'risk_level': 'high' if concentration_ratio > self.max_concentration_ratio else
                                'medium' if concentration_ratio > self.max_concentration_ratio * 0.7 else 'low'
                }
                if concentration_ratio > self.max_concentration_ratio:
                    risk_assessment['warnings'].append(f"仓位集中度过高: {concentration_ratio:.2%}")
            
            # 4. 相关性风险评估
            correlated_pairs = await self.get_correlated_pairs(symbol)
            if correlated_pairs:
                high_correlation_count = sum(1 for _, corr in correlated_pairs if abs(corr) > self.correlation_threshold)
                risk_assessment['risk_factors']['correlation'] = {
                    'value': high_correlation_count,
                    'risk_level': 'high' if high_correlation_count > 2 else
                                'medium' if high_correlation_count > 0 else 'low'
                }
                if high_correlation_count > 2:
                    risk_assessment['warnings'].append(f"高相关性交易对过多: {high_correlation_count}个")
            
            # 5. 成交量影响评估
            if symbol in self.position_sizes:
                position_value = self.position_sizes[symbol][0] * current_price
                volume_impact = position_value / volume_24h if volume_24h > 0 else float('inf')
                risk_assessment['risk_factors']['volume_impact'] = {
                    'value': volume_impact,
                    'risk_level': 'high' if volume_impact > self.volume_impact_threshold else
                                'medium' if volume_impact > self.volume_impact_threshold * 0.5 else 'low'
                }
                if volume_impact > self.volume_impact_threshold:
                    risk_assessment['warnings'].append(f"成交量影响过大: {volume_impact:.2%}")
            
            # 评估整体风险水平
            risk_levels = [factor['risk_level'] for factor in risk_assessment['risk_factors'].values()]
            high_risk_count = sum(1 for level in risk_levels if level == 'high')
            medium_risk_count = sum(1 for level in risk_levels if level == 'medium')
            
            if high_risk_count > 0:
                risk_assessment['overall_risk'] = 'high'
            elif medium_risk_count > 1:
                risk_assessment['overall_risk'] = 'medium'
            else:
                risk_assessment['overall_risk'] = 'low'
            
            return risk_assessment
            
        except Exception as e:
            logger.error(f"内部风险评估失败: {str(e)}")
            return None

    async def get_correlated_pairs(self, symbol: str) -> List[Tuple[str, float]]:
        """
        获取与指定交易对相关的其他交易对
        :param symbol: 交易对
        :return: [(相关交易对, 相关系数)]
        """
        if not hasattr(self, 'correlation_manager'):
            from correlation_manager import CorrelationManager
            self.correlation_manager = CorrelationManager()
            await self.correlation_manager.initialize_data()
        
        return await self.correlation_manager.get_correlated_pairs(symbol)