from typing import Dict, Any
from loguru import logger
import ccxt

class PositionManager:
    def __init__(self, exchange_id: str = 'binance'):
        """
        仓位管理模块初始化（专注于USDT永续合约）
        :param exchange_id: 交易所ID
        """
        # 配置交易所连接
        exchange_config = {
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',  # 使用U本位合约
                'defaultMarginMode': 'isolated',  # 使用逐仓模式
                'defaultLeverage': 5,  # 默认杠杆倍数
                'adjustForTimeDifference': True
            },
            'proxies': {
                'http': 'http://127.0.0.1:4780',
                'https': 'http://127.0.0.1:4780'
            }
        }
        self.exchange = getattr(ccxt, exchange_id)(exchange_config)
        self.positions = {}  # 当前持仓信息
        self.max_leverage = 5  # 最大杠杆倍数
        
    def open_position(self, symbol: str, side: str, amount: float, leverage: int = 1) -> Dict[str, Any]:
        """
        开仓（仅支持USDT永续合约）
        :param symbol: 交易对
        :param side: 方向 ('long' or 'short')
        :param amount: 数量
        :param leverage: 杠杆倍数
        :return: 订单信息
        """
        try:
            if not symbol.endswith('USDT'):
                raise ValueError("只支持USDT永续合约")
                
            # 检查并限制杠杆倍数
            if leverage > self.max_leverage:
                logger.warning(f"杠杆倍数 {leverage} 超过最大限制 {self.max_leverage}，将使用最大限制")
                leverage = self.max_leverage
                
            # 设置杠杆
            self.exchange.fapiPrivatePostLeverage({
                'symbol': symbol,
                'leverage': leverage
            })
            
            # 设置逐仓模式
            self.exchange.fapiPrivatePostMarginType({
                'symbol': symbol,
                'marginType': 'ISOLATED'
            })
            
            # 创建市价单
            order = self.exchange.create_market_order(
                symbol=symbol,
                side='buy' if side == 'long' else 'sell',
                amount=amount,
                params={'reduceOnly': False}
            )
            
            # 更新持仓信息
            self.positions[symbol] = {
                'side': side,
                'amount': amount,
                'leverage': leverage,
                'entry_price': order['price'],
                'order_id': order['id']
            }
            
            logger.info(f"开仓成功: {symbol} {side} {amount}x{leverage}")
            return order
            
        except Exception as e:
            logger.error(f"开仓失败: {str(e)}")
            return None
            
    def adjust_leverage(self, symbol: str, new_leverage: int) -> bool:
        """
        调整杠杆（仅支持USDT永续合约）
        :param symbol: 交易对
        :param new_leverage: 新的杠杆倍数
        :return: 是否成功
        """
        try:
            if not symbol.endswith('USDT'):
                raise ValueError("只支持USDT永续合约")
                
            # 检查并限制杠杆倍数
            if new_leverage > self.max_leverage:
                logger.warning(f"杠杆倍数 {new_leverage} 超过最大限制 {self.max_leverage}，将使用最大限制")
                new_leverage = self.max_leverage
                
            # 设置新的杠杆
            self.exchange.fapiPrivatePostLeverage({
                'symbol': symbol,
                'leverage': new_leverage
            })
            
            # 更新持仓信息
            if symbol in self.positions:
                self.positions[symbol]['leverage'] = new_leverage
                
            logger.info(f"调整杠杆成功: {symbol} -> {new_leverage}x")
            return True
            
        except Exception as e:
            logger.error(f"调整杠杆失败: {str(e)}")
            return False