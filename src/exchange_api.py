import ccxt
from typing import Dict, Any, Optional
from loguru import logger
from simulation_account import SimulationAccount

class ExchangeAPI:
    def __init__(self, 
                 exchange_id: str = 'binance',
                 api_key: str = None,
                 api_secret: str = None,
                 simulation_mode: bool = False,
                 simulation_balance: float = 1000.0):
        """
        交易所API接口初始化
        :param exchange_id: 交易所ID
        :param api_key: API密钥
        :param api_secret: API密钥
        :param simulation_mode: 是否为模拟模式
        :param simulation_balance: 模拟账户初始余额
        """
        self.simulation_mode = simulation_mode
        
        if simulation_mode:
            self.simulation_account = SimulationAccount(initial_balance=simulation_balance)
            
        # 配置交易所连接
        exchange_config = {
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',  # 使用U本位合约
                'defaultMarginMode': 'isolated',  # 使用逐仓模式
                'defaultLeverage': 2,  # 默认杠杆倍数
                'adjustForTimeDifference': True
            },
            'proxies': {
                'http': 'http://127.0.0.1:4780',
                'https': 'http://127.0.0.1:4780'
            }
        }
            
        self.exchange = getattr(ccxt, exchange_id)(exchange_config)
        
    def get_balance(self) -> Dict[str, float]:
        """获取账户余额"""
        if self.simulation_mode:
            return {'USDT': self.simulation_account.get_balance()}
        try:
            return self.exchange.fetch_balance()
        except Exception as e:
            logger.error(f"获取余额失败: {str(e)}")
            return None
        
    def place_order(self, 
                   symbol: str,
                   order_type: str,
                   side: str,
                   amount: float,
                   price: float = None,
                   leverage: int = 1) -> Dict[str, Any]:
        """
        下单
        :param symbol: 交易对（必须是USDT永续合约）
        :param order_type: 订单类型 ('limit' or 'market')
        :param side: 方向 ('buy' or 'sell')
        :param amount: 数量
        :param price: 价格（限价单需要）
        :param leverage: 杠杆倍数
        :return: 订单信息
        """
        if not symbol.endswith('USDT'):
            logger.error(f"不支持的交易对: {symbol}，只支持USDT永续合约")
            return None
            
        try:
            if self.simulation_mode:
                current_price = price if price else self.get_market_price(symbol)
                return self.simulation_account.place_order(symbol, side, amount, current_price, leverage)
                
            params = {}
            if order_type == 'limit' and price is not None:
                order = self.exchange.create_limit_order(symbol, side, amount, price, params)
            else:
                order = self.exchange.create_market_order(symbol, side, amount, params)
            logger.info(f"下单成功: {order}")
            return order
        except Exception as e:
            logger.error(f"下单失败: {str(e)}")
            return None
            
    def cancel_order(self, order_id: str, symbol: str) -> bool:
        """
        取消订单
        :param order_id: 订单ID
        :param symbol: 交易对
        :return: 是否成功
        """
        if self.simulation_mode:
            logger.info("模拟模式不支持取消订单")
            return True
            
        try:
            self.exchange.cancel_order(order_id, symbol)
            logger.info(f"取消订单成功: {order_id}")
            return True
        except Exception as e:
            logger.error(f"取消订单失败: {str(e)}")
            return False
            
    def get_market_price(self, symbol: str) -> Optional[float]:
        """
        获取市场当前价格
        :param symbol: 交易对
        :return: 当前价格
        """
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            return ticker['last']
        except Exception as e:
            logger.error(f"获取市场价格失败: {str(e)}")
            return None
            
    def get_positions(self, symbol: str = None) -> Dict[str, Any]:
        """
        获取持仓信息
        :param symbol: 交易对（可选）
        :return: 持仓信息
        """
        if self.simulation_mode:
            if symbol:
                return self.simulation_account.get_position(symbol)
            return self.simulation_account.get_all_positions()
            
        try:
            positions = self.exchange.fetch_positions(symbol) if symbol else self.exchange.fetch_positions()
            return positions
        except Exception as e:
            logger.error(f"获取持仓信息失败: {str(e)}")
            return None 