import asyncio
import signal
import sys
from loguru import logger
from typing import Optional, List, Dict, Any
from market_data_manager import MarketDataManager
from correlation_manager import CorrelationManager
from api_server import APIServer
from simulation_account import SimulationAccount

class TradingSystem:
    def __init__(self):
        # 创建API服务器（它会自动创建其他所需的组件）
        self.api_server = APIServer()
        
        # 获取其他组件的引用
        self.market_data = self.api_server.market_data
        self.correlation = self.api_server.correlation_manager
        self.simulation_account = self.api_server.simulation_account
        
        self.running = False
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        
    async def initialize(self) -> bool:
        """初始化交易系统"""
        try:
            logger.info("正在初始化交易系统...")
            
            # 初始化市场数据管理器
            await self.market_data.initialize()
            
            # 初始化相关性管理器
            await self.correlation.initialize_data()
            
            # 启动API服务器
            await self.api_server.start()
            
            self.running = True
            return True
            
        except Exception as e:
            logger.error(f"初始化交易系统失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False
            
    async def stop(self):
        """停止交易系统"""
        try:
            logger.info("接收到停止信号，正在关闭交易系统...")
            self.running = False
            
            # 停止API服务器
            await self.api_server.stop()
            
            # 停止市场数据管理器
            await self.market_data.stop()
            
            # 清理资源
            tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
                
            await asyncio.gather(*tasks, return_exceptions=True)
            
            logger.info("程序退出")
            
        except Exception as e:
            logger.error(f"停止交易系统失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            
    async def process_signals(self):
        """处理交易信号"""
        try:
            # 获取交易信号
            signals = await self.correlation.get_trading_signals()
            
            for signal in signals:
                symbol = signal['symbol']
                current_price = signal['price']
                
                # 检查是否已有相同交易对的持仓
                has_position = any(p['symbol'] == symbol for p in self.simulation_account.positions)
                if has_position:
                    continue
                
                # 检查是否有足够的可用保证金
                available_margin = self.simulation_account.get_available_balance()
                if available_margin <= 0:
                    logger.info("没有足够的可用保证金")
                    continue
                
                # 计算仓位大小（使用20倍初始杠杆）
                position_value = available_margin * 20  # 20倍杠杆
                position_size = position_value / current_price
                
                # 执行交易
                if signal['action'] == 'open_long':
                    trade_result = self.simulation_account.place_order(
                        symbol=symbol,
                        side='buy',
                        size=position_size,
                        price=current_price
                    )
                    
                    if trade_result:
                        logger.info(
                            f"开仓成功 - "
                            f"交易对: {symbol}, "
                            f"价格: {current_price}, "
                            f"数量: {position_size}, "
                            f"杠杆: 20x"
                        )
                
                # 检查是否需要调整杠杆和释放保证金
                self._check_leverage_adjustment()
                
        except Exception as e:
            logger.error(f"处理交易信号失败: {str(e)}")
            
    def _check_leverage_adjustment(self):
        """检查并调整杠杆倍数"""
        try:
            # 获取最新价格
            latest_prices = self.market_data.latest_prices
            
            # 遍历所有持仓
            for position in self.simulation_account.positions:
                symbol = position['symbol']
                if symbol not in latest_prices:
                    continue
                    
                current_price = latest_prices[symbol]['price']
                
                # 调整杠杆
                adjustment = self.correlation.adjust_position_leverage(symbol, current_price)
                
                if adjustment['adjusted']:
                    logger.info(
                        f"{symbol} 调整杠杆 - "
                        f"从 {adjustment['old_leverage']}x 到 {adjustment['new_leverage']}x, "
                        f"释放保证金: {adjustment['released_margin']:.2f} USDT, "
                        f"当前盈利: {adjustment['profit_ratio']*100:.2f}%"
                    )
                    
                    # 如果是BTC/ETH，使用释放的保证金开新仓位
                    if symbol in ['BTCUSDT', 'ETHUSDT'] and adjustment['released_margin'] > 0:
                        # 获取相关币种的交易信号
                        correlated_signals = self._get_correlated_signals(symbol)
                        
                        for corr_signal in correlated_signals:
                            corr_symbol = corr_signal['symbol']
                            
                            # 检查是否已有持仓
                            has_position = any(p['symbol'] == corr_symbol for p in self.simulation_account.positions)
                            if has_position:
                                continue
                                
                            # 使用释放的保证金开新仓
                            position_value = adjustment['released_margin'] * 20  # 20倍杠杆
                            position_size = position_value / corr_signal['price']
                            
                            trade_result = self.simulation_account.place_order(
                                symbol=corr_symbol,
                                side='buy',
                                size=position_size,
                                price=corr_signal['price']
                            )
                            
                            if trade_result:
                                logger.info(
                                    f"使用释放保证金开新仓 - "
                                    f"交易对: {corr_symbol}, "
                                    f"价格: {corr_signal['price']}, "
                                    f"数量: {position_size}, "
                                    f"杠杆: 20x"
                                )
                                break  # 只开一个新仓位
                    
        except Exception as e:
            logger.error(f"调整杠杆失败: {str(e)}")
            
    def _get_correlated_signals(self, major_symbol: str) -> List[Dict[str, Any]]:
        """
        获取与主流币相关的交易信号
        :param major_symbol: 主流币交易对
        :return: 交易信号列表
        """
        try:
            # 获取相关币种
            correlated_symbols = self.correlation._get_correlated_symbols(major_symbol)
            
            # 获取这些币种的交易信号
            signals = []
            latest_prices = self.market_data.latest_prices
            
            for symbol in correlated_symbols:
                if symbol in latest_prices:
                    signals.append({
                        'symbol': symbol,
                        'price': latest_prices[symbol]['price'],
                        'action': 'open_long'  # 跟随主流币方向
                    })
            
            return signals
            
        except Exception as e:
            logger.error(f"获取相关币种信号失败: {str(e)}")
            return []
            
    async def check_positions(self):
        """检查持仓状态"""
        try:
            # 获取最新价格
            latest_prices = self.market_data.latest_prices
            
            # 更新持仓状态
            for position in self.simulation_account.positions:
                symbol = position['symbol']
                if symbol not in latest_prices:
                    continue
                    
                current_price = latest_prices[symbol]['price']
                
                # 更新止损点
                self.risk_manager.update_stop_loss(
                    symbol=symbol,
                    current_price=current_price,
                    position=position
                )
                
                # 检查是否需要平仓
                if position['type'] == 'long':
                    if current_price <= position['stop_loss']:
                        pnl = (current_price - position['entry_price']) * position['size']
                        self.simulation_account.close_position(symbol, current_price)
                        logger.info(f"{symbol} 多仓触及止损点，平仓，盈亏: {pnl:.2f}")
                        
                elif position['type'] == 'short':
                    if current_price >= position['stop_loss']:
                        pnl = (position['entry_price'] - current_price) * position['size']
                        self.simulation_account.close_position(symbol, current_price)
                        logger.info(f"{symbol} 空仓触及止损点，平仓，盈亏: {pnl:.2f}")
                        
        except Exception as e:
            logger.error(f"检查持仓状态失败: {str(e)}")
            
    async def run(self):
        """运行交易系统"""
        self.running = True
        logger.info("交易系统开始运行")
        
        try:
            while self.running:
                # 更新市场数据
                await self.correlation.update_data()
                
                # 检查持仓状态
                await self.check_positions()
                
                # 处理交易信号
                await self.process_signals()
                
                # 等待1秒
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"交易系统运行错误: {str(e)}")
            self.running = False
            
        logger.info("交易系统停止运行")
        
def signal_handler(signum, frame):
    """信号处理函数"""
    logger.info(f"接收到信号 {signum}")
    if system.loop and system.running:
        system.loop.create_task(system.stop())
        
async def main():
    """主函数"""
    try:
        # 设置信号处理
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # 初始化系统
        if not await system.initialize():
            logger.error("初始化失败，退出程序")
            return
            
        # 保持程序运行
        while system.running:
            await asyncio.sleep(1)
            
    except Exception as e:
        logger.error(f"程序运行错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        if system.running:
            await system.stop()
            
if __name__ == "__main__":
    # 创建系统实例
    system = TradingSystem()
    
    # 使用新的事件循环初始化方式
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    system.loop = loop
    
    try:
        # 运行主函数
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("接收到键盘中断信号")
    except Exception as e:
        logger.error(f"程序异常退出: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # 关闭事件循环
        try:
            loop.close()
        except Exception as e:
            logger.error(f"关闭事件循环失败: {str(e)}")
            
        sys.exit(0) 