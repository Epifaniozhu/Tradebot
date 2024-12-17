import asyncio
import os
if os.name == 'nt':
    from asyncio import WindowsSelectorEventLoopPolicy
    asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())

from market_data_manager import MarketDataManager
from loguru import logger
import time
from datetime import datetime
from collections import defaultdict
import pandas as pd
import numpy as np
from tabulate import tabulate
import signal
import sys

class MarketMonitor:
    def __init__(self):
        self.manager = MarketDataManager()
        self.running = True
        self.stats = defaultdict(lambda: {
            'trades': 0,
            'volume': 0.0,
            'price_updates': 0,
            'last_price': None,
            'price_change_1m': 0.0,
            'volume_1m': 0.0
        })
        self.start_time = time.time()
        
        # 从realtime目录读取实时数据
        realtime_dir = 'cache/history/realtime'
        if os.path.exists(realtime_dir):
            for file in os.listdir(realtime_dir):
                if file.endswith('_trades.csv'):
                    try:
                        symbol = file.replace('_trades.csv', '')
                        file_path = os.path.join(realtime_dir, file)
                        df = pd.read_csv(file_path)
                        
                        if len(df) > 0:
                            latest = df.iloc[-1]
                            self.stats[symbol].update({
                                'last_price': float(latest['price']),
                                'volume': float(latest['quantity']),
                                'trades': 1,
                                'price_updates': 1
                            })
                    except Exception as e:
                        logger.error(f"读取 {file} 失败: {str(e)}")
                        continue
        
        # 设置信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    async def start(self):
        """启动市场监控"""
        try:
            # 初始化数据管理器
            logger.info("正在初始化市场数据管理器...")
            try:
                if not await self.manager.initialize():
                    logger.error("市场数据管理器初始化失败")
                    return
            except Exception as e:
                logger.error(f"市场数据管理器初始化错误: {str(e)}")
                import traceback
                logger.error(f"错误详情:\n{traceback.format_exc()}")
                return
                
            # 启动监控任务
            logger.info("启动市场监控...")
            try:
                await asyncio.gather(
                    self.monitor_connections(),
                    self.monitor_data_flow(),
                    self.display_statistics()
                )
            except Exception as e:
                logger.error(f"监控任务执行错误: {str(e)}")
                import traceback
                logger.error(f"错误详情:\n{traceback.format_exc()}")
                
        except Exception as e:
            logger.error(f"监控启动失败: {str(e)}")
            import traceback
            logger.error(f"错误详情:\n{traceback.format_exc()}")
        finally:
            await self.cleanup()
            
    def _signal_handler(self):
        """处理终止信号"""
        logger.info("接收到终止信号，准备清理...")
        self.running = False
        
    async def cleanup(self):
        """清理资源"""
        try:
            logger.info("正在清理资源...")
            await self.manager.cleanup()
            logger.info("资源清理完成")
        except Exception as e:
            logger.error(f"资源清理失败: {str(e)}")
            import traceback
            logger.error(f"错误详情:\n{traceback.format_exc()}")
            
    async def monitor_connections(self):
        """监控连接状态"""
        while self.running:
            try:
                active_tasks = len(self.manager.ws_tasks)
                logger.debug(f"活动WebSocket连接: {active_tasks}")
                
                if active_tasks == 0:
                    logger.warning("没有活动的WebSocket连接！")
                    
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"连接监控错误: {str(e)}")
                import traceback
                logger.error(f"错误详情:\n{traceback.format_exc()}")
                await asyncio.sleep(1)
                
    async def monitor_data_flow(self):
        """监控数据流"""
        while self.running:
            try:
                for symbol in self.manager.subscribed_symbols:
                    # 检查tick数据
                    tick_data = self.manager.real_time_cache['tick'].get(symbol, [])
                    if tick_data:
                        latest_tick = tick_data[-1]
                        self.update_stats(symbol, latest_tick)
                        
                    # 检查1分钟K线
                    kline_1m = self.manager.real_time_cache['1m'].get(symbol, [])
                    if kline_1m:
                        latest_kline = kline_1m[-1]
                        self.update_kline_stats(symbol, latest_kline)
                        
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"数据流监控错误: {str(e)}")
                import traceback
                logger.error(f"错误详情:\n{traceback.format_exc()}")
                await asyncio.sleep(1)
                
    def update_stats(self, symbol: str, tick: dict):
        """更新交易统计"""
        try:
            stats = self.stats[symbol]
            
            # 更新基本统计
            stats['trades'] += 1
            stats['volume'] += float(tick['quantity'])
            stats['price_updates'] += 1
            
            # 更新价格变化
            if stats['last_price'] is not None:
                price_change = (float(tick['price']) - stats['last_price']) / stats['last_price']
                stats['price_change_1m'] = price_change * 100  # 转换为百分比
                
            stats['last_price'] = float(tick['price'])
            
        except Exception as e:
            logger.error(f"更新统计失败 - {symbol}: {str(e)}")
            import traceback
            logger.error(f"错误详情:\n{traceback.format_exc()}")
            
    def update_kline_stats(self, symbol: str, kline: dict):
        """更新K线统计"""
        try:
            stats = self.stats[symbol]
            stats['volume_1m'] = float(kline['volume'])
            
        except Exception as e:
            logger.error(f"更新K线统计失败 - {symbol}: {str(e)}")
            import traceback
            logger.error(f"错误详情:\n{traceback.format_exc()}")
            
    async def display_statistics(self):
        """显示统计信息"""
        while self.running:
            try:
                # 准备显示数据
                display_data = []
                for symbol in sorted(self.stats.keys()):
                    stats = self.stats[symbol]
                    display_data.append([
                        symbol,
                        f"{stats['last_price']:.8f}" if stats['last_price'] is not None else "N/A",
                        f"{stats['price_change_1m']:.2f}%" if stats['price_change_1m'] != 0 else "0.00%",
                        f"{stats['volume_1m']:.2f}",
                        stats['trades'],
                        stats['price_updates']
                    ])
                
                # 清屏
                os.system('cls' if os.name == 'nt' else 'clear')
                
                # 显示表格
                print("\n=== 市场数据监控 ===")
                print(f"运行时间: {self.format_duration(time.time() - self.start_time)}")
                print(f"监控交易对: {len(self.stats)}")
                print(f"WebSocket连接: {len(self.manager.ws_tasks)}")
                print("\n")
                
                if display_data:
                    headers = ['交易对', '最新价格', '1分钟涨跌', '1分钟成交量', '成交笔数', '价格更新数']
                    print(tabulate(display_data, headers=headers, tablefmt='grid'))
                else:
                    print("等待数据...")
                
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"显示统计信息错误: {str(e)}")
                import traceback
                logger.error(f"错误详情:\n{traceback.format_exc()}")
                await asyncio.sleep(1)
                
    @staticmethod
    def format_duration(seconds: float) -> str:
        """格式化持续时间"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        seconds = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        
async def main():
    monitor = MarketMonitor()
    await monitor.start()
    
if __name__ == "__main__":
    try:
        # 配置日志
        logger.remove()  # 移除默认处理器
        logger.add(
            sys.stderr,
            format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            level="DEBUG"
        )
        logger.add(
            "logs/market_monitor_{time}.log",
            rotation="500 MB",
            retention="10 days",
            compression="zip",
            level="DEBUG"
        )
        
        # 运行主程序
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序异常退出: {str(e)}")
        import traceback
        logger.error(f"错误详情:\n{traceback.format_exc()}")
    finally:
        logger.info("程序结束") 