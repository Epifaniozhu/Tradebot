import asyncio
import logging
import sys
from datetime import datetime
from typing import Dict, List

from api_server import APIServer
from market_data_manager import MarketDataManager
from correlation_manager import CorrelationManager
from simulation_account import SimulationAccount
from position_manager import PositionManager
from config import Config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('system_test.log')
    ]
)
logger = logging.getLogger(__name__)

class SystemTest:
    def __init__(self):
        self.config = Config()
        self.market_data_manager = None
        self.correlation_manager = None
        self.simulation_account = None
        self.position_manager = None
        self.api_server = None

    async def initialize_components(self):
        """初始化所有系统组件"""
        try:
            logger.info("第1步: 开始初始化系统组件...")
            
            # 初始化市场数据管理器
            logger.info("1.1: 初始化市场数据管理器...")
            self.market_data_manager = MarketDataManager()
            success = await self.market_data_manager.initialize()
            if not success:
                raise Exception("市场数据管理器初始化失败")
            logger.info("✓ 市场数据管理器初始化成功")

            # 初始化相关性管理器
            logger.info("1.2: 初始化相关性管理器...")
            self.correlation_manager = CorrelationManager(self.market_data_manager)
            await self.correlation_manager.initialize()
            logger.info("✓ 相关性管理器初始化成功")

            # 初始化模拟账户
            logger.info("1.3: 初始化模拟账户...")
            self.simulation_account = SimulationAccount()
            self.simulation_account.initialize_account("simulation")
            logger.info("✓ 模拟账户初始化成功")

            # 初始化仓位管理器
            logger.info("1.4: 初始化仓位管理器...")
            self.position_manager = PositionManager('binance')
            logger.info("✓ 仓位管理器初始化成功")

            # 初始化API服务器
            logger.info("1.5: 初始化API服务器...")
            self.api_server = APIServer(
                self.market_data_manager,
                self.correlation_manager,
                self.simulation_account
            )
            await self.api_server.initialize()
            logger.info("✓ API服务器初始化成功")

            logger.info("第1步完成: 所有系统组件初始化成功")
            return True

        except Exception as e:
            logger.error(f"系统初始化失败: {str(e)}")
            return False

    async def test_market_data_collection(self):
        """测试市场数据收集"""
        try:
            logger.info("第2步: 测试市场数据收集...")
            
            # 等待收集足够的市场数据
            logger.info("2.1: 等待收集实时市场数据...")
            await asyncio.sleep(60)  # 等待1分钟收集数据
            
            # 验证数据收集
            market_summary = self.market_data_manager.get_market_summary()
            if not market_summary or not market_summary['data']:
                raise Exception("未能收集到市场数据")
            
            # 检查BTC和ETH的数据
            btc_data = next((x for x in market_summary['data'] if x['symbol'] == 'BTCUSDT'), None)
            eth_data = next((x for x in market_summary['data'] if x['symbol'] == 'ETHUSDT'), None)
            
            if btc_data:
                logger.info(f"BTC数据 - 价格: {btc_data['price']}, 波动率: {btc_data.get('volatility', 'N/A')}")
            if eth_data:
                logger.info(f"ETH数据 - 价格: {eth_data['price']}, 波动率: {eth_data.get('volatility', 'N/A')}")
            
            logger.info(f"✓ 成功收集到 {len(market_summary['data'])} 个交易对的市场数据")
            logger.info("第2步完成: 市场数据收集正常")
            return True

        except Exception as e:
            logger.error(f"市场数据收集测试失败: {str(e)}")
            return False

    async def test_market_analysis(self):
        """测试市场分析"""
        try:
            logger.info("第3步: 测试市场分析...")
            
            # 获取市场分析结果
            logger.info("3.1: 执行市场分析...")
            analysis_result = await self.correlation_manager.get_market_analysis()
            
            if not analysis_result:
                raise Exception("未能获取市场分析结果")
            
            # 检查BTC和ETH的分析结果
            btc_analysis = next((x for x in analysis_result if 'BTCUSDT' in x['symbol']), None)
            eth_analysis = next((x for x in analysis_result if 'ETHUSDT' in x['symbol']), None)
            
            if btc_analysis:
                logger.info(f"BTC分析结果 - 波动率: {btc_analysis.get('volatility', 'N/A')}, 趋势强度: {btc_analysis.get('trend_strength', 'N/A')}")
            if eth_analysis:
                logger.info(f"ETH分析���果 - 波动率: {eth_analysis.get('volatility', 'N/A')}, 趋势强度: {eth_analysis.get('trend_strength', 'N/A')}")
            
            logger.info("第3步完成: 市场分析功能正常")
            return True

        except Exception as e:
            logger.error(f"市场分析测试失败: {str(e)}")
            return False

    async def test_trading_operations(self):
        """测试交易操作"""
        try:
            logger.info("第4步: 测试交易操作...")
            
            # 测试开仓
            logger.info("4.1: 测试开仓操作...")
            test_symbol = "BTCUSDT"
            
            # 获取当前市场价格
            market_summary = self.market_data_manager.get_market_summary()
            btc_data = next((x for x in market_summary['data'] if x['symbol'] == test_symbol), None)
            if not btc_data:
                raise Exception(f"无法获取{test_symbol}的市场数据")
            
            current_price = btc_data['price']
            position_size = 0.001  # 测试用小仓位
            
            # 使用模拟账户下单
            logger.info(f"4.2: 尝试开仓 {test_symbol}, 价格: {current_price}, 数量: {position_size}")
            trade_result = self.simulation_account.place_order(
                symbol=test_symbol,
                side="buy",
                size=position_size,
                price=current_price
            )
            
            if trade_result:
                logger.info(f"✓ 成功开仓 {test_symbol}: {trade_result}")
                
                # 等待一段时间
                logger.info("4.3: 等待价格变动...")
                await asyncio.sleep(30)
                
                # 获取最新价格
                market_summary = self.market_data_manager.get_market_summary()
                btc_data = next((x for x in market_summary['data'] if x['symbol'] == test_symbol), None)
                if not btc_data:
                    raise Exception(f"无法获取{test_symbol}的最新价格")
                
                current_price = btc_data['price']
                
                # 平仓操作
                logger.info("4.4: 测试平仓操作...")
                close_result = self.simulation_account.place_order(
                    symbol=test_symbol,
                    side="sell",
                    size=position_size,
                    price=current_price
                )
                
                if close_result:
                    logger.info(f"✓ 成功平仓 {test_symbol}")
                else:
                    logger.error("平仓操作失败")
            else:
                logger.error("开仓操作失败")
            
            # 检查账户状态
            account_info = self.simulation_account.get_account_info()
            logger.info(f"4.5: 账户状态 - 余额: {account_info['total_balance']}, 可用: {account_info['available_balance']}")
            
            logger.info("第4步完成: 交易操作测试完成")
            return True

        except Exception as e:
            logger.error(f"交易操作测试失败: {str(e)}")
            return False

    async def cleanup(self):
        """清理资源"""
        try:
            logger.info("最后步骤: 清理系统资源...")
            
            if self.market_data_manager:
                await self.market_data_manager.cleanup()
                logger.info("✓ 市场数据管理器清理完成")
            
            if self.api_server:
                await self.api_server.cleanup()
                logger.info("✓ API服务器清理完成")
            
            logger.info("系统清理完成")

        except Exception as e:
            logger.error(f"系统清理失败: {str(e)}")

async def main():
    test = SystemTest()
    try:
        # 初始化系统
        if not await test.initialize_components():
            logger.error("系统初始化失败，终止测试")
            return

        # 测试市场数据收集
        if not await test.test_market_data_collection():
            logger.error("市场数据收集测试失败，终止测试")
            return

        # 测试市场分析
        if not await test.test_market_analysis():
            logger.error("市场分析测试失败，终止测试")
            return

        # 测试交易操作
        if not await test.test_trading_operations():
            logger.error("交易操作测试失败，终止测试")
            return

        logger.info("所有测试完成!")

    except Exception as e:
        logger.error(f"测试过程中发生错误: {str(e)}")
    finally:
        await test.cleanup()

if __name__ == "__main__":
    asyncio.run(main()) 