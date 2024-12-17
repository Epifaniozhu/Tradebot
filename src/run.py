import asyncio
from loguru import logger
from main import TradingSystem

async def main():
    try:
        # 创建交易系统实例
        system = TradingSystem()
        
        # 初始化系统
        success = await system.initialize()
        if not success:
            logger.error("系统初始化失败")
            return
            
        # 运行系统
        await system.run()
        
        # 保持程序运行
        while True:
            await asyncio.sleep(1)
            
    except Exception as e:
        logger.error(f"系统运行错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # 确保系统正确关闭
        if 'system' in locals():
            await system.stop()

if __name__ == "__main__":
    # 设置日志格式
    logger.add("logs/system_{time}.log", rotation="500 MB")
    
    # 运行系统
    asyncio.run(main()) 