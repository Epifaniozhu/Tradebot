import asyncio
from market_data_manager import MarketDataManager
from loguru import logger

async def main():
    try:
        # 创建市场数据管理器实例
        market_data = MarketDataManager()
        
        # 检查数据库状态
        logger.info("开始检查数据状态...")
        market_data.check_database_status()
        
        # 检查实时缓存状态
        logger.info("\n实时缓存状态:")
        logger.info("=" * 50)
        
        # 检查tick数据
        for symbol, data in market_data.real_time_cache['tick'].items():
            logger.info(f"Tick数据 - {symbol}:")
            logger.info(f"  数据点数量: {len(data)}")
            if data:
                logger.info(f"  最新数据时间: {data[-1]['timestamp']}")
            logger.info("-" * 30)
        
        # 检查1分钟K线数据
        for symbol, data in market_data.real_time_cache['1m'].items():
            logger.info(f"1分钟K线 - {symbol}:")
            logger.info(f"  数据点数量: {len(data)}")
            if data:
                logger.info(f"  最新数据时间: {data[-1]['timestamp']}")
            logger.info("-" * 30)
        
        # 检查5分钟K线数据
        for symbol, data in market_data.real_time_cache['5m'].items():
            logger.info(f"5分钟K线 - {symbol}:")
            logger.info(f"  数据点数量: {len(data)}")
            if data:
                logger.info(f"  最新数据时间: {data[-1]['timestamp']}")
            logger.info("-" * 30)
        
        # 检查最新价格缓存
        logger.info("\n最新价格缓存状态:")
        logger.info("=" * 50)
        for symbol, data in market_data.latest_prices.items():
            logger.info(f"{symbol}:")
            logger.info(f"  价格: {data.get('price')}")
            logger.info(f"  成交量: {data.get('volume')}")
            logger.info(f"  时间戳: {data.get('timestamp')}")
            logger.info("-" * 30)
        
        # 检查相关性数据
        logger.info("\n相关性数据状态:")
        logger.info("=" * 50)
        for symbol, data in market_data.correlation_data.items():
            logger.info(f"{symbol}:")
            logger.info(f"  时间戳数量: {len(data.get('timestamps', []))}")
            logger.info(f"  价格数据点数: {len(data.get('closes', []))}")
            logger.info(f"  成交量数据点数: {len(data.get('volumes', []))}")
            logger.info("-" * 30)
        
    except Exception as e:
        logger.error(f"检查数据状态失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(main()) 