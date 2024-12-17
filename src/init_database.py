import asyncio
import os
import aiohttp
import sqlite3
from loguru import logger
import sys

# 配置日志
logger.remove()
logger.add(
    sys.stdout,
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}",
    level="INFO"
)

class DatabaseInitializer:
    def __init__(self):
        # 更新为正确的币安合约API地址
        self.FUTURES_BASE_URL = "https://fapi.binance.com/fapi/v1"
        self.proxy = "http://127.0.0.1:4780"
        self.db_path = "data/market_data.db"
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # 配置代理设置
        self.proxy_settings = {
            'proxy': self.proxy,
            'ssl': False  # 禁用SSL验证
        }
        
    def _get_db_connection(self):
        return sqlite3.connect(self.db_path)
        
    def init_database(self):
        """初始化数据库表"""
        with self._get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 创建K线数据表
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS kline_data (
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                quote_volume REAL NOT NULL,  -- USDT计价的交易量
                PRIMARY KEY (symbol, timeframe, timestamp)
            )
            ''')
            
            # 创建索引
            cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_kline_symbol_time 
            ON kline_data (symbol, timeframe, timestamp)
            ''')
            
            conn.commit()
            logger.info("数据库表初始化完成")
            
    async def get_top_volume_symbols(self, session, limit=10):
        """获取交易量最大的期货合约"""
        endpoint = f"{self.FUTURES_BASE_URL}/ticker/24hr"
        try:
            logger.info(f"正在获取交易对数据... URL: {endpoint}")
            async with session.get(endpoint, **self.proxy_settings) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"成功获取到 {len(data)} 个交易对的数据")
                    
                    # 只选择USDT永续合约并按交易量排序
                    futures_data = [item for item in data if item['symbol'].endswith('USDT')]
                    logger.info(f"其中包含 {len(futures_data)} 个USDT永续合约")
                    
                    # 按24小时交易量排序
                    sorted_data = sorted(futures_data, 
                                      key=lambda x: float(x['quoteVolume']),  # 使用USDT计价的交易量
                                      reverse=True)
                    
                    result = [item['symbol'] for item in sorted_data[:limit]]
                    volumes = [float(item['quoteVolume'])/1000000 for item in sorted_data[:limit]]  # 转换为百万USDT
                    
                    # 打印详细信息
                    logger.info(f"选择交易量最大的 {len(result)} 个合约:")
                    for symbol, volume in zip(result, volumes):
                        logger.info(f"  {symbol}: {volume:.2f}M USDT")
                    
                    return result
                else:
                    logger.error(f"获取交易对数据失败，状态码: {response.status}")
                    response_text = await response.text()
                    logger.error(f"响应内容: {response_text}")
                    return []
        except Exception as e:
            logger.error(f"获取高交易量合约失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []
            
    async def get_klines_data(self, session, symbol: str, interval='1h', limit=500):
        """获取合约K线数据"""
        endpoint = f"{self.FUTURES_BASE_URL}/klines"  # 更新endpoint
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        try:
            logger.info(f"正在获取 {symbol} 的K线数据...")
            async with session.get(endpoint, params=params, **self.proxy_settings) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"成功获取到 {len(data)} 条K线数据")
                    return data
                else:
                    logger.error(f"获取K线数据失败，状态码: {response.status}")
                    response_text = await response.text()
                    logger.error(f"响应内容: {response_text}")
                    return None
        except Exception as e:
            logger.error(f"获取K线数据失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
            
    async def save_klines_to_db(self, symbol: str, timeframe: str, klines: list):
        """保存K线数据到数据库"""
        try:
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                
                for kline in klines:
                    # 计算USDT计价的交易量
                    volume = float(kline[5])  # 原始交易量
                    close_price = float(kline[4])  # 收盘价
                    quote_volume = volume * close_price  # USDT计价的交易量
                    
                    cursor.execute('''
                    INSERT OR REPLACE INTO kline_data 
                    (symbol, timeframe, timestamp, open, high, low, close, volume, quote_volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        symbol,
                        timeframe,
                        kline[0],  # timestamp
                        float(kline[1]),  # open
                        float(kline[2]),  # high
                        float(kline[3]),  # low
                        float(kline[4]),  # close
                        volume,
                        quote_volume
                    ))
                
                conn.commit()
                logger.info(f"保存 {symbol} 的 {len(klines)} 条K线数据")
                
        except Exception as e:
            logger.error(f"保存K线数据到数据库失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            
    async def initialize_data(self):
        """初始化数据"""
        try:
            # 初始化数据库表
            self.init_database()
            
            # 配置aiohttp会话
            timeout = aiohttp.ClientTimeout(total=30)  # 30秒超时
            conn = aiohttp.TCPConnector(ssl=False)  # 禁用SSL验证
            
            async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
                # 获取top交易对
                symbols = await self.get_top_volume_symbols(session)
                if not symbols:
                    logger.error("未获取到交易对，请检查网络连接和代理设置")
                    return
                    
                logger.info(f"获取到 {len(symbols)} 个高交易量USDT永续合约")
                
                # 获取每个交易对的历史数据
                for symbol in symbols:
                    logger.info(f"获取 {symbol} 的历史数据...")
                    klines = await self.get_klines_data(session, symbol)
                    if klines:
                        await self.save_klines_to_db(symbol, '1h', klines)
                    else:
                        logger.error(f"获取 {symbol} 的K线数据失败")
                    await asyncio.sleep(1)  # 避免请求过快
                    
                logger.info("数据初始化完成")
                
        except Exception as e:
            logger.error(f"初始化数据失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

async def main():
    """主函数"""
    initializer = DatabaseInitializer()
    await initializer.initialize_data()

if __name__ == "__main__":
    if os.name == 'nt':  # Windows系统
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main()) 