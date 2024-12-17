import asyncio
import websockets
import json
import aiohttp
from loguru import logger
import platform
import sys
import os

async def test_websocket():
    """测试WebSocket连接和实时更新"""
    try:
        async with websockets.connect('ws://localhost:8000/ws') as websocket:
            logger.info("WebSocket连接成功")
            
            # 保持连接并接收消息
            try:
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    logger.info(f"收到WebSocket消息: {data}")
            except websockets.exceptions.ConnectionClosed:
                logger.warning("WebSocket连接关闭")
    except Exception as e:
        logger.error(f"WebSocket连接失败: {str(e)}")

async def test_api_endpoints():
    """测试REST API端点"""
    base_url = 'http://localhost:8000/api'
    
    async with aiohttp.ClientSession() as session:
        try:
            # 1. 测试模拟账户
            logger.info("测试切换到模拟账户...")
            async with session.post(f'{base_url}/switch_account', 
                                json={"accountId": "simulation"}) as response:
                if response.status == 200:
                    result = await response.json()
                    logger.info(f"切换到模拟账户成功: {result}")
                else:
                    error = await response.json()
                    logger.error(f"切换到模拟账户失败: {error}")
            
            # 2. 测试获取持仓信息
            logger.info("测试获取持仓信息...")
            async with session.get(f'{base_url}/positions') as response:
                positions = await response.json()
                logger.info(f"当前持仓: {positions}")
            
            # 3. 测试获取历史记录
            logger.info("测试获取历史记录...")
            async with session.get(f'{base_url}/history') as response:
                history = await response.json()
                logger.info(f"交易历史: {history}")
            
            # 4. 测试实盘账户（预期会失败）
            logger.info("测试切换到实盘账户...")
            async with session.post(f'{base_url}/switch_account', 
                                json={"accountId": "live"}) as response:
                result = await response.json()
                if response.status != 200:
                    logger.info("预期的实盘账户切换失败")
                logger.info(f"切换到实盘账户结果: {result}")
            
            # 5. 测试启动交易
            logger.info("测试启动交易...")
            async with session.post(f'{base_url}/start_trading') as response:
                if response.status == 200:
                    result = await response.json()
                    logger.info(f"启动交易成功: {result}")
                else:
                    error = await response.json()
                    logger.error(f"启动交易失败: {error}")
            
            # 等待一段时间观察交易系统运行
            await asyncio.sleep(5)
            
            # 6. 测试停止交易
            logger.info("测试停止交易...")
            async with session.post(f'{base_url}/stop_trading') as response:
                if response.status == 200:
                    result = await response.json()
                    logger.info(f"停止交易成功: {result}")
                else:
                    error = await response.json()
                    logger.error(f"停止交易失败: {error}")
                    
        except aiohttp.ClientError as e:
            logger.error(f"API请求错误: {str(e)}")
        except Exception as e:
            logger.error(f"测试过程中出现错误: {str(e)}")

async def main():
    """运行所有测试"""
    logger.info("开始API测试...")
    
    try:
        # 确保日志目录存在
        os.makedirs("logs", exist_ok=True)
        
        # 创建两个任务：一个监听WebSocket，一个测试API
        await asyncio.gather(
            test_websocket(),
            test_api_endpoints()
        )
    except Exception as e:
        logger.error(f"测试过程中出现错误: {str(e)}")
    finally:
        logger.info("测试完成")

if __name__ == "__main__":
    # 配置日志
    logger.add("logs/api_test_{time}.log", rotation="500 MB")
    
    # 在Windows上使用SelectSelector
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # 运行测试
    asyncio.run(main()) 