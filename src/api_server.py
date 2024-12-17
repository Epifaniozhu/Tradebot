from fastapi import FastAPI, WebSocket, HTTPException, Request, APIRouter
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Literal
from pydantic import BaseModel
import json
import asyncio
import uvicorn
from datetime import datetime
from loguru import logger
from pathlib import Path
import threading
import os
from dotenv import load_dotenv
import time

# 导入交易系统组件
from correlation_manager import CorrelationManager
from market_data_manager import MarketDataManager
from position_manager import PositionManager
from risk_manager import RiskManager
from simulation_account import SimulationAccount
from exchange_api import ExchangeAPI

# 加载环境变量
load_dotenv()

class APIServer:
    def __init__(self):
        self.app = FastAPI()
        self.router = APIRouter()
        
        # 创建市场数据管理器，传入self作为websocket_server
        self.market_data = MarketDataManager(websocket_server=self)
        
        # 创建其他管理器
        self.correlation_manager = CorrelationManager(self.market_data)
        self.simulation_account = SimulationAccount()
        
        self.active_connections = []
        self.is_running = False  # 服务器运行状态
        self.is_trading = False  # 交易状态
        self.server = None
        
        # 设置路由
        self._setup_routes()
        
        # 添加CORS中间件
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # 添加路由
        self.app.include_router(self.router)
        
        # 修改静态文件服务配置
        frontend_dir = Path("frontend").resolve()
        if not frontend_dir.exists():
            logger.error(f"Frontend directory not found at {frontend_dir}")
            raise FileNotFoundError(f"Frontend directory not found at {frontend_dir}")
            
        self.app.mount("/", StaticFiles(directory=str(frontend_dir), html=True), name="frontend")
        logger.info(f"Mounted frontend static files from {frontend_dir}")
        
    def _setup_routes(self):
        @self.app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()
            self.active_connections.append(websocket)
            try:
                # 发送连接成功的日志
                await self.broadcast_log("success", "WebSocket连接已建立")
                
                while True:
                    await websocket.receive_text()
            except Exception as e:
                logger.error(f"WebSocket连接错误: {str(e)}")
            finally:
                if websocket in self.active_connections:
                    self.active_connections.remove(websocket)
                    
        @self.router.post("/api/start_trading")
        async def start_trading():
            """启动自动交易"""
            try:
                self.is_trading = True
                
                # 广播状态更新
                await self.broadcast_message("status_update", {
                    "status": "交易中",
                    "timestamp": int(time.time() * 1000)
                })
                
                # 发送启动成功的日志
                await self.broadcast_log("success", "自动交易已启动")
                
                return {"status": "success", "message": "交易已启动"}
            except Exception as e:
                logger.error(f"启动交易���败: {str(e)}")
                await self.broadcast_log("error", f"启动交易失败: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.post("/api/stop_trading")
        async def stop_trading():
            """停止自动交易"""
            try:
                self.is_trading = False
                
                # 广播状态更新
                await self.broadcast_message("status_update", {
                    "status": "已停止",
                    "timestamp": int(time.time() * 1000)
                })
                
                # 发送停止成功的日志
                await self.broadcast_log("success", "自动交易已停止")
                
                return {"status": "success", "message": "交易已停止"}
            except Exception as e:
                logger.error(f"停止交易失败: {str(e)}")
                await self.broadcast_log("error", f"停止交易失败: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.post("/api/switch_account")
        async def switch_account(request: Request):
            """切换交易账户"""
            try:
                data = await request.json()
                account_id = data.get("account_id")
                if not account_id:
                    raise HTTPException(status_code=400, detail="缺少account_id参数")
                
                # 这里可以添加账户切换的具体逻辑
                logger.info(f"切换到账户: {account_id}")
                await self.broadcast_log("success", f"已切换到{account_id}账户")
                
                return {"status": "success", "message": f"已切换到{account_id}账户"}
            except Exception as e:
                logger.error(f"切换账户失败: {str(e)}")
                await self.broadcast_log("error", f"切换账户失败: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.get("/api/balance")
        async def get_balance():
            """获取当前账户余额"""
            try:
                account_info = self.simulation_account.get_account_info()
                return {
                    "balance": account_info.get("total_balance", 0),
                    "available": account_info.get("available_balance", 0),
                    "margin": account_info.get("margin_used", 0),
                    "position_value": account_info.get("total_position_value", 0)
                }
            except Exception as e:
                logger.error(f"获取余额失败: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))
                
        @self.router.get("/api/positions")
        async def get_positions():
            """获取当前持仓信息"""
            try:
                positions = self.simulation_account.get_positions()
                total_pnl = sum(p.get("pnl", 0) for p in positions)
                pnl_amount = sum(p.get("pnl_amount", 0) for p in positions)
                return {
                    "positions": positions,
                    "totalPnl": total_pnl,
                    "pnlAmount": pnl_amount
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
                
        @self.router.get("/api/history")
        async def get_history():
            """获取历史交易记录"""
            try:
                history = self.simulation_account.get_trade_history()
                return history
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
                
    async def broadcast_message(self, message_type: str, data: Any) -> None:
        """
        广播消息给所有连接的WebSocket客户端
        :param message_type: 消息类型
        :param data: 消息数据
        """
        if not self.active_connections:
            logger.warning("没有活动的WebSocket连接")
            return
            
        # 处理布尔值和其他特殊类型
        def json_serializer(obj):
            if isinstance(obj, bool):
                return str(obj).lower()
            return str(obj)
            
        message = {
            "type": message_type,
            "data": data,
            "timestamp": int(time.time() * 1000)
        }
        
        logger.debug(f"准备广播消息: type={message_type}, connections={len(self.active_connections)}")
        
        for connection in self.active_connections:
            try:
                # 使用自定义序列化器
                json_str = json.dumps(message, default=json_serializer)
                logger.debug(f"发送消息: {json_str[:200]}...")  # 只打印前200个字符
                await connection.send_text(json_str)
                logger.debug(f"消息发送成功")
            except Exception as e:
                logger.error(f"发送WebSocket消息失败: {str(e)}")
                if connection in self.active_connections:
                    self.active_connections.remove(connection)
                    logger.warning("已移除失效���WebSocket连接")

    async def send_message(self, message: str) -> None:
        """
        发送消息给所有连接的WebSocket客户端
        :param message: 要发送的消息（已经是JSON字符串）
        """
        if not self.active_connections:
            logger.warning("没有活动的WebSocket连接")
            return
            
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
                logger.debug("消息发送成功")
            except Exception as e:
                logger.error(f"发送WebSocket消息失败: {str(e)}")
                if connection in self.active_connections:
                    self.active_connections.remove(connection)
                    logger.warning("已移除失效的WebSocket连接")

    async def broadcast_market_data(self) -> None:
        """广播市场数据"""
        try:
            market_summary = self.market_data.get_market_summary()
            if market_summary and market_summary.get('data'):
                await self.broadcast_message('market_data', {
                    'data': market_summary['data'],
                    'timestamp': market_summary['timestamp']
                })
                
        except Exception as e:
            logger.error(f"广播市场数据失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    async def start(self):
        """启动API服务器"""
        try:
            # 只初始化相关性管理器，它会负责初始化市场数据管理器
            await self.correlation_manager.initialize_data()
            await self.broadcast_log("info", "系统初始化完成")
            
            # 设置服务器状态
            self.is_running = True
            
            # 启动市场分析循环
            asyncio.create_task(self._periodic_market_analysis())
            await self.broadcast_log("success", "市场分析任务已启动")
            
            # 启动WebSocket服务器
            config = uvicorn.Config(
                app=self.app,
                host="0.0.0.0",
                port=8000,
                log_level="info"
            )
            self.server = uvicorn.Server(config)
            await self.server.serve()
            
        except Exception as e:
            logger.error(f"启动服务器失败: {str(e)}")
            await self.broadcast_log("error", f"启动服务器失败: {str(e)}")
            raise

    async def stop(self):
        """停止API服务器"""
        try:
            self.is_running = False
            self.is_trading = False
            
            # 关闭所有WebSocket连接
            for connection in self.active_connections:
                try:
                    await connection.close()
                except Exception as e:
                    logger.error(f"关闭WebSocket连接失败: {str(e)}")
            
            # 清空连接列表
            self.active_connections.clear()
            
            # 停止服务器
            if self.server:
                await self.server.shutdown()
            
            await self.broadcast_log("info", "服务器已停止")
            
        except Exception as e:
            logger.error(f"停止服务器失败: {str(e)}")
            await self.broadcast_log("error", f"停止服务器失败: {str(e)}")
            raise

    async def _periodic_market_analysis(self):
        """持续发送市场分析数据"""
        while self.is_running:
            try:
                # 获取市场分析数据
                analysis_data = self.correlation_manager.get_market_analysis()
                if analysis_data:
                    # 直接广播市场分析数据，不需要额外嵌套
                    await self.broadcast_message("market_data", analysis_data)
                    
                    # 如果正在交易，获取交易信号
                    if self.is_trading:
                        signals = await self.correlation_manager.get_trading_signals()
                        if signals:
                            await self.broadcast_message("trading_signals", signals)
                        else:
                            # 如果没有信号，获取原因
                            reasons = await self.correlation_manager.get_no_signal_reasons()
                            await self.broadcast_message("log", {
                                "type": "info",
                                "message": f"未生成交易信号: {reasons}"
                            })
                    
                    # 更新数据
                    await self.correlation_manager.update_data()
                
                await asyncio.sleep(1)  # 每秒更新一次
                
            except Exception as e:
                logger.error(f"市场分析循环出错: {str(e)}")
                await asyncio.sleep(1)  # 出错时等待1秒后继续

    async def broadcast_log(self, type: str, message: str) -> None:
        """
        广播日志消息
        :param type: 日志类型 ('success', 'error', 'info', 'warning')
        :param message: 日志消息
        """
        await self.broadcast_message("log", {
            "type": type,
            "message": message
        })

    async def initialize(self):
        """初始化API服务器"""
        try:
            # 初始化相关性管理器
            await self.correlation_manager.initialize_data()
            
            # 启动WebSocket服务器
            self.websocket_manager = WebSocketManager()
            
            return True
        except Exception as e:
            logger.error(f"初始化API服务器失败: {str(e)}")
            return False

    async def cleanup(self):
        """清理API服务器资源"""
        try:
            logger.info("开始清理API服务器资源...")
            
            # 关闭WebSocket连接
            if hasattr(self, 'websocket_manager'):
                await self.websocket_manager.close_all()
            
            # 关闭HTTP服务器
            if hasattr(self, 'app') and hasattr(self.app, 'shutdown'):
                await self.app.shutdown()
            
            # 清理其他资源
            if hasattr(self, 'market_data'):
                await self.market_data.cleanup()
            
            if hasattr(self, 'correlation_manager'):
                if hasattr(self.correlation_manager, 'cleanup'):
                    await self.correlation_manager.cleanup()
            
            logger.info("API服务器资源清理完成")
            
        except Exception as e:
            logger.error(f"清理API服务器资源失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())