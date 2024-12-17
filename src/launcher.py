import os
import sys
import webbrowser
import subprocess
from pathlib import Path
import uvicorn
import threading
import time
from loguru import logger

def open_browser():
    """等待服务器启动后打开浏览器"""
    time.sleep(2)  # 等待服务器启动
    webbrowser.open('http://localhost:8000')

def main():
    # 设置工作目录
    if getattr(sys, 'frozen', False):
        # 如果是打包后的exe
        application_path = os.path.dirname(sys.executable)
    else:
        # 如果是直接运行的python脚本
        application_path = os.path.dirname(os.path.abspath(__file__))
    
    os.chdir(os.path.dirname(application_path))
    
    # 配置日志
    logger.add("logs/trading_{time}.log", rotation="500 MB")
    
    # 创建必要的目录
    Path("logs").mkdir(exist_ok=True)
    Path("data").mkdir(exist_ok=True)
    
    # 在新线程中打开浏览器
    threading.Thread(target=open_browser, daemon=True).start()
    
    # 启动FastAPI服务器
    logger.info("正在启动交易系统...")
    uvicorn.run(
        "api_server:app",
        host="0.0.0.0",
        port=8000,
        reload=False
    )

if __name__ == "__main__":
    main() 