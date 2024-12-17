from typing import Dict, Any
from pathlib import Path
import json
import os

CONSERVATIVE_THRESHOLDS = {
    'tick_change': 0.005,          # 0.5%
    'volatility_threshold': 0.008,  # 0.8%
    'volume_spike': 2.5,           # 2.5倍
    'trend_confirmation': 0.8,     # 80%
    'min_correlation': 0.75,       # 75%
    'max_concentration': 0.2,      # 20%
    'min_liquidity': 3000000,     # 300万USDT
    'volume_impact': 0.05          # 5%
}

MODERATE_THRESHOLDS = {
    'tick_change': 0.004,          # 0.4%
    'volatility_threshold': 0.006,  # 0.6%
    'volume_spike': 2.0,           # 2.0倍
    'trend_confirmation': 0.75,    # 75%
    'min_correlation': 0.7,        # 70%
    'max_concentration': 0.25,     # 25%
    'min_liquidity': 2000000,     # 200万USDT
    'volume_impact': 0.08          # 8%
}

AGGRESSIVE_THRESHOLDS = {
    'tick_change': 0.003,          # 0.3%
    'volatility_threshold': 0.005,  # 0.5%
    'volume_spike': 1.8,           # 1.8倍
    'trend_confirmation': 0.7,     # 70%
    'min_correlation': 0.65,       # 65%
    'max_concentration': 0.3,      # 30%
    'min_liquidity': 1000000,     # 100万USDT
    'volume_impact': 0.1           # 10%
}

class Config:
    def __init__(self):
        # 基础配置
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
        # 交易配置
        self.initial_balance = 10000.0  # 初始资金
        self.max_position_size = 0.2    # 单个持仓最大占比20%
        self.max_total_position = 0.8   # 最大总持仓80%
        self.auto_close_loss = 0.05     # 5%自动平仓
        self.initial_leverage = 20      # 初始杠杆倍数
        
        # 市场数据配置
        self.market_state = 'MODERATE'  # 默认使用适中配置
        self.update_interval = 60       # 数据更新间隔（秒）
        self.max_history_days = 15      # 最大历史数据保存天数
        self.max_mink_hours = 2         # 最大分钟K线保存小时数
        
        # WebSocket配置
        self.ws_batch_size = 200        # WebSocket订阅批次大小
        self.ws_reconnect_delay = 5     # 重连延迟（秒）
        
        # API配置
        self.api_host = "0.0.0.0"
        self.api_port = 8000
        
        # 加载用户配置（如果存在）
        self._load_user_config()
    
    def _load_user_config(self):
        """加载用户配置文件"""
        config_file = self.data_dir / "config.json"
        if config_file.exists():
            try:
                with open(config_file, "r", encoding="utf-8") as f:
                    user_config = json.load(f)
                    for key, value in user_config.items():
                        if hasattr(self, key):
                            setattr(self, key, value)
            except Exception as e:
                print(f"加载用户配置失败: {str(e)}")
    
    def save_config(self):
        """保存当前配置到文件"""
        config_file = self.data_dir / "config.json"
        try:
            config_data = {
                key: value for key, value in self.__dict__.items()
                if not key.startswith('_') and not isinstance(value, Path)
            }
            with open(config_file, "w", encoding="utf-8") as f:
                json.dump(config_data, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"保存配置失败: {str(e)}")
    
    def get_thresholds(self) -> Dict[str, Any]:
        """获取当前市场状态对应的阈值配置"""
        if self.market_state == 'CONSERVATIVE':
            return CONSERVATIVE_THRESHOLDS
        elif self.market_state == 'AGGRESSIVE':
            return AGGRESSIVE_THRESHOLDS
        return MODERATE_THRESHOLDS
    
    def update_market_state(self, new_state: str):
        """更新市场状态"""
        if new_state in ['CONSERVATIVE', 'MODERATE', 'AGGRESSIVE']:
            self.market_state = new_state
            self.save_config() 