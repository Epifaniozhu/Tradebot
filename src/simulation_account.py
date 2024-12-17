from typing import List, Dict, Any
from datetime import datetime
import json
from pathlib import Path
from loguru import logger
import os

class SimulationAccount:
    def __init__(self):
        self.positions: List[Dict[str, Any]] = []
        self.trade_history: List[Dict[str, Any]] = []
        self.balance = 10000.0  # 初始资金10000 USDT
        self.frozen_balance = 0.0  # 冻结资金（用于委托订单）
        self.max_position_size = 0.2  # 单个持仓最大占比20%
        self.max_total_position = 0.8  # 最大总持仓80%
        self.auto_close_loss = 0.05  # 5%自动平仓
        self.leverage = 20  # 初始杠杆倍数20倍
        
        # 手续费率（USDT永续合约）
        self.maker_fee_rate = 0.0002  # Maker费率 0.02%
        self.taker_fee_rate = 0.0004  # Taker费率 0.04%
        self.total_fee_paid = 0.0  # 总支付手续费
        
        # 期货合约特定参数
        self.funding_rate = 0.0001  # 资金费率（每8小时）
        self.liquidation_threshold = 0.8  # 清算阈值（维持保证金率）
        self.min_margin_ratio = 0.05  # 最小维持保证金率
        
        # 创建数据目录
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
        # 加载历史数据
        self._load_data()
        
    def _calculate_fee(self, trade_value: float, is_taker: bool = True) -> float:
        """
        计算手续费
        :param trade_value: 交易金额
        :param is_taker: 是否是Taker单
        :return: 手续费金额
        """
        fee_rate = self.taker_fee_rate if is_taker else self.maker_fee_rate
        return trade_value * fee_rate
        
    def _load_data(self):
        """加载历史数据"""
        try:
            # 加载持仓数据
            positions_file = self.data_dir / "positions.json"
            if positions_file.exists():
                with open(positions_file, "r", encoding="utf-8") as f:
                    self.positions = json.load(f)
                    
            # 加载交易历史
            history_file = self.data_dir / "trade_history.json"
            if history_file.exists():
                with open(history_file, "r", encoding="utf-8") as f:
                    self.trade_history = json.load(f)
                    
            # 加载账户余额和手续费数据
            balance_file = self.data_dir / "balance.json"
            if balance_file.exists():
                with open(balance_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.balance = data.get("balance", self.balance)
                    self.frozen_balance = data.get("frozen_balance", self.frozen_balance)
                    self.total_fee_paid = data.get("total_fee_paid", self.total_fee_paid)
                    
            logger.info(f"加载历史数据成功，当前余额: {self.balance} USDT")
        except Exception as e:
            logger.error(f"加载历史数据失败: {str(e)}")
            
    def _save_data(self):
        """保存数据"""
        try:
            # 保存持仓数据
            with open(self.data_dir / "positions.json", "w", encoding="utf-8") as f:
                json.dump(self.positions, f, ensure_ascii=False, indent=2)
                
            # 保存交易历史
            with open(self.data_dir / "trade_history.json", "w", encoding="utf-8") as f:
                json.dump(self.trade_history, f, ensure_ascii=False, indent=2)
                
            # 保存账户余额和手续费数据
            with open(self.data_dir / "balance.json", "w", encoding="utf-8") as f:
                json.dump({
                    "balance": self.balance,
                    "frozen_balance": self.frozen_balance,
                    "total_fee_paid": self.total_fee_paid
                }, f, ensure_ascii=False, indent=2)
                
            logger.info("保存数据成功")
        except Exception as e:
            logger.error(f"保存数据失败: {str(e)}")
        
    def initialize_account(self, account_id: str):
        """初始化账户"""
        logger.info(f"初始化模拟账户，初始资金: {self.balance} USDT")
        self._load_data()
        
    def get_positions(self) -> List[Dict[str, Any]]:
        """获取当前持仓"""
        return self.positions
        
    def get_trade_history(self) -> List[Dict[str, Any]]:
        """获取交易历史"""
        return self.trade_history
        
    def get_account_info(self) -> Dict[str, float]:
        """获取账户信息"""
        total_position_value = sum(p.get("position_value", 0) for p in self.positions)
        margin_used = total_position_value / self.leverage  # 已用保证金
        available_balance = self.balance - margin_used - self.frozen_balance  # 可用余额
        
        return {
            "total_balance": self.balance,  # 总余额
            "available_balance": available_balance,  # 可用余额
            "frozen_balance": self.frozen_balance,  # 冻结余额
            "margin_used": margin_used,  # 已用保证金
            "total_position_value": total_position_value,  # 总持仓价值
            "position_ratio": total_position_value / (self.balance * self.leverage) if self.balance > 0 else 0,  # 仓位比例
            "leverage": self.leverage,  # 杠杆倍数
            "unrealized_pnl": sum(p.get("pnl_amount", 0) for p in self.positions),  # 未实现盈亏
            "total_fee_paid": self.total_fee_paid  # 总手续费支出
        }
        
    def check_margin_requirement(self, symbol: str, size: float, price: float) -> bool:
        """
        检查保证金要求
        :return: True if sufficient margin, False otherwise
        """
        position_value = size * price
        required_margin = position_value / self.leverage
        
        # 计算预估手续费
        estimated_fee = self._calculate_fee(position_value, is_taker=True)
        total_required = required_margin + estimated_fee
        
        # 获取当前账户信息
        account_info = self.get_account_info()
        available_balance = account_info["available_balance"]
        
        # 检查可用余额是否足够（包括手续费）
        if total_required > available_balance:
            logger.warning(f"保证金不足: 需要 {required_margin} USDT (手续费 {estimated_fee} USDT), 可用 {available_balance} USDT")
            return False
            
        # 检查是否超过单个持仓限制
        max_position_value = self.balance * self.leverage * self.max_position_size
        if position_value > max_position_value:
            logger.warning(f"超过单个持仓限制: {position_value} > {max_position_value}")
            return False
            
        # 检查是否超过总持仓限制
        total_position_value = account_info["total_position_value"] + position_value
        max_total_value = self.balance * self.leverage * self.max_total_position
        if total_position_value > max_total_value:
            logger.warning(f"超过总持仓限制: {total_position_value} > {max_total_value}")
            return False
            
        return True
        
    def place_order(self, symbol: str, side: str, size: float, price: float = None, is_taker: bool = True):
        try:
            timestamp = datetime.now()
            
            if price is None:
                price = self.get_market_price(symbol)
                if not price:
                    raise ValueError(f"无法获取 {symbol} 的市场价格")
            
            # 如果是开仓，检查是否已有主流币仓位
            if side == "buy" and symbol in ['BTCUSDT', 'ETHUSDT']:
                has_major = any(p["symbol"] in ['BTCUSDT', 'ETHUSDT'] for p in self.positions)
                if has_major:
                    raise ValueError("已有主流币仓位，不能开新的主流币仓位")
            
            # 计算交易价值
            trade_value = size * price
            
            # 计算所需保证金
            margin_required = trade_value / self.leverage
            
            # 检查可用余额
            if margin_required > self.get_available_balance():
                raise ValueError("可用余额不足")
            
            # 计算手续费
            fee = self._calculate_fee(trade_value, is_taker)
            
            # 记录交易
            trade = {
                "symbol": symbol,
                "side": side,
                "size": size,
                "price": price,
                "value": trade_value,
                "margin": margin_required,
                "fee": fee,
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "is_taker": is_taker
            }
            
            self.trade_history.append(trade)
            
            # 扣除手续费
            self.balance -= fee
            self.total_fee_paid += fee
            
            # 更新持仓
            position = next((p for p in self.positions if p["symbol"] == symbol), None)
            if position:
                if side == "buy":
                    # 计算新的平均入场价（考虑手续费）
                    total_value = position["size"] * position["entry_price"] + size * price
                    total_size = position["size"] + size
                    position["entry_price"] = total_value / total_size
                    position["size"] = total_size
                else:
                    position["size"] -= size
                    
                # 更新持仓价值和维持保证金
                position["position_value"] = position["size"] * price
                position["margin"] = position["position_value"] / position["leverage"]
                position["maintenance_margin"] = position["position_value"] * self.min_margin_ratio
                    
                # 如果持仓为0，移除该持仓
                if position["size"] == 0:
                    self.positions.remove(position)
            else:
                if side == "buy":
                    # 创建新持仓（入场价包含手续费）
                    self.positions.append({
                        "symbol": symbol,
                        "size": size,
                        "entry_price": price,
                        "position_value": trade_value,
                        "margin": margin_required,
                        "maintenance_margin": trade_value * self.min_margin_ratio,
                        "leverage": self.leverage,
                        "pnl": 0,
                        "pnl_amount": 0,
                        "total_fee": fee,
                        "funding_fee": 0,
                        "last_funding_time": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                        "is_major": symbol in ['BTCUSDT', 'ETHUSDT'],  # 标记是否是主流币仓位
                        "profit_protected": False,  # 标记是否已经保护了利润
                        "major_type": symbol.replace("USDT", "")  # 标记主流币类型（BTC或ETH）
                    })
            
            # 保存数据
            self._save_data()
            
            return trade
            
        except Exception as e:
            logger.error(f"下单失败: {str(e)}")
            return None
        
    def update_positions(self, current_prices: Dict[str, float]):
        """
        更新持仓盈亏
        :param current_prices: 当前价格 {symbol: price}
        """
        total_pnl = 0
        btc_position_closed = False
        eth_position_closed = False
        
        for position in self.positions[:]:  # 使用切片创建副本以避免在迭代时修改
            symbol = position["symbol"]
            if symbol in current_prices:
                current_price = current_prices[symbol]
                entry_price = position["entry_price"]
                size = position["size"]
                
                # 更新持仓价值
                position["position_value"] = size * current_price
                position["margin"] = position["position_value"] / position["leverage"]
                
                # 计算盈亏
                if entry_price > 0:
                    pnl_percent = (current_price - entry_price) / entry_price * 100
                    pnl_amount = (current_price - entry_price) * size
                    
                    position["pnl"] = pnl_percent
                    position["pnl_amount"] = pnl_amount
                    total_pnl += pnl_amount
                    
                    # 检查是否需要自动平仓（亏损超过5%）
                    if pnl_percent <= -5:
                        logger.warning(f"触发5%自动平仓: {symbol} @ {current_price}")
                        self.place_order(symbol, "sell", size, current_price)
                        if position["is_btc"]:
                            btc_position_closed = True
                        elif position["is_eth"]:
                            eth_position_closed = True
                        continue
                    
                    # 如果是主流币且盈利超过2%但还未保护利润
                    if (position["is_btc"] or position["is_eth"]) and pnl_percent >= 2 and not position["profit_protected"]:
                        # 根据币种调整杠杆
                        old_leverage = position["leverage"]
                        if position["is_btc"]:
                            new_leverage = 125  # BTC调到125倍
                        else:  # ETH
                            new_leverage = 100  # ETH调到100倍
                            
                        position["leverage"] = new_leverage
                        # 计算可以释放的保证金
                        released_margin = position["margin"] * (1 - old_leverage/new_leverage)
                        self.balance += released_margin
                        position["margin"] = position["position_value"] / new_leverage
                        position["profit_protected"] = True
                        logger.info(f"{symbol}仓位盈利超过2%，调整杠杆到{new_leverage}倍，释放保证金: {released_margin:.2f} USDT")
                    
                    # 如果主流币仓位被平仓，同时平掉其关联的仓位
                    if (btc_position_closed and position.get("major_type") == "BTC") or \
                       (eth_position_closed and position.get("major_type") == "ETH"):
                        logger.info(f"{position['major_type']}仓位已平仓，同时平掉关联仓位: {symbol}")
                        self.place_order(symbol, "sell", size, current_price)
                        
        # 更新账户余额（包括未实现盈亏）
        self.balance += total_pnl
                        
        # 保存数据
        self._save_data()
        
    def get_balance(self) -> Dict[str, float]:
        """获取账户余额"""
        return {"USDT": self.balance}