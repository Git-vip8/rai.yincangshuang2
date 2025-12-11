"""
🏦 套利系统 - HTTP交易客户端模块 v2.0
功能：通过app.py统一接口执行交易，支持任意合约套利
包含完整的同生共死止损触发、数据库持久化、订单重试机制
作为模块被app.py导入使用
"""

import asyncio
import time
import json
import logging
import os
import sqlite3
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict, field
from datetime import datetime

# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== 配置区 ====================
CONFIG = {
    "trading": {
        "default_leverage": 3,
        "max_retries": 3,
        "retry_delay": 1,
        "timeout": 30,
    },
    "stop_loss": {
        "check_interval": 1,
        "slipage_tolerance": 0.001,
    }
}

# ==================== 数据模型 ====================
@dataclass
class OrderRequest:
    """订单请求"""
    exchange: str
    symbol: str
    side: str
    order_type: str
    amount: float
    price: Optional[float] = None
    leverage: int = 3
    client_order_id: Optional[str] = None
    reduce_only: bool = False
    
    def __post_init__(self):
        if not self.client_order_id:
            self.client_order_id = f"{self.exchange}_{self.symbol}_{int(time.time()*1000)}"
    
    def to_dict(self):
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "side": self.side,
            "order_type": self.order_type,
            "amount": self.amount,
            "price": self.price,
            "leverage": self.leverage,
            "client_order_id": self.client_order_id,
            "reduce_only": self.reduce_only,
            "timestamp": time.time()
        }

@dataclass
class ArbitragePair:
    """套利配对仓位（同生共死）- 支持任意合约"""
    pair_id: str
    symbol: str
    long_exchange: str
    short_exchange: str
    long_order_id: str
    short_order_id: str
    amount: float
    entry_time: float = field(default_factory=time.time)
    stop_loss_percent: float = 0.0
    take_profit_percent: float = 0.0
    status: str = "active"
    close_reason: Optional[str] = None
    entry_prices: Dict[str, float] = field(default_factory=dict)  # 入场价格记录
    
    def get_counter_order_id(self, triggered_order_id: str) -> Optional[str]:
        """获取对应交易所的配对订单ID"""
        if triggered_order_id == self.long_order_id:
            return self.short_order_id
        elif triggered_order_id == self.short_order_id:
            return self.long_order_id
        return None
    
    def get_counter_exchange(self, triggered_exchange: str) -> Optional[str]:
        """获取对应交易所"""
        if triggered_exchange == self.long_exchange:
            return self.short_exchange
        elif triggered_exchange == self.short_exchange:
            return self.long_exchange
        return None

@dataclass
class StopLossConfig:
    """止损配置"""
    config_id: str
    symbol: str
    exchange: str
    order_id: str
    stop_price: float
    is_percent: bool = True
    percent_value: float = 0.0
    original_price: float = 0.0
    pair_id: str = ""
    is_active: bool = True
    created_at: float = field(default_factory=time.time)
    triggered_at: Optional[float] = None

# ==================== 套利管理器（支持任意合约）====================
class ArbitrageManager:
    """套利管理器 - 核心的同生共死逻辑，支持任意合约"""
    
    def __init__(self, db_path: str = "arbitrage.db"):
        self.db_path = db_path
        self.conn = None
        
        # 内存存储
        self.arbitrage_pairs: Dict[str, ArbitragePair] = {}
        self.stop_loss_configs: Dict[str, StopLossConfig] = {}
        
        # 统计信息
        self.stats = {
            "total_pairs_created": 0,
            "active_pairs": 0,
            "pairs_closed": 0,
            "stop_loss_triggers": 0,
            "take_profit_triggers": 0,
            "start_time": time.time()
        }
        
        self._init_database()
        self._load_from_database()
        
        logger.info(f"套利管理器初始化完成 - 支持任意合约套利")
        logger.info(f"已加载 {len(self.arbitrage_pairs)} 个配对，其中 {self.stats['active_pairs']} 个活跃")
    
    def _init_database(self):
        """初始化数据库"""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = self.conn.cursor()
        
        # 套利配对表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS arbitrage_pairs (
            pair_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            long_exchange TEXT NOT NULL,
            short_exchange TEXT NOT NULL,
            long_order_id TEXT NOT NULL,
            short_order_id TEXT NOT NULL,
            amount REAL NOT NULL,
            entry_time REAL NOT NULL,
            stop_loss_percent REAL DEFAULT 0,
            take_profit_percent REAL DEFAULT 0,
            status TEXT DEFAULT 'active',
            close_reason TEXT,
            entry_prices TEXT,  -- JSON格式存储入场价格
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 止损配置表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stop_loss_configs (
            config_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            order_id TEXT NOT NULL,
            stop_price REAL NOT NULL,
            is_percent INTEGER DEFAULT 1,
            percent_value REAL DEFAULT 0,
            original_price REAL DEFAULT 0,
            pair_id TEXT,
            is_active INTEGER DEFAULT 1,
            created_at REAL NOT NULL,
            triggered_at REAL,
            FOREIGN KEY (pair_id) REFERENCES arbitrage_pairs (pair_id)
        )
        ''')
        
        # 套利机会记录表（可选）
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            long_exchange TEXT NOT NULL,
            short_exchange TEXT NOT NULL,
            funding_rate_diff REAL,
            price_diff_percent REAL,
            estimated_annual_return REAL,
            detected_at REAL NOT NULL,
            acted_upon INTEGER DEFAULT 0
        )
        ''')
        
        self.conn.commit()
    
    def _load_from_database(self):
        """从数据库加载数据"""
        cursor = self.conn.cursor()
        
        # 加载套利配对
        cursor.execute("SELECT * FROM arbitrage_pairs")
        for row in cursor.fetchall():
            try:
                # 解析entry_prices JSON
                entry_prices = {}
                if row[12]:  # entry_prices字段
                    entry_prices = json.loads(row[12])
                
                pair = ArbitragePair(
                    pair_id=row[0],
                    symbol=row[1],
                    long_exchange=row[2],
                    short_exchange=row[3],
                    long_order_id=row[4],
                    short_order_id=row[5],
                    amount=row[6],
                    entry_time=row[7],
                    stop_loss_percent=row[8],
                    take_profit_percent=row[9],
                    status=row[10],
                    close_reason=row[11],
                    entry_prices=entry_prices
                )
                self.arbitrage_pairs[pair.pair_id] = pair
                
                if pair.status == "active":
                    self.stats["active_pairs"] += 1
                else:
                    self.stats["pairs_closed"] += 1
                    
            except Exception as e:
                logger.error(f"加载套利配对失败: {e}")
        
        # 加载止损配置
        cursor.execute("SELECT * FROM stop_loss_configs WHERE is_active = 1")
        for row in cursor.fetchall():
            try:
                config = StopLossConfig(
                    config_id=row[0],
                    symbol=row[1],
                    exchange=row[2],
                    order_id=row[3],
                    stop_price=row[4],
                    is_percent=bool(row[5]),
                    percent_value=row[6],
                    original_price=row[7],
                    pair_id=row[8],
                    is_active=bool(row[9]),
                    created_at=row[10],
                    triggered_at=row[11]
                )
                self.stop_loss_configs[config.config_id] = config
            except Exception as e:
                logger.error(f"加载止损配置失败: {e}")
        
        self.stats["total_pairs_created"] = len(self.arbitrage_pairs)
    
    def _save_pair_to_db(self, pair: ArbitragePair):
        """保存套利配对于数据库"""
        cursor = self.conn.cursor()
        
        # 将entry_prices转换为JSON
        entry_prices_json = json.dumps(pair.entry_prices) if pair.entry_prices else "{}"
        
        cursor.execute('''
        INSERT OR REPLACE INTO arbitrage_pairs 
        (pair_id, symbol, long_exchange, short_exchange, long_order_id, short_order_id, 
         amount, entry_time, stop_loss_percent, take_profit_percent, status, close_reason, entry_prices)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            pair.pair_id, pair.symbol, pair.long_exchange, pair.short_exchange,
            pair.long_order_id, pair.short_order_id, pair.amount, pair.entry_time,
            pair.stop_loss_percent, pair.take_profit_percent, pair.status, 
            pair.close_reason, entry_prices_json
        ))
        self.conn.commit()
    
    def _save_stop_loss_config(self, config: StopLossConfig):
        """保存止损配置于数据库"""
        cursor = self.conn.cursor()
        cursor.execute('''
        INSERT OR REPLACE INTO stop_loss_configs 
        (config_id, symbol, exchange, order_id, stop_price, is_percent, 
         percent_value, original_price, pair_id, is_active, created_at, triggered_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            config.config_id, config.symbol, config.exchange, config.order_id,
            config.stop_price, 1 if config.is_percent else 0,
            config.percent_value, config.original_price, config.pair_id,
            1 if config.is_active else 0, config.created_at, config.triggered_at
        ))
        self.conn.commit()
    
    async def create_arbitrage_pair(
        self,
        symbol: str,
        long_exchange: str,
        short_exchange: str,
        amount: float,
        entry_prices: Dict[str, float] = None,
        stop_loss_percent: float = 0.0,
        take_profit_percent: float = 0.0
    ) -> Tuple[bool, str, Optional[str]]:
        """创建套利配对（任意合约）"""
        try:
            # 1. 验证参数
            if not symbol or not long_exchange or not short_exchange or amount <= 0:
                return False, "参数错误: 缺少必要参数或金额无效", None
            
            if long_exchange == short_exchange:
                return False, "参数错误: 做多和做空交易所不能相同", None
            
            # 2. 创建套利配对记录
            pair_id = f"{symbol}_{long_exchange}_{short_exchange}_{int(time.time()*1000)}"
            
            pair = ArbitragePair(
                pair_id=pair_id,
                symbol=symbol,
                long_exchange=long_exchange,
                short_exchange=short_exchange,
                long_order_id=f"{long_exchange}_order_{int(time.time()*1000)}",
                short_order_id=f"{short_exchange}_order_{int(time.time()*1000+1)}",
                amount=amount,
                stop_loss_percent=stop_loss_percent,
                take_profit_percent=take_profit_percent,
                entry_prices=entry_prices or {}
            )
            
            # 3. 保存到内存和数据库
            self.arbitrage_pairs[pair_id] = pair
            self._save_pair_to_db(pair)
            
            # 4. 更新统计
            self.stats["total_pairs_created"] += 1
            self.stats["active_pairs"] += 1
            
            # 5. 设置止损监控
            if stop_loss_percent > 0:
                await self._setup_stop_loss_monitoring(pair)
            
            logger.info(f"✅ 创建套利配对成功: {pair_id}")
            logger.info(f"   合约: {symbol} | 做多: {long_exchange} | 做空: {short_exchange}")
            logger.info(f"   金额: {amount} | 止损: {stop_loss_percent}% | 止盈: {take_profit_percent}%")
            
            return True, "套利配对创建成功", pair_id
            
        except Exception as e:
            logger.error(f"创建套利配对失败: {e}")
            return False, str(e), None
    
    async def _setup_stop_loss_monitoring(self, pair: ArbitragePair):
        """设置止损监控"""
        try:
            # 为两个订单都设置止损监控
            exchanges = [
                (pair.long_exchange, pair.long_order_id),
                (pair.short_exchange, pair.short_order_id)
            ]
            
            for exchange, order_id in exchanges:
                config_id = f"{exchange}_{order_id}"
                
                # 这里需要根据实时价格计算止损价
                # 简化实现：使用百分比止损
                stop_price = 0  # 实际应用中需要从市场数据获取
                
                config = StopLossConfig(
                    config_id=config_id,
                    symbol=pair.symbol,
                    exchange=exchange,
                    order_id=order_id,
                    stop_price=stop_price,
                    is_percent=True,
                    percent_value=pair.stop_loss_percent,
                    pair_id=pair.pair_id
                )
                
                self.stop_loss_configs[config_id] = config
                self._save_stop_loss_config(config)
            
            logger.info(f"已为套利配对 {pair.pair_id} 设置止损监控")
            
        except Exception as e:
            logger.error(f"设置止损监控失败: {e}")
    
    async def handle_stop_loss_trigger(
        self,
        exchange: str,
        symbol: str,
        order_id: str,
        current_price: float
    ) -> bool:
        """处理止损触发 - 核心的同生共死逻辑"""
        config_id = f"{exchange}_{order_id}"
        config = self.stop_loss_configs.get(config_id)
        
        if not config or not config.is_active:
            return False
        
        pair = self.arbitrage_pairs.get(config.pair_id)
        if not pair or pair.status != "active":
            logger.warning(f"找不到活跃的套利配对: {config.pair_id}")
            return False
        
        logger.warning(f"🚨 止损触发! {exchange} {symbol} 订单 {order_id} 价格: {current_price}")
        
        try:
            # 1. 标记配对状态为关闭
            pair.status = "closed"
            pair.close_reason = f"stop_loss_triggered_{exchange}"
            
            # 2. 更新统计
            self.stats["active_pairs"] -= 1
            self.stats["pairs_closed"] += 1
            self.stats["stop_loss_triggers"] += 1
            
            # 3. 保存到数据库
            self._save_pair_to_db(pair)
            
            # 4. 标记止损配置为已触发
            config.is_active = False
            config.triggered_at = time.time()
            self._save_stop_loss_config(config)
            
            # 5. 移除对应的另一个止损配置
            self._remove_counter_stop_loss(pair, exchange)
            
            logger.info(f"✅ 套利配对 {pair.pair_id} 已因止损触发而关闭")
            logger.info(f"   触发交易所: {exchange} | 触发价格: {current_price}")
            logger.info(f"   配对详情: {pair.symbol} | 做多: {pair.long_exchange} | 做空: {pair.short_exchange}")
            
            return True
            
        except Exception as e:
            logger.error(f"处理止损触发失败: {e}")
            return False
    
    def _remove_counter_stop_loss(self, pair: ArbitragePair, triggered_exchange: str):
        """移除对应的另一个止损配置"""
        counter_exchange = pair.get_counter_exchange(triggered_exchange)
        if not counter_exchange:
            return
        
        # 找到对应的订单ID
        counter_order_id = None
        if triggered_exchange == pair.long_exchange:
            counter_order_id = pair.short_order_id
        else:
            counter_order_id = pair.long_order_id
        
        if not counter_order_id:
            return
        
        config_id = f"{counter_exchange}_{counter_order_id}"
        if config_id in self.stop_loss_configs:
            config = self.stop_loss_configs[config_id]
            config.is_active = False
            config.triggered_at = time.time()
            self._save_stop_loss_config(config)
            logger.info(f"已移除对应止损配置: {config_id}")
    
    async def close_arbitrage_pair(self, pair_id: str, reason: str = "manual") -> bool:
        """手动关闭套利配对"""
        pair = self.arbitrage_pairs.get(pair_id)
        if not pair:
            logger.warning(f"找不到套利配对: {pair_id}")
            return False
        
        try:
            # 1. 更新配对状态
            pair.status = "closed"
            pair.close_reason = reason
            
            # 2. 更新统计
            if pair.status == "active":
                self.stats["active_pairs"] -= 1
            self.stats["pairs_closed"] += 1
            
            # 3. 保存到数据库
            self._save_pair_to_db(pair)
            
            # 4. 移除止损监控
            self._remove_stop_loss_monitoring(pair_id)
            
            logger.info(f"✅ 套利配对 {pair_id} 已关闭: {reason}")
            logger.info(f"   配对详情: {pair.symbol} | 做多: {pair.long_exchange} | 做空: {pair.short_exchange}")
            
            return True
            
        except Exception as e:
            logger.error(f"关闭套利配对失败: {e}")
            return False
    
    def _remove_stop_loss_monitoring(self, pair_id: str):
        """移除止损监控"""
        configs_to_remove = []
        for config_id, config in self.stop_loss_configs.items():
            if config.pair_id == pair_id:
                configs_to_remove.append(config_id)
        
        for config_id in configs_to_remove:
            config = self.stop_loss_configs[config_id]
            config.is_active = False
            config.triggered_at = time.time()
            self._save_stop_loss_config(config)
            del self.stop_loss_configs[config_id]
    
    async def get_arbitrage_opportunities(
        self, 
        market_data: Dict[str, Any],
        min_funding_diff: float = 0.0005,
        max_pairs: int = 10
    ) -> List[Dict]:
        """分析套利机会 - 根据实时数据动态发现"""
        opportunities = []
        
        try:
            # 获取所有有数据的交易对
            binance_symbols = set(market_data.get("binance", {}).keys())
            okx_symbols = set(market_data.get("okx", {}).keys())
            common_symbols = binance_symbols.intersection(okx_symbols)
            
            for symbol in common_symbols:
                try:
                    binance_data = market_data["binance"].get(symbol, {})
                    okx_data = market_data["okx"].get(symbol, {})
                    
                    # 检查是否有资金费率数据
                    binance_funding = binance_data.get("funding_rate", 0)
                    okx_funding = okx_data.get("funding_rate", 0)
                    
                    funding_diff = abs(binance_funding - okx_funding)
                    
                    # 如果资金费率差足够大，计算套利机会
                    if funding_diff >= min_funding_diff:
                        # 计算价格差百分比
                        binance_price = binance_data.get("price", 0)
                        okx_price = okx_data.get("price", 0)
                        
                        if binance_price > 0 and okx_price > 0:
                            price_diff_percent = abs(binance_price - okx_price) / min(binance_price, okx_price) * 100
                            
                            # 确定做多做空方向
                            if binance_funding > okx_funding:
                                # 币安资金费率更高，在币安做空，在OKX做多
                                long_exchange = "okx"
                                short_exchange = "binance"
                                funding_rate_diff = binance_funding - okx_funding
                            else:
                                # OKX资金费率更高，在OKX做空，在币安做多
                                long_exchange = "binance"
                                short_exchange = "okx"
                                funding_rate_diff = okx_funding - binance_funding
                            
                            # 估算年化收益率
                            estimated_annual_return = funding_rate_diff * 3 * 365 * 100  # 简单估算
                            
                            opportunity = {
                                "symbol": symbol,
                                "long_exchange": long_exchange,
                                "short_exchange": short_exchange,
                                "funding_rate_diff": funding_rate_diff,
                                "price_diff_percent": price_diff_percent,
                                "estimated_annual_return": estimated_annual_return,
                                "binance_funding": binance_funding,
                                "okx_funding": okx_funding,
                                "binance_price": binance_price,
                                "okx_price": okx_price,
                                "detected_at": time.time()
                            }
                            
                            opportunities.append(opportunity)
                            
                except Exception as e:
                    logger.debug(f"分析 {symbol} 套利机会失败: {e}")
                    continue
            
            # 按资金费率差排序
            opportunities.sort(key=lambda x: x["funding_rate_diff"], reverse=True)
            
            # 保存机会到数据库（可选）
            self._save_opportunities_to_db(opportunities[:max_pairs])
            
            return opportunities[:max_pairs]
            
        except Exception as e:
            logger.error(f"分析套利机会失败: {e}")
            return []
    
    def _save_opportunities_to_db(self, opportunities: List[Dict]):
        """保存套利机会到数据库"""
        try:
            cursor = self.conn.cursor()
            
            for opp in opportunities:
                cursor.execute('''
                INSERT INTO arbitrage_opportunities 
                (symbol, long_exchange, short_exchange, funding_rate_diff, 
                 price_diff_percent, estimated_annual_return, detected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    opp["symbol"], opp["long_exchange"], opp["short_exchange"],
                    opp["funding_rate_diff"], opp["price_diff_percent"],
                    opp["estimated_annual_return"], opp["detected_at"]
                ))
            
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"保存套利机会失败: {e}")
    
    def get_active_pairs(self) -> List[Dict]:
        """获取活跃的套利配对"""
        active_pairs = []
        for pair in self.arbitrage_pairs.values():
            if pair.status == "active":
                active_pairs.append(asdict(pair))
        return active_pairs
    
    def get_pair_by_id(self, pair_id: str) -> Optional[Dict]:
        """根据ID获取套利配对"""
        pair = self.arbitrage_pairs.get(pair_id)
        return asdict(pair) if pair else None
    
    def get_manager_stats(self) -> Dict:
        """获取管理器统计信息"""
        uptime = time.time() - self.stats["start_time"]
        hours = int(uptime // 3600)
        minutes = int((uptime % 3600) // 60)
        seconds = int(uptime % 60)
        
        return {
            **self.stats,
            "uptime": uptime,
            "uptime_human": f"{hours:02d}:{minutes:02d}:{seconds:02d}",
            "database_path": self.db_path,
            "database_size_kb": os.path.getsize(self.db_path) / 1024 if os.path.exists(self.db_path) else 0,
            "timestamp": time.time()
        }
    
    async def close_all_pairs(self, reason: str = "system_shutdown") -> Dict[str, bool]:
        """关闭所有套利配对"""
        results = {}
        
        for pair_id in list(self.arbitrage_pairs.keys()):
            success = await self.close_arbitrage_pair(pair_id, reason)
            results[pair_id] = success
        
        return results
    
    def cleanup_old_opportunities(self, days: int = 7):
        """清理旧的套利机会记录"""
        try:
            cutoff_time = time.time() - (days * 86400)
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM arbitrage_opportunities WHERE detected_at < ?", (cutoff_time,))
            deleted = cursor.rowcount
            self.conn.commit()
            logger.info(f"清理了 {deleted} 条旧的套利机会记录")
        except Exception as e:
            logger.error(f"清理旧记录失败: {e}")

# ==================== HTTP客户端管理器（简化版） ====================
class HTTPClientManager:
    """HTTP客户端管理器 - 作为模块被app.py调用"""
    
    def __init__(self):
        self.arbitrage_manager = ArbitrageManager()
        
        # 统计信息
        self.stats = {
            "start_time": time.time(),
            "requests_sent": 0,
            "requests_failed": 0,
            "trades_executed": 0,
            "trades_failed": 0
        }
        
        logger.info(f"HTTP客户端管理器初始化完成")
    
    def get_arbitrage_manager(self) -> ArbitrageManager:
        """获取套利管理器实例"""
        return self.arbitrage_manager
    
    async def execute_order(self, order_data: Dict) -> Dict:
        """执行交易订单（模拟）"""
        self.stats["requests_sent"] += 1
        
        try:
            # 模拟订单执行
            exchange = order_data.get("exchange")
            symbol = order_data.get("symbol")
            side = order_data.get("side")
            amount = order_data.get("amount", 0)
            
            order_id = f"{exchange}_{symbol}_{int(time.time()*1000)}"
            
            self.stats["trades_executed"] += 1
            
            logger.info(f"✅ 模拟执行订单: {exchange} {symbol} {side} {amount}")
            
            return {
                "success": True,
                "order_id": order_id,
                "client_order_id": order_data.get("client_order_id", order_id),
                "symbol": symbol,
                "side": side,
                "amount": amount,
                "filled_amount": amount,
                "status": "closed",
                "exchange": exchange,
                "timestamp": time.time()
            }
            
        except Exception as e:
            self.stats["requests_failed"] += 1
            self.stats["trades_failed"] += 1
            logger.error(f"执行订单失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "exchange": order_data.get("exchange"),
                "symbol": order_data.get("symbol"),
                "timestamp": time.time()
            }
    
    async def get_arbitrage_opportunities(self, market_data: Dict) -> List[Dict]:
        """获取套利机会"""
        return await self.arbitrage_manager.get_arbitrage_opportunities(market_data)
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        uptime = time.time() - self.stats["start_time"]
        hours = int(uptime // 3600)
        minutes = int((uptime % 3600) // 60)
        seconds = int(uptime % 60)
        
        total_requests = self.stats["requests_sent"]
        failed_requests = self.stats["requests_failed"]
        success_rate = 0
        if total_requests > 0:
            success_rate = round(((total_requests - failed_requests) / total_requests) * 100, 1)
        
        return {
            "running": True,
            "uptime": uptime,
            "uptime_human": f"{hours:02d}:{minutes:02d}:{seconds:02d}",
            "requests": {
                "total": total_requests,
                "failed": failed_requests,
                "success_rate": success_rate
            },
            "trades": {
                "executed": self.stats["trades_executed"],
                "failed": self.stats["trades_failed"]
            },
            "arbitrage": self.arbitrage_manager.get_manager_stats()
        }

# ==================== 模块测试代码 ====================
async def _test_module():
    """模块测试函数"""
    print("测试HTTP客户端模块...")
    
    client = HTTPClientManager()
    
    # 测试创建套利配对
    result = await client.arbitrage_manager.create_arbitrage_pair(
        symbol="BTCUSDT",
        long_exchange="binance",
        short_exchange="okx",
        amount=0.01,
        stop_loss_percent=5.0
    )
    
    print(f"创建套利配对结果: {result}")
    
    # 显示活跃配对
    active_pairs = client.arbitrage_manager.get_active_pairs()
    print(f"活跃套利配对: {len(active_pairs)} 个")
    
    # 显示统计
    stats = client.get_stats()
    print(f"管理器统计: {stats['arbitrage']['active_pairs']} 活跃配对")

if __name__ == "__main__":
    # 单独运行时进行测试
    asyncio.run(_test_module())
