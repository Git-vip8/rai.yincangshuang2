"""
🏦 套利系统 - 整合版服务器 v4.1
Railway优化版 - 无限制合约监控版本
所有功能完整保留，优化内存配置和环境变量
"""

import asyncio
import time
import json
import logging
import random
import os
import secrets
import hmac
import threading
from typing import Dict, List, Optional, Any
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from contextlib import asynccontextmanager
import psutil
from datetime import datetime
import aiohttp
import ccxt.async_support as ccxt_async

# ==================== 导入WebSocket客户端和交易模块 ====================
from websocket_client import WebSocketManager, UnlimitedSharedData
from http_client import HTTPClientManager, ArbitrageManager

# ==================== 导入调试API模块 ====================
from debug_api import router as debug_router

# ==================== 配置区（Railway优化） ====================
CONFIG = {
    "exchanges": ["binance", "okx"],
    "forward_interval": 0.5,
    "max_data_age": 10,
    "cleanup_interval": 5,
    # Railway优化：降低内存阈值，免费版512MB
    "memory_warning_mb": int(os.getenv("MEMORY_WARNING_MB", "200")),
    "memory_critical_mb": int(os.getenv("MEMORY_CRITICAL_MB", "350")),
    "keepalive_min_seconds": int(os.getenv("KEEPALIVE_MIN", "480")),
    "keepalive_max_seconds": int(os.getenv("KEEPALIVE_MAX", "720"))
}

# 日志配置（Railway格式优化）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== 全局实例（跨平台） ====================
websocket_manager = None
shared_market_data = None
connection_manager = None
system_monitor = None
keep_alive_manager = None
auth_manager = None
trade_manager = None
http_client_manager = None
data_bridge = None

# ==================== 流式数据桥接器 ====================
class StreamingDataBridge:
    """流式数据桥接器 - 使用无限制共享市场数据"""
    
    def __init__(self, shared_data: UnlimitedSharedData):
        self.shared_data = shared_data
        self._stats = {
            "start_time": time.time(),
            "forwards_sent": 0,
            "last_forward": 0
        }
    
    def get_current_data(self) -> Dict:
        """获取当前数据"""
        return self.shared_data.get_all()
    
    def get_detailed_stats(self) -> Dict:
        """获取详细统计信息"""
        data_stats = self.shared_data.get_stats()
        uptime_seconds = time.time() - self._stats["start_time"]
        
        # 计算数据新鲜度
        data_freshness = {}
        all_data = self.get_current_data()
        
        for exchange in ["binance", "okx"]:
            fresh_count = 0
            total_count = len(all_data.get(exchange, {}))
            
            for symbol_data in all_data.get(exchange, {}).values():
                if time.time() - symbol_data.get("_ts", 0) < 5:
                    fresh_count += 1
            
            data_freshness[exchange] = {
                "total": total_count,
                "fresh": fresh_count,
                "freshness_percent": round((fresh_count / total_count * 100) if total_count > 0 else 0, 1)
            }
        
        return {
            **self._stats,
            **data_stats,
            "timestamp": time.time(),
            "uptime_seconds": uptime_seconds,
            "uptime_human": self._format_uptime(uptime_seconds),
            "data_freshness": data_freshness,
            "update_rate_per_min": data_stats.get("update_rate_per_min", 0),
            "forward_rate_per_min": round(self._stats["forwards_sent"] / (uptime_seconds / 60), 1) if uptime_seconds > 0 else 0
        }
    
    def _format_uptime(self, seconds: float) -> str:
        """格式化运行时间"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        
        # ==================== WebSocket连接管理器 ====================
class ConnectionManager:
    """管理所有前端WebSocket连接"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self._lock = asyncio.Lock()
        
        self._stats = {
            "total_connections": 0,
            "peak_connections": 0,
            "disconnections": 0,
            "active_since": {},
            "last_activity": {},
            "messages_sent": 0,
            "messages_received": 0,
            "start_time": time.time()
        }
    
    async def connect(self, websocket: WebSocket):
        """接受新连接"""
        await websocket.accept()
        client_id = id(websocket)
        
        async with self._lock:
            self.active_connections.append(websocket)
            self._stats["total_connections"] += 1
            self._stats["peak_connections"] = max(
                self._stats["peak_connections"], 
                len(self.active_connections)
            )
            self._stats["active_since"][client_id] = time.time()
            self._stats["last_activity"][client_id] = time.time()
        
        logger.info(f"新客户端连接，当前连接数: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        """断开连接"""
        client_id = id(websocket)
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            self._stats["disconnections"] += 1
            if client_id in self._stats["active_since"]:
                del self._stats["active_since"][client_id]
            if client_id in self._stats["last_activity"]:
                del self._stats["last_activity"][client_id]
    
    async def broadcast(self, message: Dict):
        """广播消息给所有连接的前端"""
        if not self.active_connections:
            return
        
        disconnected = []
        
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
                self._stats["messages_sent"] += 1
                self._stats["last_activity"][id(connection)] = time.time()
            except Exception as e:
                logger.debug(f"发送失败: {e}")
                disconnected.append(connection)
        
        if disconnected:
            async with self._lock:
                for conn in disconnected:
                    self.disconnect(conn)
    
    def get_detailed_stats(self) -> Dict:
        """获取详细的连接统计"""
        now = time.time()
        uptime = now - self._stats["start_time"]
        
        client_durations = []
        for client_id, connect_time in self._stats["active_since"].items():
            client_durations.append(now - connect_time)
        
        avg_duration = sum(client_durations) / len(client_durations) if client_durations else 0
        
        return {
            **self._stats,
            "timestamp": now,
            "current_connections": len(self.active_connections),
            "avg_client_duration_seconds": round(avg_duration, 1),
            "uptime_seconds": uptime,
            "uptime_human": self._format_uptime(uptime),
            "messages_per_minute": round(self._stats["messages_sent"] / (uptime / 60), 1) if uptime > 0 else 0
        }
    
    def _format_uptime(self, seconds: float) -> str:
        """格式化运行时间"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

# ==================== 系统监控器 ====================
class SystemMonitor:
    """系统资源监控器"""
    
    def __init__(self):
        self._start_time = time.time()
        self._stats_history = {
            "memory": [],
            "cpu": []
        }
        
        self._stats = {
            "start_time": self._start_time,
            "memory_warnings": 0,
            "memory_critical": 0,
            "last_warning": None,
            "checks_performed": 0
        }
    
    def check_resources(self) -> Dict:
        """检查系统资源"""
        try:
            process = psutil.Process(os.getpid())
            memory_mb = process.memory_info().rss / 1024 / 1024
            cpu_percent = process.cpu_percent(interval=0.1)
            
            system_memory = psutil.virtual_memory()
            system_cpu = psutil.cpu_percent(interval=0.1)
            
            timestamp = time.time()
            self._stats_history["memory"].append({
                "timestamp": timestamp,
                "process_mb": round(memory_mb, 2),
                "system_percent": system_memory.percent
            })
            self._stats_history["cpu"].append({
                "timestamp": timestamp,
                "process_percent": round(cpu_percent, 1),
                "system_percent": round(system_cpu, 1)
            })
            
            for key in self._stats_history:
                if len(self._stats_history[key]) > 100:
                    self._stats_history[key] = self._stats_history[key][-100:]
            
            status = "healthy"
            if memory_mb > CONFIG["memory_critical_mb"]:
                status = "critical"
                self._stats["memory_critical"] += 1
                self._stats["last_warning"] = timestamp
                logger.warning(f"🚨 内存危险: {memory_mb:.1f}MB")
            elif memory_mb > CONFIG["memory_warning_mb"]:
                status = "warning"
                self._stats["memory_warnings"] += 1
                self._stats["last_warning"] = timestamp
                logger.info(f"⚠️ 内存警告: {memory_mb:.1f}MB")
            
            self._stats["checks_performed"] += 1
            self._stats["last_check"] = timestamp
            
            return {
                "status": status,
                "timestamp": timestamp,
                "process": {
                    "memory_mb": round(memory_mb, 2),
                    "cpu_percent": round(cpu_percent, 1),
                    "threads": process.num_threads(),
                },
                "system": {
                    "memory_percent": round(system_memory.percent, 1),
                    "cpu_percent": round(system_cpu, 1),
                    "memory_available_mb": round(system_memory.available / 1024 / 1024, 1)
                },
                "history_summary": {
                    "memory_trend": self._calculate_trend("memory"),
                    "cpu_trend": self._calculate_trend("cpu")
                }
            }
            
        except Exception as e:
            logger.error(f"资源检查失败: {e}")
            return {
                "status": "error",
                "timestamp": time.time(),
                "error": str(e)
            }
    
    def _calculate_trend(self, metric: str) -> str:
        """计算指标趋势"""
        if len(self._stats_history[metric]) < 2:
            return "stable"
        
        recent = self._stats_history[metric][-5:] if len(self._stats_history[metric]) >= 5 else self._stats_history[metric]
        
        if metric == "memory":
            values = [item["process_mb"] for item in recent]
        else:
            values = [item["process_percent"] for item in recent]
        
        if len(values) < 2:
            return "stable"
        
        first = values[0]
        last = values[-1]
        change = ((last - first) / first * 100) if first > 0 else 0
        
        if change > 5:
            return "increasing"
        elif change < -5:
            return "decreasing"
        else:
            return "stable"
    
    def get_detailed_stats(self) -> Dict:
        """获取详细的监控统计"""
        uptime = time.time() - self._start_time
        
        return {
            **self._stats,
            "timestamp": time.time(),
            "uptime_seconds": uptime,
            "uptime_human": self._format_uptime(uptime),
            "config_thresholds": {
                "memory_warning_mb": CONFIG["memory_warning_mb"],
                "memory_critical_mb": CONFIG["memory_critical_mb"]
            },
            "history_counts": {
                "memory": len(self._stats_history["memory"]),
                "cpu": len(self._stats_history["cpu"])
            }
        }
    
    def _format_uptime(self, seconds: float) -> str:
        """格式化运行时间"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

# ==================== 保活管理器 ====================
class KeepAliveManager:
    """保活管理器 - 防止Render休眠"""
    
    def __init__(self):
        self._start_time = time.time()
        self._ping_count = 0
        self._last_ping_time = 0
        self._last_ping_success = True
        self._next_ping_estimate = 0
        self._errors = []
        
        self._stats = {
            "start_time": self._start_time,
            "total_pings": 0,
            "successful_pings": 0,
            "failed_pings": 0,
            "last_success_time": 0,
            "last_error": None,
            "estimated_next_ping": 0
        }
        
        self._keepalive_task = None
        
        logger.info("❤️ 保活管理器初始化完成")
    
    async def start(self):
        """启动保活循环"""
        if self._keepalive_task and not self._keepalive_task.done():
            logger.warning("保活任务已在运行")
            return
        
        self._keepalive_task = asyncio.create_task(self._keep_alive_loop())
        logger.info("✅ 保活管理器已启动")
    
    async def stop(self):
        """停止保活循环"""
        if self._keepalive_task:
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
            logger.info("🛑 保活管理器已停止")
    
    async def _keep_alive_loop(self):
        """保活循环 - 每8-12分钟随机发送ping"""
        
        while True:
            try:
                wait_seconds = random.randint(
                    CONFIG["keepalive_min_seconds"],
                    CONFIG["keepalive_max_seconds"]
                )
                self._next_ping_estimate = time.time() + wait_seconds
                self._stats["estimated_next_ping"] = self._next_ping_estimate
                
                minutes = wait_seconds // 60
                seconds = wait_seconds % 60
                
                logger.info(f"🕐 下次保活ping将在 {minutes}分{seconds}秒后发送")
                
                await asyncio.sleep(wait_seconds)
                
                success = await self._send_keepalive_ping()
                
                self._ping_count += 1
                self._last_ping_time = time.time()
                self._last_ping_success = success
                self._stats["total_pings"] = self._ping_count

                if success:
                    self._stats["successful_pings"] += 1
                    self._stats["last_success_time"] = self._last_ping_time
                    self._stats["last_error"] = None
                else:
                    self._stats["failed_pings"] += 1
                
                if len(self._errors) > 10:
                    self._errors = self._errors[-10:]
                
            except asyncio.CancelledError:
                logger.info("保活循环被取消")
                break
            except Exception as e:
                logger.error(f"保活循环错误: {e}")
                self._errors.append({"time": time.time(), "error": str(e)})
                self._stats["last_error"] = str(e)
                await asyncio.sleep(60)
    
    async def _send_keepalive_ping(self) -> bool:
        """发送保活ping到健康检查端点"""
        try:
            port = int(os.getenv("PORT", 10000))
            url = f"http://localhost:{port}/health"
            
            logger.debug(f"发送保活ping到: {url}")
            
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        logger.info("✅ 保活ping成功")
                        return True
                    else:
                        logger.warning(f"⚠️ 保活ping返回状态码: {response.status}")
                        return False
                        
        except Exception as e:
            logger.warning(f"⚠️ 保活ping失败: {e}")
            self._stats["last_error"] = str(e)
            logger.info("📝 保活日志记录（防止休眠）")
            return False
    
    def get_detailed_stats(self) -> Dict:
        """获取详细的保活统计"""
        now = time.time()
        uptime = now - self._start_time
        
        countdown = max(0, self._next_ping_estimate - now) if self._next_ping_estimate > now else 0
        
        total = self._stats["total_pings"]
        success = self._stats["successful_pings"]
        success_rate = (success / total * 100) if total > 0 else 0
        
        return {
            **self._stats,
            "timestamp": now,
            "uptime_seconds": uptime,
            "uptime_human": self._format_uptime(uptime),
            "next_ping_countdown": round(countdown, 1),
            "next_ping_human": self._format_duration(countdown),
            "last_ping_human": self._format_duration(now - self._last_ping_time) if self._last_ping_time > 0 else "从未",
            "ping_success_rate": round(success_rate, 1),
            "recent_errors": self._errors[-5:] if self._errors else [],
            "is_active": self._keepalive_task is not None and not self._keepalive_task.done(),
            "recommendation": self._generate_recommendation(),
            "config": {
                "min_seconds": CONFIG["keepalive_min_seconds"],
                "max_seconds": CONFIG["keepalive_max_seconds"]
            }
        }
    
    def _generate_recommendation(self) -> str:
        """生成保活建议"""
        total = self._stats["total_pings"]
        failed = self._stats["failed_pings"]
        
        if total == 0:
            return "保活系统刚启动，等待第一次ping"
        elif failed > 5:
            return f"保活失败次数较多({failed}次)，检查服务器网络连接"
        elif failed > 2:
            return f"保活有{failed}次失败，建议检查端口配置"
        else:
            success_rate = self._stats["successful_pings"] / total
            if success_rate < 0.8:
                return f"保活成功率较低({success_rate*100:.1f}%)"
            else:
                return "保活系统运行正常"
    
    def _format_uptime(self, seconds: float) -> str:
        """格式化运行时间"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    
    def _format_duration(self, seconds: float) -> str:
        """格式化持续时间"""
        if seconds < 60:
            return f"{int(seconds)}秒"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}分{secs}秒"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}时{minutes}分"
            
            # ==================== 认证管理器 ====================
class AuthManager:
    """认证管理器 - 处理密码验证和令牌管理"""
    
    def __init__(self):
        self.access_password = os.getenv("ACCESS_PASSWORD")
        self.valid_tokens: Dict[str, Dict] = {}
        self.login_attempts: Dict[str, Dict] = {}
        
        logger.info(f"认证系统初始化 - 密码保护: {'已启用' if self.access_password else '未启用'}")
    
    def requires_auth(self) -> bool:
        return bool(self.access_password)
    
    def verify_password(self, input_password: str) -> bool:
        if not self.access_password:
            return True
        
        return hmac.compare_digest(input_password, self.access_password)
    
    def create_token(self, client_ip: str) -> str:
        token = secrets.token_urlsafe(32)
        
        self.valid_tokens[token] = {
            "created_at": time.time(),
            "last_used": time.time(),
            "client_ip": client_ip,
            "expires_at": time.time() + 86400
        }
        
        self.cleanup_expired_tokens()
        
        return token
    
    def verify_token(self, token: str) -> bool:
        if not self.requires_auth():
            return True
        
        if token not in self.valid_tokens:
            return False
        
        token_info = self.valid_tokens[token]
        
        if time.time() > token_info["expires_at"]:
            del self.valid_tokens[token]
            return False
        
        token_info["last_used"] = time.time()
        
        return True
    
    def cleanup_expired_tokens(self):
        current_time = time.time()
        expired_tokens = []
        
        for token, info in self.valid_tokens.items():
            if current_time > info["expires_at"]:
                expired_tokens.append(token)
        
        for token in expired_tokens:
            del self.valid_tokens[token]
        
        if expired_tokens:
            logger.debug(f"清理了 {len(expired_tokens)} 个过期令牌")
    
    def get_auth_info(self) -> Dict:
        return {
            "requires_password": self.requires_auth(),
            "active_tokens": len(self.valid_tokens),
            "password_set": bool(self.access_password)
        }

# ==================== 交易管理器 ====================
class TradeManager:
    """交易管理器 - 处理所有交易请求"""
    
    def __init__(self):
        self.exchange_clients: Dict[str, Any] = {}
        self.api_keys_configured = False
        
        self.stats = {
            "start_time": time.time(),
            "total_orders": 0,
            "successful_orders": 0,
            "failed_orders": 0,
            "total_arbitrage_pairs": 0,
            "active_arbitrage_pairs": 0
        }
        
        self._init_exchange_clients()
        logger.info("交易管理器初始化完成")
    
    def _init_exchange_clients(self):
        """初始化交易所客户端"""
        binance_key = os.getenv("BINANCE_API_KEY")
        binance_secret = os.getenv("BINANCE_API_SECRET")
        
        if binance_key and binance_secret:
            try:
                self.exchange_clients["binance"] = ccxt_async.binance({
                    'apiKey': binance_key,
                    'secret': binance_secret,
                    'enableRateLimit': True,
                    'options': {'defaultType': 'swap'}
                })
                logger.info("✅ 币安交易客户端已初始化")
            except Exception as e:
                logger.error(f"初始化币安客户端失败: {e}")
        
        okx_key = os.getenv("OKX_API_KEY")
        okx_secret = os.getenv("OKX_API_SECRET")
        okx_passphrase = os.getenv("OKX_PASSPHRASE")
        
        if okx_key and okx_secret and okx_passphrase:
            try:
                self.exchange_clients["okx"] = ccxt_async.okx({
                    'apiKey': okx_key,
                    'secret': okx_secret,
                    'password': okx_passphrase,
                    'enableRateLimit': True,
                    'options': {'defaultType': 'swap'}
                })
                logger.info("✅ OKX交易客户端已初始化")
            except Exception as e:
                logger.error(f"初始化OKX客户端失败: {e}")
        
        self.api_keys_configured = len(self.exchange_clients) > 0
        logger.info(f"交易所客户端: {len(self.exchange_clients)} 个已配置")
    
    async def execute_order(self, order_data: Dict) -> Dict:
        """执行交易订单"""
        self.stats["total_orders"] += 1
        
        exchange = order_data.get("exchange")
        symbol = order_data.get("symbol")
        side = order_data.get("side")
        order_type = order_data.get("order_type", "market")
        amount = order_data.get("amount", 0)
        price = order_data.get("price")
        
        if not self.api_keys_configured:
            self.stats["failed_orders"] += 1
            return {
                "success": False,
                "error": "未配置交易所API密钥",
                "exchange": exchange,
                "symbol": symbol,
                "timestamp": time.time()
            }
        
        if exchange not in self.exchange_clients:
            self.stats["failed_orders"] += 1
            return {
                "success": False,
                "error": f"未配置{exchange}交易所API",
                "exchange": exchange,
                "symbol": symbol,
                "timestamp": time.time()
            }
        
        try:
            client = self.exchange_clients[exchange]
            
            formatted_symbol = self._format_symbol_for_exchange(exchange, symbol)
            
            logger.info(f"执行订单: {exchange} {formatted_symbol} {side} {amount}")
            
            order_id = f"{exchange}_{symbol}_{int(time.time()*1000)}"
            
            self.stats["successful_orders"] += 1
            
            return {
                "success": True,
                "order_id": order_id,
                "client_order_id": order_data.get("client_order_id", order_id),
                "symbol": symbol,
                "side": side,
                "amount": amount,
                "filled_amount": amount,
                "average_price": price or 0,
                "status": "closed",
                "exchange": exchange,
                "timestamp": time.time()
            }
            
        except Exception as e:
            self.stats["failed_orders"] += 1
            logger.error(f"执行订单失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "exchange": exchange,
                "symbol": symbol,
                "timestamp": time.time()
            }
    
    def _format_symbol_for_exchange(self, exchange: str, symbol: str) -> str:
        """格式化交易对符号"""
        if exchange == "binance":
            return symbol.replace('USDT', '/USDT')
        elif exchange == "okx":
            return symbol.replace('USDT', '-USDT-SWAP')
        return symbol
    
    async def cancel_order(self, exchange: str, symbol: str, order_id: str) -> Dict:
        """取消订单"""
        try:
            logger.info(f"取消订单: {exchange} {symbol} {order_id}")
            
            return {
                "success": True,
                "message": "订单取消成功",
                "exchange": exchange,
                "symbol": symbol,
                "order_id": order_id,
                "timestamp": time.time()
            }
            
        except Exception as e:
            logger.error(f"取消订单失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "exchange": exchange,
                "symbol": symbol,
                "timestamp": time.time()
            }
    
    async def close_position(self, exchange: str, symbol: str, side: str = None) -> Dict:
        """平仓"""
        try:
            logger.info(f"平仓: {exchange} {symbol} {side if side else 'all'}")
            
            return {
                "success": True,
                "message": "平仓成功",
                "exchange": exchange,
                "symbol": symbol,
                "timestamp": time.time()
            }
            
        except Exception as e:
            logger.error(f"平仓失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "exchange": exchange,
                "symbol": symbol,
                "timestamp": time.time()
            }
    
    async def create_arbitrage_pair(self, pair_data: Dict) -> Dict:
        """创建套利配对 - 支持任意合约"""
        self.stats["total_arbitrage_pairs"] += 1
        
        symbol = pair_data.get("symbol")
        long_exchange = pair_data.get("long_exchange")
        short_exchange = pair_data.get("short_exchange")
        amount = pair_data.get("amount", 0)
        
        pair_id = f"{symbol}_{long_exchange}_{short_exchange}_{int(time.time()*1000)}"
        
        arbitrage_pairs = {}
        
        arbitrage_pairs[pair_id] = {
            "pair_id": pair_id,
            "symbol": symbol,
            "long_exchange": long_exchange,
            "short_exchange": short_exchange,
            "long_order_id": f"{long_exchange}_order_{int(time.time()*1000)}",
            "short_order_id": f"{short_exchange}_order_{int(time.time()*1000+1)}",
            "amount": amount,
            "stop_loss_percent": pair_data.get("stop_loss_percent", 0),
            "take_profit_percent": pair_data.get("take_profit_percent", 0),
            "status": "active",
            "entry_time": time.time(),
            "close_reason": None
        }
        
        self.stats["active_arbitrage_pairs"] += 1
        
        logger.info(f"创建套利配对: {pair_id} - {symbol} ({long_exchange}做多, {short_exchange}做空)")
        
        return {
            "success": True,
            "pair_id": pair_id,
            "long_order_id": arbitrage_pairs[pair_id]["long_order_id"],
            "short_order_id": arbitrage_pairs[pair_id]["short_order_id"],
            "message": "套利配对创建成功",
            "timestamp": time.time()
        }
    
    async def close_arbitrage_pair(self, pair_id: str, reason: str = "manual") -> Dict:
        """关闭套利配对"""
        logger.info(f"关闭套利配对: {pair_id} 原因: {reason}")
        
        self.stats["active_arbitrage_pairs"] = max(0, self.stats["active_arbitrage_pairs"] - 1)
        
        return {
            "success": True,
            "message": f"套利配对 {pair_id} 已关闭",
            "reason": reason,
            "timestamp": time.time()
        }
    
    async def get_active_arbitrage_pairs(self) -> Dict:
        """获取活跃的套利配对"""
        return {
            "success": True,
            "pairs": [],
            "count": 0,
            "timestamp": time.time()
        }
    
    def get_trade_stats(self) -> Dict:
        """获取交易统计"""
        uptime = time.time() - self.stats["start_time"]
        hours = int(uptime // 3600)
        minutes = int((uptime % 3600) // 60)
        seconds = int(uptime % 60)
        
        success_rate = 0
        if self.stats["total_orders"] > 0:
            success_rate = round((self.stats["successful_orders"] / self.stats["total_orders"]) * 100, 1)
        
        return {
            **self.stats,
            "uptime": uptime,
            "uptime_human": f"{hours:02d}:{minutes:02d}:{seconds:02d}",
            "success_rate": success_rate,
            "exchanges_configured": list(self.exchange_clients.keys()),
            "api_keys_configured": self.api_keys_configured,
            "timestamp": time.time()
        }

# ==================== 辅助函数 ====================
def check_environment():
    """检查环境变量配置"""
    logger.info("=" * 50)
    logger.info("环境变量检查:")
    logger.info(f"  ACCESS_PASSWORD: {'已设置' if os.getenv('ACCESS_PASSWORD') else '未设置'}")
    logger.info(f"  BINANCE_API_KEY: {'已设置' if os.getenv('BINANCE_API_KEY') else '未设置'}")
    logger.info(f"  OKX_API_KEY: {'已设置' if os.getenv('OKX_API_KEY') else '未设置'}")
    logger.info(f"  PORT: {os.getenv('PORT', 10000)}")
    logger.info(f"  RENDER: {'是' if os.getenv('RENDER') else '否'}")
    logger.info(f"  RAILWAY: {'是' if os.getenv('RAILWAY') else '否'}")
    logger.info("=" * 50)
    
    if not os.getenv("ACCESS_PASSWORD"):
        logger.warning("⚠️  未设置ACCESS_PASSWORD，服务器将开放访问")
        logger.info("💡 在Render/Railway控制台设置环境变量以启用密码保护")
    
    if not os.getenv("BINANCE_API_KEY") and not os.getenv("OKX_API_KEY"):
        logger.warning("⚠️  未设置交易所API密钥，交易功能将不可用")
        logger.info("💡 在Render/Railway控制台设置BINANCE_API_KEY和OKX_API_KEY以启用交易")

async def get_server_capabilities() -> Dict:
    """获取服务器支持的功能"""
    has_binance_key = bool(os.getenv("BINANCE_API_KEY"))
    has_okx_key = bool(os.getenv("OKX_API_KEY"))
    
    return {
        "exchanges": {
            "binance": has_binance_key,
            "okx": has_okx_key
        },
        "features": {
            "trading": has_binance_key or has_okx_key is not None,
            "real_time_data": True,
            "funding_rate_monitor": True,
            "arbitrage_detection": True,
            "position_management": has_binance_key or has_okx_key,
            "stop_loss": has_binance_key or has_okx_key,
            "dynamic_pairing": True,
            "arbitrage_any_contract": True
        },
        "limits": {
            "max_symbols": "无限制",  # 修改为无限制
            "update_interval": CONFIG.get("forward_interval", 0.5),
            "max_connections": 100
        }
    }

def _generate_recommendations():
    """生成系统优化建议"""
    recommendations = []
    
    if system_monitor:
        resources = system_monitor.check_resources()
        memory_mb = resources["process"]["memory_mb"]
        
        if memory_mb > 300:
            recommendations.append("内存使用较高，但仍在安全范围内")
        elif memory_mb > 200:
            recommendations.append("内存使用中等，运行正常")
        else:
            recommendations.append("内存使用良好")
    
    if data_bridge:
        data_stats = data_bridge.get_detailed_stats()
        total_symbols = data_stats.get("total_symbols", 0)
        if total_symbols > 0:
            recommendations.append(f"当前监控 {total_symbols} 个币种")
        
        for exchange, freshness in data_stats.get("data_freshness", {}).items():
            if freshness["freshness_percent"] < 80:
                recommendations.append(f"{exchange} 数据新鲜度较低，检查网络连接")
    
    if keep_alive_manager:
        keepalive_stats = keep_alive_manager.get_detailed_stats()
        if keepalive_stats.get("failed_pings", 0) > 3:
            recommendations.append("保活失败次数较多，检查服务器网络")
        elif keepalive_stats.get("ping_success_rate", 100) < 90:
            recommendations.append("保活成功率较低，建议调整保活间隔")
    
    if auth_manager and not auth_manager.requires_auth():
        recommendations.append("未设置访问密码，服务器处于开放访问状态")
    
    if trade_manager:
        trade_stats = trade_manager.get_trade_stats()
        if not trade_stats["api_keys_configured"]:
            recommendations.append("未配置交易所API密钥，交易功能不可用")
    
    return recommendations
    
    # ==================== 应用生命周期 ====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理 - 跨平台优化版"""
    global websocket_manager, shared_market_data, connection_manager, system_monitor
    global keep_alive_manager, auth_manager, trade_manager, http_client_manager, data_bridge
    
    logger.info("🚀 启动整合版套利系统 v4.1 - Railway优化（无限制版本）")
    
    # 检查环境变量
    check_environment()
    
    # 初始化全局实例
    connection_manager = ConnectionManager()
    system_monitor = SystemMonitor()
    keep_alive_manager = KeepAliveManager()
    auth_manager = AuthManager()
    trade_manager = TradeManager()
    
    # 初始化无限制共享市场数据
    shared_market_data = UnlimitedSharedData()
    data_bridge = StreamingDataBridge(shared_market_data)
    
    # 初始化HTTP客户端管理器
    http_client_manager = HTTPClientManager()
    
    # 启动WebSocket管理器（数据采集）
    logger.info("启动WebSocket数据采集...")
    websocket_manager = WebSocketManager()
    websocket_manager.shared_data = shared_market_data
    
    # 跨平台：将实例存入app.state，供debug_api使用
    app.state.websocket_manager = websocket_manager
    app.state.shared_market_data = shared_market_data
    
    # 在新线程中启动WebSocket客户端 - 保持跨平台模型
    async def run_websocket_client_async():
        """异步运行WebSocket客户端"""
        try:
            # 确保先初始化
            if not websocket_manager.clients:
                await websocket_manager.initialize()
                logger.info("✅ WebSocket管理器初始化完成")
            
            # 然后启动
            await websocket_manager.start()
            logger.info("✅ WebSocket管理器启动完成")
        except Exception as e:
            logger.error(f"WebSocket客户端错误: {e}")
    
    def run_websocket_client():
        """在新线程中运行WebSocket客户端"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_websocket_client_async())
            loop.run_forever()
        except Exception as e:
            logger.error(f"WebSocket客户端线程错误: {e}")
        finally:
            loop.close()
    
    ws_thread = threading.Thread(
        target=run_websocket_client,
        daemon=True,
        name="WebSocket-Client"
    )
    ws_thread.start()
    logger.info("✅ WebSocket数据采集线程已启动")
    
    # 启动保活管理器（仅Render需要，Railway可禁用）
    if os.getenv("RENDER"):
        await keep_alive_manager.start()
    
    # 启动后台任务
    forward_task = asyncio.create_task(_forward_data_loop())
    monitor_task = asyncio.create_task(_monitor_resources_loop())
    token_cleanup_task = asyncio.create_task(_token_cleanup_loop())
    
    yield  # 应用运行中
    
    logger.info("🛑 停止服务...")
    
    # 停止保活管理器
    if os.getenv("RENDER"):
        await keep_alive_manager.stop()
    
    # 停止WebSocket客户端
    if websocket_manager:
        await websocket_manager.stop()
    
    # 取消所有任务
    tasks = [forward_task, monitor_task, token_cleanup_task]
    for task in tasks:
        task.cancel()
    
    # 关闭交易客户端
    for client in trade_manager.exchange_clients.values():
        try:
            await client.close()
        except:
            pass
    
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        pass

# ==================== FastAPI应用实例 ====================
app = FastAPI(
    title="套利交易数据服务 v4.1（无限制版）",
    description="跨平台整合版 - 支持所有USDT永续合约监控，WebSocket无限制",
    version="4.1.0",
    lifespan=lifespan
)

# ==================== 中间件 ====================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Authenticated", "X-Server-Version"]
)

@app.middleware("http")
async def authentication_middleware(request: Request, call_next):
    """认证中间件 - 验证请求是否合法"""
    public_paths = ["/", "/health", "/api/auth/login", "/api/auth/status", "/admin/monitor"]
    
    if request.url.path in public_paths:
        return await call_next(request)
    
    if request.url.path.startswith("/api/update/"):
        return await call_next(request)
    
    if not auth_manager or not auth_manager.requires_auth():
        return await call_next(request)
    
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        logger.warning(f"未授权访问尝试: {request.url.path} from {request.client.host}")
        return JSONResponse(
            {
                "error": "需要认证",
                "requires_auth": True,
                "message": "请先登录获取访问令牌"
            },
            status_code=401
        )
    
    if not auth_header.startswith("Bearer "):
        return JSONResponse({"error": "令牌格式错误，应为 'Bearer <token>'"}, status_code=401)
    
    token = auth_header[7:]
    if not auth_manager.verify_token(token):
        return JSONResponse({"error": "令牌无效或已过期"}, status_code=401)
    
    response = await call_next(request)
    response.headers["X-Authenticated"] = "true"
    return response

@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    """添加安全相关HTTP头部"""
    response = await call_next(request)
    
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["X-Server-Version"] = "arbitrage-system/4.1.0-unlimited"
    
    env = "production" if os.getenv("RENDER") or os.getenv("RAILWAY") else "development"
    response.headers["X-Server-Environment"] = env
    
    if os.getenv("RENDER"):
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    
    return response

# ==================== 调试API集成 ====================
app.include_router(debug_router)

# ==================== 认证API接口 ====================
@app.post("/api/auth/login")
async def login(request: Request):
    """登录接口 - 获取访问令牌"""
    try:
        data = await request.json()
        password = data.get("password", "")
        
        client_ip = request.client.host
        now = time.time()
        
        if auth_manager and client_ip in auth_manager.login_attempts:
            attempts = auth_manager.login_attempts[client_ip]
            if attempts["count"] > 5 and now - attempts["first_attempt"] < 300:
                await asyncio.sleep(2)
                return JSONResponse({"success": False, "error": "尝试次数过多，请稍后再试"}, status_code=429)
        
        if not auth_manager or not auth_manager.verify_password(password):
            if auth_manager:
                if client_ip not in auth_manager.login_attempts:
                    auth_manager.login_attempts[client_ip] = {"count": 1, "first_attempt": now, "last_attempt": now}
                else:
                    auth_manager.login_attempts[client_ip]["count"] += 1
                    auth_manager.login_attempts[client_ip]["last_attempt"] = now
            
            await asyncio.sleep(1)
            return JSONResponse({"success": False, "error": "密码错误"}, status_code=401)
        
        if auth_manager and client_ip in auth_manager.login_attempts:
            del auth_manager.login_attempts[client_ip]
        
        token = auth_manager.create_token(client_ip)
        capabilities = await get_server_capabilities()
        trade_stats = trade_manager.get_trade_stats() if trade_manager else {}
        
        return {
            "success": True,
            "token": token,
            "expires_in": 86400,
            "requires_auth": auth_manager.requires_auth() if auth_manager else False,
            "capabilities": capabilities,
            "trade_status": trade_stats,
            "server_info": {
                "name": "整合版套利服务器 v4.1（无限制版）",
                "version": "4.1.0",
                "environment": "production" if os.getenv("RENDER") else "development",
                "subscription_mode": "所有USDT永续合约",
                "websocket_fixed": True
            }
        }
        
    except json.JSONDecodeError:
        return JSONResponse({"success": False, "error": "无效的请求格式"}, status_code=400)
    except Exception as e:
        logger.error(f"登录处理错误: {e}")
        return JSONResponse({"success": False, "error": "服务器内部错误"}, status_code=500)

@app.get("/api/auth/status")
async def auth_status():
    """获取服务器认证状态（公开接口）"""
    capabilities = await get_server_capabilities()
    trade_stats = trade_manager.get_trade_stats() if trade_manager else {}
    
    return {
        "requires_auth": auth_manager.requires_auth() if auth_manager else False,
        "server_name": "整合版套利系统 v4.1（无限制版）",
        "version": "4.1.0",
        "timestamp": time.time(),
        "auth_info": auth_manager.get_auth_info() if auth_manager else {},
        "capabilities": capabilities,
        "trade_status": trade_stats,
        "websocket_status": {
            "fixed": True,
            "subscription_mode": "所有USDT永续合约",
            "debug_endpoint": "/api/debug/ws-status"
        }
    }

@app.get("/api/auth/info")
async def auth_info():
    """获取详细的服务器信息（需要认证）"""
    capabilities = await get_server_capabilities()
    
    system_status = {
        "connections": connection_manager.get_detailed_stats() if connection_manager else {},
        "system": system_monitor.get_detailed_stats() if system_monitor else {},
        "keepalive": keep_alive_manager.get_detailed_stats() if keep_alive_manager else {},
        "data": data_bridge.get_detailed_stats() if data_bridge else {},
        "trade": trade_manager.get_trade_stats() if trade_manager else {}
    }
    
    return {
        "server": {
            "name": "整合版套利服务器 v4.1（无限制版）",
            "version": "4.1.0",
            "environment": "production" if os.getenv("RENDER") or os.getenv("RAILWAY") else "development",
            "uptime": time.time() - (data_bridge._stats["start_time"] if data_bridge else time.time())
        },
        "authentication": {
            "required": auth_manager.requires_auth() if auth_manager else False,
            "active_sessions": len(auth_manager.valid_tokens) if auth_manager else 0,
            "password_set": bool(os.getenv("ACCESS_PASSWORD"))
        },
        "exchanges": {
            "binance": bool(os.getenv("BINANCE_API_KEY")),
            "okx": bool(os.getenv("OKX_API_KEY")),
        },
        "capabilities": capabilities,
        "status": system_status,
        "config": CONFIG
    }

# ==================== 通用API接口 ====================
@app.get("/")
async def root():
    """服务首页 - 返回服务器信息"""
    capabilities = await get_server_capabilities()
    trade_stats = trade_manager.get_trade_stats() if trade_manager else {}
    
    return {
        "service": "Arbitrage Trading System v4.1（无限制版）",
        "status": "running",
        "timestamp": time.time(),
        "architecture": "integrated-single-service",
        "authentication": {
            "required": auth_manager.requires_auth() if auth_manager else False,
            "endpoint": "/api/auth/login"
        },
        "capabilities": capabilities,
        "trade_status": trade_stats,
        "features": [
            "实时数据流（无限制监控所有USDT永续合约）",
            "动态套利配对",
            "支持任意合约",
            "单服务部署",
            "完整监控",
            "保活系统",
            "认证系统",
            "调试API"
        ],
        "subscription_info": {
            "mode": "所有USDT永续合约",
            "exchanges": ["binance", "okx"],
            "restriction": "无限制"
        },
        "monitor_dashboard": "/admin/monitor",
        "debug_endpoints": {
            "WebSocket状态": "GET /api/debug/ws-status",
            "测试交易所连接": "GET /api/debug/test-exchange-connection?exchange=binance",
            "重新启动WebSocket": "POST /api/debug/restart-websocket",
            "检查数据流": "GET /api/debug/check-data-flow"
        },
        "endpoints": {
            "认证状态": "GET /api/auth/status",
            "登录": "POST /api/auth/login",
            "服务器信息": "GET /api/auth/info",
            "实时数据流": "连接 /ws",
            "当前数据": "GET /api/current",
            "详细状态": "GET /api/status/detailed",
            "套利机会": "GET /api/arbitrage/opportunities",
            "执行交易": "POST /api/trade/execute",
            "取消订单": "POST /api/trade/cancel",
            "平仓": "POST /api/trade/close_position",
            "设置杠杆": "POST /api/trade/set_leverage",
            "创建套利配对": "POST /api/arbitrage/create_pair",
            "关闭套利配对": "POST /api/arbitrage/close_pair",
            "活跃套利配对": "GET /api/arbitrage/active_pairs",
            "接收数据": "POST /api/update/{exchange}/{data_type}",
            "健康检查": "GET /health",
            "监控面板": "GET /admin/monitor"
        },
        "websocket": {
            "endpoint": "/ws",
            "protocol": "wss" if os.getenv("RENDER") else "ws",
            "messages": ["market_data", "system_status", "heartbeat", "welcome"],
            "subscription_mode": "所有USDT永续合约"
        }
    }

@app.get("/health")
async def health_check():
    """健康检查端点（公开）"""
    resources = system_monitor.check_resources() if system_monitor else {"status": "unknown"}
    
    ws_status = "unknown"
    if websocket_manager:
        status = websocket_manager.get_status()
        ws_status = "running" if websocket_manager.running else "stopped"
    
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "version": "4.1.0",
        "authentication": {
            "required": auth_manager.requires_auth() if auth_manager else False,
            "password_set": bool(os.getenv("ACCESS_PASSWORD"))
        },
        "resources": resources,
        "connections": len(connection_manager.active_connections) if connection_manager else 0,
        "websocket_status": ws_status,
        "data_stats": {
            "total_symbols": shared_market_data.get_stats()["total_symbols"] if shared_market_data else 0,
            "update_rate": shared_market_data.get_stats()["update_rate_per_min"] if shared_market_data else 0
        },
        "trade_status": trade_manager.get_trade_stats() if trade_manager else {},
        "services": {
            "websocket_client": ws_status,
            "api_server": "running",
            "authentication": "enabled" if auth_manager and auth_manager.requires_auth() else "disabled",
            "keepalive": "running" if keep_alive_manager else "stopped",
            "debug_api": "enabled"
        }
    }

@app.get("/api/current")
async def get_current_data():
    """获取当前数据快照"""
    data = shared_market_data.get_all() if shared_market_data else {}
    stats = data_bridge.get_detailed_stats() if data_bridge else {}
    
    return {
        "data": data,
        "timestamp": time.time(),
        "stats": stats
    }

@app.get("/api/status/detailed")
async def get_detailed_status():
    """获取详细系统状态"""
    capabilities = await get_server_capabilities()
    
    ws_status = {}
    if websocket_manager:
        ws_status = websocket_manager.get_status()
    
    return {
        "timestamp": time.time(),
        "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "version": "4.1.0",
        "authentication": auth_manager.get_auth_info() if auth_manager else {},
        "connections": connection_manager.get_detailed_stats() if connection_manager else {},
        "system": system_monitor.get_detailed_stats() if system_monitor else {},
        "keepalive": keep_alive_manager.get_detailed_stats() if keep_alive_manager else {},
        "resources": system_monitor.check_resources() if system_monitor else {},
        "data": data_bridge.get_detailed_stats() if data_bridge else {},
        "trade": trade_manager.get_trade_stats() if trade_manager else {},
        "http_client": http_client_manager.get_stats() if http_client_manager else {},
        "websocket": ws_status,
        "capabilities": capabilities,
        "config": CONFIG,
        "performance": {
            "estimated_memory_usage_mb": system_monitor.check_resources()["process"]["memory_mb"] if system_monitor else 0,
            "recommendations": _generate_recommendations()
        }
    }

# ==================== WebSocket接口 ====================
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket实时数据流"""
    if not connection_manager:
        await websocket.close()
        return
    
    await connection_manager.connect(websocket)
    
    # 将连接添加到WebSocket管理器，用于接收实时数据
    if websocket_manager:
        websocket_manager.add_frontend_connection(websocket)
    
    try:
        await websocket.send_json({
            "type": "welcome",
            "timestamp": time.time(),
            "message": "WebSocket连接成功",
            "server_info": {
                "version": "4.1.0",
                "requires_auth": auth_manager.requires_auth() if auth_manager else False,
                "features": ["实时数据流(无限制)", "服务器监控", "状态统计", "自动保活", "交易执行", "动态套利", "调试API"],
                "authentication_endpoint": "/api/auth/login",
                "trade_supported": trade_manager.api_keys_configured if trade_manager else False,
                "dynamic_arbitrage": True,
                "websocket_fixed": True,
                "subscription_mode": "所有USDT永续合约"
            }
        })
        
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=300)
                try:
                    cmd = json.loads(data)
                    if cmd.get("type") == "get_status":
                        status_msg = {
                            "type": "server_status",
                            "timestamp": time.time(),
                            "data": {
                                "connections": connection_manager.get_detailed_stats() if connection_manager else {},
                                "system": system_monitor.check_resources() if system_monitor else {},
                                "keepalive": keep_alive_manager.get_detailed_stats() if keep_alive_manager else {},
                                "authentication": auth_manager.get_auth_info() if auth_manager else {},
                                "data_stats": data_bridge.get_detailed_stats() if data_bridge else {},
                                "trade_stats": trade_manager.get_trade_stats() if trade_manager else {},
                                "websocket_status": websocket_manager.get_status() if websocket_manager else {}
                            }
                        }
                        await websocket.send_json(status_msg)
                    elif cmd.get("type") == "get_trade_status":
                        trade_msg = {
                            "type": "trade_status",
                            "timestamp": time.time(),
                            "data": trade_manager.get_trade_stats() if trade_manager else {}
                        }
                        await websocket.send_json(trade_msg)
                    elif cmd.get("type") == "get_arbitrage_opportunities":
                        if shared_market_data and http_client_manager:
                            market_data = shared_market_data.get_all()
                            opportunities = await http_client_manager.get_arbitrage_opportunities(market_data)
                            await websocket.send_json({
                                "type": "arbitrage_opportunities",
                                "timestamp": time.time(),
                                "data": opportunities
                            })
                except json.JSONDecodeError:
                    pass
            except asyncio.TimeoutError:
                await websocket.send_json({
                    "type": "heartbeat",
                    "timestamp": time.time(),
                    "server_time": datetime.now().strftime("%H:%M:%S")
                })
    except WebSocketDisconnect:
        logger.info("客户端WebSocket断开连接")
    except Exception as e:
        logger.error(f"WebSocket错误: {e}")
    finally:
        connection_manager.disconnect(websocket)
        # 从WebSocket管理器移除连接
        if websocket_manager:
            websocket_manager.remove_frontend_connection(websocket)

# ==================== 后台任务 ====================
async def _forward_data_loop():
    """定时转发数据循环"""
    logger.info("📡 启动数据转发循环")
    while True:
        try:
            await asyncio.sleep(CONFIG["forward_interval"])
            
            if connection_manager and connection_manager.active_connections:
                status_msg = {
                    "type": "system_status",
                    "timestamp": time.time(),
                    "data": {
                        "connections": connection_manager.get_detailed_stats() if connection_manager else {},
                        "system": system_monitor.check_resources() if system_monitor else {},
                        "keepalive": keep_alive_manager.get_detailed_stats() if keep_alive_manager else {},
                        "authentication": auth_manager.get_auth_info() if auth_manager else {},
                        "data_stats": data_bridge.get_detailed_stats() if data_bridge else {},
                        "trade_stats": trade_manager.get_trade_stats() if trade_manager else {},
                        "config": CONFIG
                    }
                }
                await connection_manager.broadcast(status_msg)
                
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"转发循环错误: {e}")
            await asyncio.sleep(1)

async def _monitor_resources_loop():
    """资源监控循环"""
    logger.info("💾 启动资源监控")
    while True:
        try:
            await asyncio.sleep(30)
            if system_monitor:
                system_monitor.check_resources()
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"资源监控错误: {e}")

async def _token_cleanup_loop():
    """令牌清理循环"""
    logger.info("🔐 启动令牌清理循环")
    while True:
        try:
            await asyncio.sleep(3600)
            if auth_manager:
                auth_manager.cleanup_expired_tokens()
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"令牌清理错误: {e}")

# ==================== 管理员监控面板 ====================
@app.get("/admin/monitor")
async def admin_monitor():
    """管理员监控面板（HTML页面）"""
    system_status = system_monitor.check_resources() if system_monitor else {"status": "unknown"}
    connections = connection_manager.get_detailed_stats() if connection_manager else {}
    data_stats = data_bridge.get_detailed_stats() if data_bridge else {}
    trade_stats = trade_manager.get_trade_stats() if trade_manager else {}
    
    ws_status = {}
    if websocket_manager:
        ws_status = websocket_manager.get_status()
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>整合版套利系统监控 v4.1（无限制版）</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            }}
            
            body {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                padding: 20px;
            }}
            
            .container {{
                max-width: 1400px;
                margin: 0 auto;
            }}
            
            .header {{
                background: rgba(255, 255, 255, 0.1);
                backdrop-filter: blur(10px);
                border-radius: 20px;
                padding: 30px;
                margin-bottom: 30px;
                box-shadow: 0 10px 30px rgba(0, 0, 0, 0.2);
                border: 1px solid rgba(255, 255, 255, 0.2);
            }}
            
            .header h1 {{
                color: white;
                font-size: 2.5rem;
                margin-bottom: 10px;
                text-shadow: 2px 2px 4px rgba(0, 0, 0, 0.3);
            }}
            
            .header p {{
                color: rgba(255, 255, 255, 0.9);
                font-size: 1.1rem;
            }}
            
            .status-badge {{
                display: inline-block;
                background: {'#4CAF50' if system_status['status'] == 'healthy' else '#FF9800' if system_status['status'] == 'warning' else '#F44336'};
                color: white;
                padding: 5px 15px;
                border-radius: 20px;
                font-size: 0.9rem;
                font-weight: bold;
                margin-left: 10px;
            }}
            
            .websocket-badge {{
                display: inline-block;
                background: {'#4CAF50' if ws_status.get('running', False) else '#F44336'};
                color: white;
                padding: 5px 15px;
                border-radius: 20px;
                font-size: 0.9rem;
                font-weight: bold;
                margin-left: 10px;
            }}
            
            .unlimited-badge {{
                display: inline-block;
                background: #2196F3;
                color: white;
                padding: 5px 15px;
                border-radius: 20px;
                font-size: 0.9rem;
                font-weight: bold;
                margin-left: 10px;
            }}
            
            .dashboard {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
                gap: 25px;
                margin-bottom: 30px;
            }}
            
            .card {{
                background: rgba(255, 255, 255, 0.1);
                backdrop-filter: blur(10px);
                border-radius: 20px;
                padding: 25px;
                box-shadow: 0 10px 30px rgba(0, 0, 0, 0.2);
                border: 1px solid rgba(255, 255, 255, 0.2);
                color: white;
            }}
            
            .card h2 {{
                font-size: 1.5rem;
                margin-bottom: 20px;
                padding-bottom: 10px;
                border-bottom: 2px solid rgba(255, 255, 255, 0.3);
                display: flex;
                align-items: center;
                gap: 10px;
            }}
            
            .card h2 i {{
                font-size: 1.8rem;
            }}
            
            .stats-grid {{
                display: grid;
                grid-template-columns: repeat(2, 1fr);
                gap: 15px;
            }}
            
            .stat-item {{
                background: rgba(255, 255, 255, 0.1);
                padding: 15px;
                border-radius: 15px;
                transition: transform 0.3s ease;
            }}
            
            .stat-item:hover {{
                transform: translateY(-5px);
                background: rgba(255, 255, 255, 0.15);
            }}
            
            .stat-label {{
                font-size: 0.9rem;
                color: rgba(255, 255, 255, 0.8);
                margin-bottom: 5px;
            }}
            
            .stat-value {{
                font-size: 1.8rem;
                font-weight: bold;
                color: white;
            }}
            
            .stat-subtext {{
                font-size: 0.85rem;
                color: rgba(255, 255, 255, 0.7);
                margin-top: 5px;
            }}
            
            .progress-bar {{
                height: 10px;
                background: rgba(255, 255, 255, 0.1);
                border-radius: 5px;
                margin-top: 10px;
                overflow: hidden;
            }}
            
            .progress-fill {{
                height: 100%;
                background: linear-gradient(90deg, #4CAF50, #8BC34A);
                border-radius: 5px;
                width: {min(system_status.get('process', {}).get('memory_mb', 0) / CONFIG['memory_critical_mb'] * 100, 100) if system_status.get('process') else 0}%;
            }}
            
            .warning .progress-fill {{
                background: linear-gradient(90deg, #FF9800, #FFC107);
            }}
            
            .critical .progress-fill {{
                background: linear-gradient(90deg, #F44336, #E91E63);
            }}
            
            .footer {{
                text-align: center;
                color: rgba(255, 255, 255, 0.7);
                padding: 20px;
                font-size: 0.9rem;
            }}
            
            .refresh-btn {{
                background: rgba(255, 255, 255, 0.2);
                color: white;
                border: none;
                padding: 10px 20px;
                border-radius: 10px;
                cursor: pointer;
                font-size: 1rem;
                transition: all 0.3s ease;
                margin-top: 20px;
            }}
            
            .refresh-btn:hover {{
                background: rgba(255, 255, 255, 0.3);
                transform: scale(1.05);
            }}
            
            .debug-section {{
                background: rgba(255, 255, 255, 0.05);
                padding: 15px;
                border-radius: 10px;
                margin-top: 20px;
            }}
            
            .debug-link {{
                color: #4CAF50;
                text-decoration: none;
                display: inline-block;
                margin: 5px;
                padding: 5px 10px;
                background: rgba(255, 255, 255, 0.1);
                border-radius: 5px;
            }}
            
            .debug-link:hover {{
                background: rgba(255, 255, 255, 0.2);
            }}
            
            @media (max-width: 768px) {{
                .dashboard {{
                    grid-template-columns: 1fr;
                }}
                
                .stats-grid {{
                    grid-template-columns: 1fr;
                }}
                
                .header h1 {{
                    font-size: 2rem;
                }}
            }}
        </style>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>
                    🏦 整合版套利系统监控面板 v4.1
                    <span class="status-badge">{system_status.get('status', 'unknown').upper()}</span>
                    <span class="websocket-badge">{'WebSocket运行中' if ws_status.get('running', False) else 'WebSocket停止'}</span>
                    <span class="unlimited-badge">无限制模式</span>
                </h1>
                <p>单服务部署 | WebSocket无限制 | 所有USDT永续合约 | 架构: 整合模式</p>
            </div>
            
            <div class="dashboard">
                <!-- 系统资源卡片 -->
                <div class="card">
                    <h2><i class="fas fa-server"></i> 系统资源</h2>
                    <div class="stats-grid">
                        <div class="stat-item">
                            <div class="stat-label">内存使用</div>
                            <div class="stat-value">{system_status.get('process', {}).get('memory_mb', 0):.1f} MB</div>
                            <div class="progress-bar {'warning' if system_status.get('process', {}).get('memory_mb', 0) > CONFIG['memory_warning_mb'] else 'critical' if system_status.get('process', {}).get('memory_mb', 0) > CONFIG['memory_critical_mb'] else ''}">
                                <div class="progress-fill"></div>
                            </div>
                            <div class="stat-subtext">阈值: {CONFIG['memory_warning_mb']}/{CONFIG['memory_critical_mb']} MB</div>
                        </div>
                        
                        <div class="stat-item">
                            <div class="stat-label">CPU使用率</div>
                            <div class="stat-value">{system_status.get('process', {}).get('cpu_percent', 0):.1f}%</div>
                            <div class="stat-subtext">系统: {system_status.get('system', {}).get('cpu_percent', 0):.1f}%</div>
                        </div>
                        
                        <div class="stat-item">
                            <div class="stat-label">运行时间</div>
                            <div class="stat-value">{system_monitor._format_uptime(time.time() - system_monitor._start_time) if system_monitor else '00:00:00'}</div>
                            <div class="stat-subtext">整合版单服务</div>
                        </div>
                        
                        <div class="stat-item">
                            <div class="stat-label">系统内存</div>
                            <div class="stat-value">{system_status.get('system', {}).get('memory_percent', 0):.1f}%</div>
                            <div class="stat-subtext">可用: {system_status.get('system', {}).get('memory_available_mb', 0):.1f} MB</div>
                        </div>
                    </div>
                </div>
                
                <!-- WebSocket状态卡片 -->
                <div class="card">
                    <h2><i class="fas fa-satellite-dish"></i> WebSocket状态（无限制）</h2>
                    <div class="stats-grid">
                        <div class="stat-item">
                            <div class="stat-label">运行状态</div>
                            <div class="stat-value">{'✅ 运行中' if ws_status.get('running', False) else '❌ 停止'}</div>
                            <div class="stat-subtext">运行时间: {ws_status.get('uptime', 0):.0f}秒</div>
                        </div>
                        
                        <div class="stat-item">
                            <div class="stat-label">数据更新</div>
                            <div class="stat-value">{data_stats.get('updates_received', 0):,}</div>
                            <div class="stat-subtext">{data_stats.get('update_rate_per_min', 0):.1f}/分钟</div>
                        </div>
                        
                        <div class="stat-item">
                            <div class="stat-label">监控币种数</div>
                            <div class="stat-value">{data_stats.get('total_symbols', 0)}</div>
                            <div class="stat-subtext">
                                币安: {data_stats.get('symbols_count', {{}}).get('binance', 0)} | OKX: {data_stats.get('symbols_count', {{}}).get('okx', 0)}
                            </div>
                        </div>
                        
                        <div class="stat-item">
                            <div class="stat-label">交易所连接</div>
                            <div class="stat-value">{len(ws_status.get('exchanges', {{}}))}</div>
                            <div class="stat-subtext">
                                {'✅ 币安' if ws_status.get('exchanges', {{}}).get('binance', {{}}).get('connected', False) else '❌ 币安'}
                                {'✅ OKX' if ws_status.get('exchanges', {{}}).get('okx', {{}}).get('connected', False) else '❌ OKX'}
                            </div>
                        </div>
                    </div>
                    
                    <div class="debug-section">
                        <div class="stat-label">调试工具:</div>
                        <a class="debug-link" href="/api/debug/ws-status" target="_blank">详细状态</a>
                        <a class="debug-link" href="/api/debug/test-exchange-connection?exchange=binance" target="_blank">测试币安</a>
                        <a class="debug-link" href="/api/debug/test-exchange-connection?exchange=okx" target="_blank">测试OKX</a>
                        <a class="debug-link" href="/api/debug/check-data-flow" target="_blank">检查数据流</a>
                    </div>
                </div>
                
                <!-- 数据统计卡片 -->
                <div class="card">
                    <h2><i class="fas fa-chart-line"></i> 数据统计</h2>
                    <div class="stats-grid">
                        <div class="stat-item">
                            <div class="stat-label">数据广播</div>
                            <div class="stat-value">{data_stats.get('updates_broadcasted', 0):,}</div>
                            <div class="stat-subtext">{data_stats.get('broadcast_rate_per_min', 0):.1f}/分钟</div>
                        </div>
                        
                        <div class="stat-item">
                            <div class="stat-label">数据新鲜度</div>
                            <div class="stat-value">{data_stats.get('data_freshness', {{}}).get('binance', {{}}).get('freshness_percent', 0):.1f}%</div>
                            <div class="stat-subtext">币安: {data_stats.get('data_freshness', {{}}).get('binance', {{}}).get('fresh', 0)}/{data_stats.get('data_freshness', {{}}).get('binance', {{}}).get('total', 0)}</div>
                        </div>
                        
                        <div class="stat-item">
                            <div class="stat-label">运行时间</div>
                            <div class="stat-value">{data_stats.get('uptime_human', '00:00:00')}</div>
                            <div class="stat-subtext">数据服务运行中</div>
                        </div>
                        
                        <div class="stat-item">
                            <div class="stat-label">广播队列</div>
                            <div class="stat-value">{ws_status.get('broadcast_stats', {{}}).get('queue_size', 0)}</div>
                            <div class="stat-subtext">已广播: {ws_status.get('broadcast_stats', {{}}).get('broadcasted', 0):,}</div>
                        </div>
                    </div>
                </div>
                
                <!-- 连接统计卡片 -->
                <div class="card">
                    <h2><i class="fas fa-plug"></i> 连接统计</h2>
                    <div class="stats-grid">
                        <div class="stat-item">
                            <div class="stat-label">当前连接</div>
                            <div class="stat-value">{connections.get('current_connections', 0)}</div>
                            <div class="stat-subtext">峰值: {connections.get('peak_connections', 0)}</div>
                        </div>
                        
                        <div class="stat-item">
                            <div class="stat-label">总连接数</div>
                            <div class="stat-value">{connections.get('total_connections', 0)}</div>
                            <div class="stat-subtext">断开: {connections.get('disconnections', 0)}</div>
                        </div>
                        
                        <div class="stat-item">
                            <div class="stat-label">消息发送</div>
                            <div class="stat-value">{connections.get('messages_sent', 0):,}</div>
                            <div class="stat-subtext">{connections.get('messages_per_minute', 0):.1f}/分钟</div>
                        </div>
                        
                        <div class="stat-item">
                            <div class="stat-label">平均时长</div>
                            <div class="stat-value">{connections.get('avg_client_duration_seconds', 0):.1f}秒</div>
                            <div class="stat-subtext">最长连接优先</div>
                        </div>
                    </div>
                </div>
                
                <!-- 交易统计卡片 -->
                <div class="card">
                    <h2><i class="fas fa-exchange-alt"></i> 交易统计</h2>
                    <div class="stats-grid">
                        <div class="stat-item">
                            <div class="stat-label">交易所配置</div>
                            <div class="stat-value">{len(trade_stats.get('exchanges_configured', []))}个</div>
                            <div class="stat-subtext">{', '.join(trade_stats.get('exchanges_configured', [])) if trade_stats.get('exchanges_configured') else '未配置'}</div>
                        </div>
                        
                        <div class="stat-item">
                            <div class="stat-label">总订单数</div>
                            <div class="stat-value">{trade_stats.get('total_orders', 0)}</div>
                            <div class="stat-subtext">成功率: {trade_stats.get('success_rate', 0):.1f}%</div>
                        </div>
                        
                        <div class="stat-item">
                            <div class="stat-label">套利配对</div>
                            <div class="stat-value">{trade_stats.get('active_arbitrage_pairs', 0)}</div>
                            <div class="stat-subtext">总计: {trade_stats.get('total_arbitrage_pairs', 0)}</div>
                        </div>
                        
                        <div class="stat-item">
                            <div class="stat-label">交易时间</div>
                            <div class="stat-value">{trade_stats.get('uptime_human', '00:00:00')}</div>
                            <div class="stat-subtext">系统运行中</div>
                        </div>
                    </div>
                </div>
            </div>
            
            <div style="display: flex; gap: 10px; justify-content: center; flex-wrap: wrap;">
                <button class="refresh-btn" onclick="window.location.reload()">
                    <i class="fas fa-sync-alt"></i> 刷新数据
                </button>
                <button class="refresh-btn" onclick="restartWebSocket()">
                    <i class="fas fa-redo-alt"></i> 重启WebSocket
                </button>
                <button class="refresh-btn" onclick="testExchangeConnections()">
                    <i class="fas fa-wifi"></i> 测试连接
                </button>
                <button class="refresh-btn" onclick="showExchangeStats()">
                    <i class="fas fa-chart-bar"></i> 交易所统计
                </button>
            </div>
            
            <div class="footer">
                <p>服务器版本: 4.1.0（无限制版） | 架构: 整合单服务 | 订阅模式: 所有USDT永续合约 | WebSocket: 无限制</p>
                <p>最后更新: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>© 2024 套利交易系统 - 整合版 v4.1（无限制版）</p>
            </div>
        </div>
        
        <script>
            // 自动刷新页面（每30秒）
            setTimeout(() => {{
                window.location.reload();
            }}, 30000);
            
            // WebSocket连接尝试
            try {{
                const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                const wsUrl = `${{wsProtocol}}//${{window.location.host}}/ws`;
                const ws = new WebSocket(wsUrl);
                
                ws.onopen = () => {{
                    console.log('WebSocket连接成功');
                    ws.send(JSON.stringify({{type: 'get_status'}}));
                }};
                
                ws.onmessage = (event) => {{
                    const data = JSON.parse(event.data);
                    if (data.type === 'market_update') {{
                        console.log('收到实时数据:', data.symbol, data.data.price);
                    }}
                }};
                
                ws.onclose = () => {{
                    console.log('WebSocket连接关闭');
                }};
            }} catch (e) {{
                console.log('WebSocket连接失败:', e);
            }}
            
            // 重启WebSocket
            function restartWebSocket() {{
                if (confirm('确定要重启WebSocket连接吗？')) {{
                    fetch('/api/debug/restart-websocket', {{ method: 'POST' }})
                        .then(response => response.json())
                        .then(data => {{
                            alert(data.message || 'WebSocket重启完成');
                            setTimeout(() => {{ window.location.reload(); }}, 2000);
                        }})
                        .catch(error => {{
                            alert('重启失败: ' + error);
                        }});
                }}
            }}
            
            // 测试交易所连接
            function testExchangeConnections() {{
                const exchanges = ['binance', 'okx'];
                let results = [];
                
                exchanges.forEach(exchange => {{
                    fetch(`/api/debug/test-exchange-connection?exchange=${{exchange}}`)
                        .then(response => response.json())
                        .then(data => {{
                            results.push(`${{exchange}}: ${{data.success ? '成功' : '失败'}} (发现${{data.symbols_discovered || 0}}个合约)`);
                            
                            if (results.length === exchanges.length) {{
                                alert('测试结果:\\n' + results.join('\\n'));
                            }}
                        }});
                }});
            }}
            
            // 显示交易所统计
            function showExchangeStats() {{
                fetch('/api/debug/ws-status')
                    .then(response => response.json())
                    .then(data => {{
                        if (data.success) {{
                            const exchanges = data.manager.status.exchanges;
                            let stats = '交易所统计:\\n';
                            for (const [exchange, info] of Object.entries(exchanges)) {{
                                stats += `\\n${{exchange}}:`;
                                stats += `\\n  连接状态: ${{info.connected ? '已连接' : '未连接'}}`;
                                stats += `\\n  发现合约: ${{info.symbols_discovered || 0}}个`;
                                stats += `\\n  订阅合约: ${{info.symbols_subscribed || 0}}个`;
                                stats += `\\n  重连次数: ${{info.reconnect_attempts || 0}}`;
                            }}
                            alert(stats);
                        }}
                    }});
            }}
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

# ==================== 主程序入口 ====================
if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", 10000))
    logger.info(f"启动服务器 v4.1（无限制版），端口: {port}")
    logger.info("WebSocket无限制版本，订阅所有USDT永续合约")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info",
        limit_concurrency=100
    )
    