"""
🏦 套利系统 - WebSocket客户端模块 v4.0
功能：订阅所有USDT永续合约，完全无限制，实时数据流过不保存
"""

import asyncio
import json
import time
import logging
import hmac
import hashlib
import os
import threading
from typing import Dict, List, Optional, Any, Set, Callable
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import aiohttp
from enum import Enum
import ccxt.async_support as ccxt_async

# ==================== 配置区 ====================
CONFIG = {
    "exchanges": {
        "binance": {
            "ws_public_url": "wss://stream.binance.com:9443/ws",
            "rest_url": "https://api.binance.com",
        },
        "okx": {
            "ws_public_url": "wss://ws.okx.com:8443/ws/v5/public",
            "rest_url": "https://www.okx.com",
        }
    },
    "subscription": {
        "reconnect_delay": 5,
        "max_reconnect_attempts": 10,
        "batch_subscribe_size": 50,  # 批量订阅大小
        "subscribe_delay": 0.5,      # 订阅间隔
    },
    "data_handling": {
        "broadcast_queue_size": 10000, # 广播队列大小（提高）
        "cleanup_old_threshold": 3600, # 1小时前的数据才清理
    }
}

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== 数据模型 ====================
@dataclass
class MarketData:
    """统一的市场数据结构"""
    exchange: str
    symbol: str
    data_type: str
    timestamp: float
    data: Dict[str, Any]
    
    def to_dict(self):
        return asdict(self)

@dataclass
class FundingInfo:
    """资金费率信息"""
    symbol: str
    funding_rate: float
    next_funding_time: int
    countdown_seconds: int
    timestamp: float = 0.0
    
    def __post_init__(self):
        if self.timestamp == 0:
            self.timestamp = time.time()
        if self.next_funding_time:
            current_ms = int(time.time() * 1000)
            self.countdown_seconds = max(0, (self.next_funding_time - current_ms) // 1000)

# ==================== 无限制共享数据 ====================
import threading

class UnlimitedSharedData:
    """无限制共享数据 - 不限制数量，只保存当前数据"""
    
    def __init__(self):
        self._data = {
            "binance": {},
            "okx": {}
        }
        self._lock = threading.RLock()
        self._stats = {
            "updates_received": 0,
            "updates_broadcasted": 0,
            "last_update": 0,
            "start_time": time.time(),
            "symbols_count": {
                "binance": 0,
                "okx": 0
            },
            "memory_warnings": 0
        }
        self.broadcast_callback = None
        self._last_cleanup = time.time()
        
    def set_broadcast_callback(self, callback: Callable):
        """设置广播回调函数"""
        self.broadcast_callback = callback
    
    def update(self, exchange: str, data_type: str, data: Dict):
        """更新市场数据并立即广播"""
        with self._lock:
            symbol = str(data.get("symbol", "")).upper().strip()
            if not symbol or exchange not in self._data:
                return
            
            timestamp = time.time()
            
            # 检查是否需要清理（每10分钟一次）
            if timestamp - self._last_cleanup > 600:  # 10分钟
                self._cleanup_old_data(timestamp)
                self._last_cleanup = timestamp
            
            # 创建或更新记录（完全不限制数量）
            if symbol not in self._data[exchange]:
                self._data[exchange][symbol] = {
                    "_ts": timestamp,
                    "_exchange": exchange,
                    "_symbol": symbol,
                    "_created": timestamp
                }
                self._stats["symbols_count"][exchange] = len(self._data[exchange])
                
                # 记录内存使用情况（每100个新币种记录一次）
                total_symbols = sum(self._stats["symbols_count"].values())
                if total_symbols % 100 == 0:
                    logger.info(f"当前监控币种数: {total_symbols} (币安: {self._stats['symbols_count']['binance']}, OKX: {self._stats['symbols_count']['okx']})")
            
            record = self._data[exchange][symbol]
            
            # 更新数据
            if data_type == "price":
                record.update({
                    "price": float(data.get("price", 0)),
                    "daily_change": data.get("daily_change", 0),
                    "price_ts": timestamp,
                })
            elif data_type == "funding":
                record.update({
                    "funding_rate": float(data.get("funding_rate", 0)),
                    "next_funding": data.get("next_funding_time", 0),
                    "countdown": data.get("countdown", 0),
                    "funding_ts": timestamp,
                })
            
            # 更新最后时间戳
            record["_ts"] = timestamp
            
            # 更新统计
            self._stats["updates_received"] += 1
            self._stats["last_update"] = timestamp
            
            # 立即广播
            if self.broadcast_callback:
                try:
                    broadcast_data = {
                        "exchange": exchange,
                        "symbol": symbol,
                        "data_type": data_type,
                        "data": {
                            k: v for k, v in record.items() 
                            if not k.startswith('_')
                        },
                        "timestamp": timestamp
                    }
                    self.broadcast_callback(broadcast_data)
                    self._stats["updates_broadcasted"] += 1
                except Exception as e:
                    logger.debug(f"广播数据失败: {e}")
    
    def _cleanup_old_data(self, current_time: float):
        """清理过时数据（只清理超过1小时未更新的数据）"""
        cleanup_threshold = CONFIG["data_handling"]["cleanup_old_threshold"]
        
        for exchange in ["binance", "okx"]:
            if exchange not in self._data:
                continue
            
            symbols_to_remove = []
            for symbol, record in self._data[exchange].items():
                last_update = record.get("_ts", 0)
                # 只清理超过1小时未更新的数据
                if current_time - last_update > cleanup_threshold:
                    symbols_to_remove.append(symbol)
            
            if symbols_to_remove:
                for symbol in symbols_to_remove:
                    del self._data[exchange][symbol]
                
                self._stats["symbols_count"][exchange] = len(self._data[exchange])
                logger.debug(f"清理 {exchange} 的 {len(symbols_to_remove)} 个过时币种")
    
    def get_current_snapshot(self, exchange: Optional[str] = None):
        """获取当前数据快照（用于前端请求）"""
        with self._lock:
            import copy
            if exchange:
                if exchange in self._data:
                    return copy.deepcopy(self._data[exchange])
                return {}
            return copy.deepcopy(self._data)
    
    def get_stats(self):
        """获取统计信息"""
        with self._lock:
            uptime = time.time() - self._stats["start_time"]
            total_symbols = sum(self._stats["symbols_count"].values())
            
            # 计算更新速率
            update_rate = 0
            if uptime > 0:
                update_rate = self._stats["updates_received"] / (uptime / 60)
            
            broadcast_rate = 0
            if uptime > 0:
                broadcast_rate = self._stats["updates_broadcasted"] / (uptime / 60)
            
            return {
                **self._stats,
                "total_symbols": total_symbols,
                "uptime_seconds": uptime,
                "update_rate_per_min": round(update_rate, 1),
                "broadcast_rate_per_min": round(broadcast_rate, 1),
            }

# ==================== 交易所WebSocket客户端基类 ====================
class ExchangeWebSocketClient:
    """交易所WebSocket客户端基类"""
    
    def __init__(self, exchange: str, shared_data: UnlimitedSharedData):
        self.exchange_name = exchange
        self.shared_data = shared_data
        self.api_key = ""
        self.api_secret = ""
        
        # 连接状态
        self.ws = None
        self.session = None
        self.is_connected = False
        
        # 重连管理
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = CONFIG["subscription"]["max_reconnect_attempts"]
        
        # 数据存储
        self.daily_open_prices = {}  # 日开盘价缓存（每日更新）
        self.usdt_perpetual_symbols = []  # USDT永续合约列表
        
        # CCXT实例
        self.ccxt_client = None
        self._init_ccxt_client()
        
        # 统计
        self.stats = {
            "messages_received": 0,
            "symbols_discovered": 0,
            "symbols_subscribed": 0,
            "last_message": 0,
            "subscription_errors": 0
        }
    
    def _init_ccxt_client(self):
        """初始化CCXT客户端"""
        try:
            exchange_class = getattr(ccxt_async, self.exchange_name)
            config = {
                'apiKey': self.api_key,
                'secret': self.api_secret,
                'enableRateLimit': True,
                'options': {'defaultType': 'swap'}
            }
            self.ccxt_client = exchange_class(config)
        except Exception as e:
            logger.error(f"初始化CCXT客户端失败: {e}")
    
    async def initialize(self):
        """初始化客户端"""
        logger.info(f"初始化 {self.exchange_name} 客户端")
        
        # 获取所有USDT永续合约列表
        await self._fetch_all_usdt_perpetual_symbols()
        
        logger.info(f"{self.exchange_name}: 准备订阅 {len(self.usdt_perpetual_symbols)} 个USDT永续合约")
    
    async def _fetch_all_usdt_perpetual_symbols(self):
        """获取所有USDT永续合约（无限制）"""
        try:
            if not self.ccxt_client:
                logger.error(f"{self.exchange_name} CCXT客户端未初始化")
                return
            
            markets = await self.ccxt_client.load_markets()
            
            all_usdt_symbols = []
            total_markets = len(markets)
            logger.info(f"{self.exchange_name}: 分析 {total_markets} 个市场...")
            
            for symbol, market in markets.items():
                # 条件：必须是永续合约 + 活跃
                is_perpetual = market.get('swap', False) or market.get('linear', False)
                is_active = market.get('active', False)
                
                if not (is_perpetual and is_active):
                    continue
                
                # 检查是否USDT计价
                symbol_upper = symbol.upper()
                is_usdt = False
                
                if self.exchange_name == "binance":
                    # 币安：BTC/USDT, BTCUSDT
                    is_usdt = '/USDT' in symbol_upper
                elif self.exchange_name == "okx":
                    # OKX：BTC-USDT-SWAP
                    is_usdt = '-USDT-SWAP' in symbol_upper
                
                if not is_usdt:
                    continue
                
                # 清理符号格式
                if self.exchange_name == "binance":
                    clean_symbol = symbol.replace('/', '')
                elif self.exchange_name == "okx":
                    clean_symbol = symbol.replace('-USDT-SWAP', 'USDT')
                
                # 确保以USDT结尾
                if not clean_symbol.endswith('USDT'):
                    clean_symbol = f"{clean_symbol}USDT"
                
                all_usdt_symbols.append(clean_symbol)
            
            # 去重并排序
            self.usdt_perpetual_symbols = sorted(list(set(all_usdt_symbols)))
            self.stats["symbols_discovered"] = len(self.usdt_perpetual_symbols)
            
            logger.info(f"✅ {self.exchange_name}: 发现 {len(self.usdt_perpetual_symbols)} 个USDT永续合约")
            
            # 显示统计信息
            if self.usdt_perpetual_symbols:
                # 按字母分组显示
                symbol_groups = {}
                for s in self.usdt_perpetual_symbols:
                    prefix = s[:3]  # 前3个字符作为分组
                    symbol_groups.setdefault(prefix, 0)
                    symbol_groups[prefix] += 1
                
                # 显示最多的10个分组
                top_groups = sorted(symbol_groups.items(), key=lambda x: x[1], reverse=True)[:10]
                group_info = ", ".join([f"{g[0]}:{g[1]}" for g in top_groups])
                logger.info(f"{self.exchange_name} 币种分组统计: {group_info}")
                
                # 显示前5个和最后5个
                sample = self.usdt_perpetual_symbols[:5] + ["..."] + self.usdt_perpetual_symbols[-5:]
                logger.info(f"{self.exchange_name} 合约示例: {sample}")
                
        except Exception as e:
            logger.error(f"获取 {self.exchange_name} USDT永续合约失败: {e}")
            self.usdt_perpetual_symbols = []
    
    def _format_symbol_for_ccxt(self, symbol: str) -> str:
        """格式化交易对符号"""
        if self.exchange_name == "binance":
            return symbol.replace('USDT', '/USDT')
        elif self.exchange_name == "okx":
            return symbol.replace('USDT', '-USDT-SWAP')
        return symbol
    
    def _calculate_daily_change(self, symbol: str, current_price: float) -> float:
        """计算今日涨跌幅"""
        if symbol in self.daily_open_prices:
            open_price = self.daily_open_prices[symbol]
            if open_price > 0:
                return ((current_price - open_price) / open_price) * 100
        return 0.0
    
    async def connect(self):
        """连接WebSocket"""
        raise NotImplementedError
    
    async def subscribe(self):
        """订阅市场数据"""
        raise NotImplementedError
    
    async def disconnect(self):
        """断开连接"""
        if self.ws:
            try:
                await self.ws.close()
            except:
                pass
        if self.session:
            try:
                await self.session.close()
            except:
                pass
        self.is_connected = False
        self.ws = None
        self.session = None
    
    async def run(self):
        """主运行循环"""
        logger.info(f"启动 {self.exchange_name} WebSocket客户端")
        
        while True:
            try:
                # 连接
                connected = await self.connect()
                if not connected:
                    raise Exception("连接失败")
                
                # 订阅
                subscribed = await self.subscribe()
                if not subscribed:
                    raise Exception("订阅失败")
                
                # 重置重连计数
                self.reconnect_attempts = 0
                
                # 接收消息
                await self._receive_messages()
                
            except asyncio.CancelledError:
                logger.info(f"{self.exchange_name} WebSocket被取消")
                break
            except Exception as e:
                logger.error(f"{self.exchange_name} WebSocket错误: {e}")
                
                # 断开连接
                await self.disconnect()
                
                # 重连逻辑
                self.reconnect_attempts += 1
                if self.reconnect_attempts > self.max_reconnect_attempts:
                    logger.error(f"{self.exchange_name} 达到最大重连次数 {self.max_reconnect_attempts}，停止重连")
                    break
                
                delay = min(CONFIG["subscription"]["reconnect_delay"] * (2 ** (self.reconnect_attempts - 1)), 60)
                logger.warning(f"{self.exchange_name} {delay}秒后重连 (尝试 {self.reconnect_attempts}/{self.max_reconnect_attempts})...")
                await asyncio.sleep(delay)
        
        logger.info(f"{self.exchange_name} WebSocket客户端停止")

# ==================== 币安客户端 ====================
class BinanceWebSocketClient(ExchangeWebSocketClient):
    """币安交易所WebSocket客户端"""
    
    def __init__(self, shared_data: UnlimitedSharedData):
        super().__init__("binance", shared_data)
        self.ws_url = CONFIG["exchanges"]["binance"]["ws_public_url"]
    
    async def connect(self):
        """连接币安WebSocket"""
        try:
            self.session = aiohttp.ClientSession()
            self.ws = await self.session.ws_connect(
                self.ws_url,
                heartbeat=30,
                timeout=10,
                autoping=True
            )
            self.is_connected = True
            logger.info("币安WebSocket连接成功")
            return True
        except Exception as e:
            logger.error(f"连接币安WebSocket失败: {e}")
            return False
    
    async def subscribe(self):
        """订阅币安所有USDT永续合约数据"""
        if not self.is_connected:
            return False
        
        try:
            # 确保有合约列表
            if not self.usdt_perpetual_symbols:
                await self._fetch_all_usdt_perpetual_symbols()
            
            if not self.usdt_perpetual_symbols:
                logger.error("币安: 没有获取到USDT永续合约列表")
                return False
            
            logger.info(f"币安: 开始订阅 {len(self.usdt_perpetual_symbols)} 个USDT永续合约")
            
            # 准备订阅流 - 只订阅必要的
            streams = []
            
            for symbol in self.usdt_perpetual_symbols:
                # 转换为小写，币安要求小写
                symbol_lower = symbol.lower()
                
                # 每个合约订阅两个流（最小化）
                streams.append(f"{symbol_lower}@ticker")      # 实时价格
                streams.append(f"{symbol_lower}@markPrice")   # 资金费率
            
            logger.info(f"币安: 需要订阅 {len(streams)} 个数据流")
            
            # 分批订阅
            batch_size = 100
            total_batches = (len(streams) + batch_size - 1) // batch_size
            successful_batches = 0
            
            for i in range(0, len(streams), batch_size):
                chunk = streams[i:i+batch_size]
                
                try:
                    subscribe_msg = {
                        "method": "SUBSCRIBE",
                        "params": chunk,
                        "id": i // batch_size + 1
                    }
                    
                    await self.ws.send_json(subscribe_msg)
                    
                    batch_num = i // batch_size + 1
                    logger.info(f"币安: 批量订阅 {batch_num}/{total_batches} ({len(chunk)}个流)")
                    
                    successful_batches += 1
                    
                    # 短暂延迟，避免被限流
                    await asyncio.sleep(CONFIG["subscription"]["subscribe_delay"])
                    
                except Exception as e:
                    logger.warning(f"币安批量订阅 {i//batch_size+1} 失败: {e}")
                    self.stats["subscription_errors"] += 1
            
            self.stats["symbols_subscribed"] = len(self.usdt_perpetual_symbols)
            logger.info(f"✅ 币安: 订阅完成，共 {len(self.usdt_perpetual_symbols)} 个合约，{successful_batches}/{total_batches} 批成功")
            return successful_batches > 0
            
        except Exception as e:
            logger.error(f"币安订阅失败: {e}")
            return False
    
    async def _receive_messages(self):
        """接收和处理消息"""
        async for msg in self.ws:
            self.stats["messages_received"] += 1
            self.stats["last_message"] = time.time()
            
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    await self._process_message(data)
                except json.JSONDecodeError as e:
                    logger.debug(f"币安消息JSON解析失败: {e}")
                except Exception as e:
                    logger.debug(f"币安消息处理失败: {e}")
                    
            elif msg.type == aiohttp.WSMsgType.PING:
                await self.ws.pong()
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                logger.warning(f"币安WebSocket连接关闭: {msg.type}")
                break
    
    async def _process_message(self, data: Dict):
        """处理消息"""
        # 心跳响应
        if data.get('result') is None and 'id' in data:
            return
        
        stream_type = data.get('e', '')
        
        if stream_type == '24hrTicker':
            await self._handle_ticker_data(data)
        elif stream_type == 'markPriceUpdate':
            await self._handle_mark_price(data)
    
    async def _handle_ticker_data(self, data: Dict):
        """处理ticker数据"""
        symbol = data.get('s', '').upper()
        if not symbol:
            return
        
        current_price = float(data.get('c', 0))
        if current_price <= 0:
            return
        
        # 计算今日涨跌幅
        daily_change = self._calculate_daily_change(symbol, current_price)
        
        # 更新共享数据
        self.shared_data.update(
            exchange="binance",
            data_type="price",
            data={
                "symbol": symbol,
                "price": current_price,
                "daily_change": round(daily_change, 2),
                "volume_24h": float(data.get('v', 0)),
            }
        )
    
    async def _handle_mark_price(self, data: Dict):
        """处理标记价格和资金费率"""
        symbol = data.get('s', '').upper()
        if not symbol:
            return
        
        funding_rate = float(data.get('r', 0))
        next_funding_time = int(data.get('T', 0))
        
        # 计算倒计时（秒）
        countdown = 0
        if next_funding_time:
            current_ms = int(time.time() * 1000)
            countdown = max(0, (next_funding_time - current_ms) // 1000)
        
        self.shared_data.update(
            exchange="binance",
            data_type="funding",
            data={
                "symbol": symbol,
                "funding_rate": funding_rate,
                "next_funding_time": next_funding_time,
                "countdown": countdown
            }
        )

# ==================== OKX客户端 ====================
class OKXWebSocketClient(ExchangeWebSocketClient):
    """OKX交易所WebSocket客户端"""
    
    def __init__(self, shared_data: UnlimitedSharedData):
        super().__init__("okx", shared_data)
        self.ws_url = CONFIG["exchanges"]["okx"]["ws_public_url"]
    
    async def connect(self):
        """连接OKX WebSocket"""
        try:
            self.session = aiohttp.ClientSession()
            self.ws = await self.session.ws_connect(
                self.ws_url,
                heartbeat=25,
                timeout=10,
                autoping=True
            )
            self.is_connected = True
            logger.info("OKX WebSocket连接成功")
            return True
        except Exception as e:
            logger.error(f"连接OKX WebSocket失败: {e}")
            return False
    
    async def subscribe(self):
        """订阅OKX所有USDT永续合约数据"""
        if not self.is_connected:
            return False
        
        try:
            # 确保有合约列表
            if not self.usdt_perpetual_symbols:
                await self._fetch_all_usdt_perpetual_symbols()
            
            if not self.usdt_perpetual_symbols:
                logger.error("OKX: 没有获取到USDT永续合约列表")
                return False
            
            logger.info(f"OKX: 开始订阅 {len(self.usdt_perpetual_symbols)} 个USDT永续合约")
            
            # 准备订阅参数
            args = []
            
            for symbol in self.usdt_perpetual_symbols:
                # 转换为OKX格式：BTCUSDT -> BTC-USDT-SWAP
                okx_symbol = symbol.replace('USDT', '-USDT-SWAP')
                
                # 每个合约订阅两个频道
                args.append({
                    "channel": "tickers",
                    "instId": okx_symbol
                })
                
                args.append({
                    "channel": "funding-rate",
                    "instId": okx_symbol
                })
            
            logger.info(f"OKX: 需要订阅 {len(args)} 个频道")
            
            # 分批订阅
            batch_size = CONFIG["subscription"]["batch_subscribe_size"] * 2
            total_batches = (len(args) + batch_size - 1) // batch_size
            successful_batches = 0
            
            for i in range(0, len(args), batch_size):
                batch_args = args[i:i+batch_size]
                
                try:
                    subscribe_msg = {
                        "op": "subscribe",
                        "args": batch_args
                    }
                    
                    await self.ws.send_json(subscribe_msg)
                    
                    batch_num = i // batch_size + 1
                    contracts_in_batch = len(batch_args) // 2
                    logger.info(f"OKX: 批量订阅 {batch_num}/{total_batches} ({contracts_in_batch}个合约)")
                    
                    successful_batches += 1
                    
                    # 短暂延迟
                    await asyncio.sleep(CONFIG["subscription"]["subscribe_delay"])
                    
                except Exception as e:
                    logger.warning(f"OKX批量订阅 {i//batch_size+1} 失败: {e}")
                    self.stats["subscription_errors"] += 1
            
            self.stats["symbols_subscribed"] = len(self.usdt_perpetual_symbols)
            logger.info(f"✅ OKX: 订阅完成，共 {len(self.usdt_perpetual_symbols)} 个合约，{successful_batches}/{total_batches} 批成功")
            return successful_batches > 0
            
        except Exception as e:
            logger.error(f"OKX订阅失败: {e}")
            return False
    
    async def _receive_messages(self):
        """接收和处理消息"""
        async for msg in self.ws:
            self.stats["messages_received"] += 1
            self.stats["last_message"] = time.time()
            
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    await self._process_message(data)
                except json.JSONDecodeError as e:
                    logger.debug(f"OKX消息JSON解析失败: {e}")
                except Exception as e:
                    logger.debug(f"OKX消息处理失败: {e}")
                    
            elif msg.type == aiohttp.WSMsgType.PING:
                await self.ws.pong()
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                logger.warning(f"OKX WebSocket连接关闭: {msg.type}")
                break
    
    async def _process_message(self, data: Dict):
        """处理消息"""
        event = data.get('event', '')
        
        if event == 'subscribe':
            logger.debug(f"OKX订阅成功: {data.get('arg', {})}")
            return
        elif event == 'error':
            logger.error(f"OKX订阅错误: {data}")
            return
        
        if 'data' in data:
            arg = data.get('arg', {})
            channel = arg.get('channel', '')
            
            if channel == 'tickers':
                await self._handle_ticker_data(data['data'])
            elif channel == 'funding-rate':
                await self._handle_funding_data(data['data'])
    
    async def _handle_ticker_data(self, data_list: List):
        """处理OKX ticker数据"""
        if not data_list:
            return
        
        for data in data_list:
            inst_id = data.get('instId', '')
            symbol = inst_id.replace('-USDT-SWAP', 'USDT')
            
            last_price = float(data.get('last', 0))
            if last_price <= 0:
                continue
            
            # 计算今日涨跌幅
            daily_change = self._calculate_daily_change(symbol, last_price)
            
            # 更新共享数据
            self.shared_data.update(
                exchange="okx",
                data_type="price",
                data={
                    "symbol": symbol,
                    "price": last_price,
                    "daily_change": round(daily_change, 2),
                    "bid": float(data.get('bidPx', 0)),
                    "ask": float(data.get('askPx', 0)),
                }
            )
    
    async def _handle_funding_data(self, data_list: List):
        """处理OKX资金费率数据"""
        if not data_list:
            return
        
        for data in data_list:
            inst_id = data.get('instId', '')
            symbol = inst_id.replace('-USDT-SWAP', 'USDT')
            
            funding_rate = float(data.get('fundingRate', 0))
            next_funding_time = int(data.get('fundingTime', 0))
            
            # 计算倒计时
            countdown = 0
            if next_funding_time:
                current_ms = int(time.time() * 1000)
                countdown = max(0, (next_funding_time - current_ms) // 1000)
            
            self.shared_data.update(
                exchange="okx",
                data_type="funding",
                data={
                    "symbol": symbol,
                    "funding_rate": funding_rate,
                    "next_funding_time": next_funding_time,
                    "countdown": countdown,
                    "estimated_rate": float(data.get('nextFundingRate', 0))
                }
            )

# ==================== WebSocket管理器 ====================
class WebSocketManager:
    """WebSocket管理器 - 协调所有交易所客户端"""
    
    def __init__(self):
        self.shared_data = UnlimitedSharedData()
        self.clients = {}
        self.running = False
        self.tasks = []
        
        # 广播队列
        self.broadcast_queue = asyncio.Queue(maxsize=CONFIG["data_handling"]["broadcast_queue_size"])
        self.frontend_connections = []
        
        # 统计
        self.stats = {
            "start_time": 0,
            "exchange_status": {},
            "broadcasted_messages": 0,
            "dropped_messages": 0,
            "queue_size_history": [],
            "last_queue_check": 0
        }
        
        # 设置广播回调
        self.shared_data.set_broadcast_callback(self._on_data_update)
        
        logger.info("WebSocket管理器初始化完成（无限制版本）")
    
    def _on_data_update(self, data: Dict):
        """接收到数据更新时的回调"""
        try:
            # 非阻塞方式放入队列
            self.broadcast_queue.put_nowait({
                "type": "market_update",
                "timestamp": time.time(),
                **data
            })
        except asyncio.QueueFull:
            self.stats["dropped_messages"] += 1
            # 每丢弃1000条消息记录一次
            if self.stats["dropped_messages"] % 1000 == 0:
                logger.warning(f"广播队列已满，已丢弃 {self.stats['dropped_messages']} 条消息")
    
    async def initialize(self):
        """初始化所有交易所客户端"""
        logger.info("初始化WebSocket管理器...")
        
        # 初始化币安客户端
        binance_client = BinanceWebSocketClient(self.shared_data)
        self.clients["binance"] = binance_client
        await binance_client.initialize()
        
        # 初始化OKX客户端
        okx_client = OKXWebSocketClient(self.shared_data)
        self.clients["okx"] = okx_client
        await okx_client.initialize()
        
        logger.info("WebSocket管理器初始化完成")
    
    async def start(self):
        """启动所有WebSocket连接"""
        if self.running:
            logger.warning("WebSocket管理器已在运行")
            return
        
        # 确保先初始化
        if not self.clients:
            logger.info("检测到WebSocket管理器未初始化，正在自动初始化...")
            await self.initialize()
        
        self.running = True
        self.stats["start_time"] = time.time()
        
        logger.info("启动WebSocket管理器...")
        
        # 启动广播工作线程
        broadcast_task = asyncio.create_task(self._broadcast_worker())
        self.tasks.append(broadcast_task)
        
        # 为每个交易所启动任务
        for exchange_name, client in self.clients.items():
            task = asyncio.create_task(client.run())
            self.tasks.append(task)
            self.stats["exchange_status"][exchange_name] = {
                "connected": False,
                "last_update": 0,
                "symbols_discovered": client.stats.get("symbols_discovered", 0),
                "symbols_subscribed": client.stats.get("symbols_subscribed", 0)
            }
        
        logger.info(f"WebSocket管理器启动完成，共 {len(self.clients)} 个交易所")
    
    async def stop(self):
        """停止所有WebSocket连接"""
        if not self.running:
            return
        
        self.running = False
        logger.info("停止WebSocket管理器...")
        
        # 停止所有客户端
        for client in self.clients.values():
            await client.disconnect()
        
        # 取消所有任务
        for task in self.tasks:
            task.cancel()
        
        try:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        
        # 清理广播队列
        while not self.broadcast_queue.empty():
            try:
                self.broadcast_queue.get_nowait()
                self.broadcast_queue.task_done()
            except:
                pass
        
        logger.info("WebSocket管理器已停止")
    
    def add_frontend_connection(self, websocket):
        """添加前端WebSocket连接"""
        if websocket not in self.frontend_connections:
            self.frontend_connections.append(websocket)
            logger.debug(f"添加前端连接，当前连接数: {len(self.frontend_connections)}")
    
    def remove_frontend_connection(self, websocket):
        """移除前端WebSocket连接"""
        if websocket in self.frontend_connections:
            self.frontend_connections.remove(websocket)
            logger.debug(f"移除前端连接，剩余连接数: {len(self.frontend_connections)}")
    
    async def _broadcast_worker(self):
        """广播工作线程 - 将市场数据推送给前端"""
        logger.info("启动广播工作线程")
        
        while self.running:
            try:
                # 从队列获取数据（阻塞等待）
                message = await self.broadcast_queue.get()
                
                # 记录队列状态（每分钟一次）
                current_time = time.time()
                if current_time - self.stats["last_queue_check"] > 60:
                    queue_size = self.broadcast_queue.qsize()
                    self.stats["queue_size_history"].append({
                        "timestamp": current_time,
                        "size": queue_size
                    })
                    if len(self.stats["queue_size_history"]) > 1440:  # 保留24小时
                        self.stats["queue_size_history"].pop(0)
                    self.stats["last_queue_check"] = current_time
                    
                    if queue_size > 5000:
                        logger.warning(f"广播队列积压: {queue_size} 条消息")
                
                # 如果没有前端连接，直接丢弃
                if not self.frontend_connections:
                    self.broadcast_queue.task_done()
                    await asyncio.sleep(0.1)
                    continue
                
                # 准备广播消息
                try:
                    broadcast_msg = json.dumps(message)
                except:
                    self.broadcast_queue.task_done()
                    continue
                
                # 广播给所有前端连接
                disconnected = []
                for ws in self.frontend_connections:
                    try:
                        await ws.send_text(broadcast_msg)
                    except Exception as e:
                        logger.debug(f"广播到前端失败: {e}")
                        disconnected.append(ws)
                
                # 清理断开的连接
                for ws in disconnected:
                    self.remove_frontend_connection(ws)
                
                # 更新统计
                self.stats["broadcasted_messages"] += 1
                
                # 标记任务完成
                self.broadcast_queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"广播工作线程错误: {e}")
                await asyncio.sleep(1)
        
        logger.info("广播工作线程停止")
    
    def get_current_data(self):
        """获取当前数据快照（用于HTTP请求）"""
        return self.shared_data.get_current_snapshot()
    
    def get_status(self):
        """获取状态信息"""
        status = {
            "running": self.running,
            "uptime": time.time() - self.stats["start_time"] if self.stats["start_time"] > 0 else 0,
            "data_stats": self.shared_data.get_stats(),
            "broadcast_stats": {
                "queue_size": self.broadcast_queue.qsize(),
                "queue_history": self.stats["queue_size_history"][-10:],  # 最近10个记录
                "broadcasted": self.stats["broadcasted_messages"],
                "dropped": self.stats["dropped_messages"],
                "frontend_connections": len(self.frontend_connections)
            },
            "exchanges": {}
        }
        
        for exchange_name, client in self.clients.items():
            status["exchanges"][exchange_name] = {
                "connected": client.is_connected,
                "reconnect_attempts": client.reconnect_attempts,
                "symbols_discovered": client.stats.get("symbols_discovered", 0),
                "symbols_subscribed": client.stats.get("symbols_subscribed", 0),
                "messages_received": client.stats.get("messages_received", 0),
                "subscription_errors": client.stats.get("subscription_errors", 0),
                "last_message": client.stats.get("last_message", 0)
            }
        
        return status

# ==================== 模块测试代码 ====================
async def _test_module():
    """模块测试函数"""
    print("测试无限制WebSocket客户端模块...")
    
    manager = WebSocketManager()
    await manager.initialize()
    await manager.start()
    
    try:
        # 运行60秒测试
        print("运行60秒测试，观察合约数量...")
        for i in range(60):
            await asyncio.sleep(1)
            status = manager.get_status()
            data_stats = status["data_stats"]
            
            total_symbols = data_stats['total_symbols']
            update_rate = data_stats['update_rate_per_min']
            
            print(f"第{i+1}秒 - 总币种: {total_symbols} | "
                  f"更新速率: {update_rate:.0f}/分钟 | "
                  f"广播队列: {status['broadcast_stats']['queue_size']}")
            
            # 显示各交易所发现的数量
            for exchange, info in status["exchanges"].items():
                print(f"  {exchange}: 发现{info['symbols_discovered']}个，订阅{info['symbols_subscribed']}个")
            
    except KeyboardInterrupt:
        print("\n测试中断")
    finally:
        await manager.stop()
        print("测试完成")

if __name__ == "__main__":
    # 单独运行时进行测试
    asyncio.run(_test_module())