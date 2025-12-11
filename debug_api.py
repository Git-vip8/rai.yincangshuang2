"""
🏦 套利系统 - 调试API模块
用于在线诊断WebSocket连接问题，无需重启服务
"""

from fastapi import APIRouter, HTTPException, Request
import asyncio
import time
import json
from typing import Dict, Any, Optional
import logging
import aiohttp
import os  # 添加缺失的os模块导入

from websocket_client import WebSocketManager, UnlimitedSharedData, BinanceWebSocketClient, OKXWebSocketClient

logger = logging.getLogger(__name__)
router = APIRouter(tags=["debug"], prefix="/api/debug")

@router.get("/ws-status")
async def debug_websocket_status(request: Request):
    """诊断WebSocket连接状态"""
    try:
        # 从app中导入全局实例
        from app import websocket_manager, shared_market_data
        
        if not websocket_manager:
            return {
                "success": False,
                "error": "WebSocket管理器未初始化",
                "timestamp": time.time()
            }
        
        status = websocket_manager.get_status()
        shared_stats = shared_market_data.get_stats() if shared_market_data else {}
        
        # 检查WebSocket客户端状态
        client_status = {}
        if hasattr(websocket_manager, 'clients'):
            for exchange, client in websocket_manager.clients.items():
                client_status[exchange] = {
                    "connected": client.is_connected if hasattr(client, 'is_connected') else False,
                    "reconnect_attempts": client.reconnect_attempts if hasattr(client, 'reconnect_attempts') else 0,
                    "symbols_discovered": client.stats.get("symbols_discovered", 0) if hasattr(client, 'stats') else 0,
                    "symbols_subscribed": client.stats.get("symbols_subscribed", 0) if hasattr(client, 'stats') else 0
                }
        
        # 获取当前数据样本
        current_data = shared_market_data.get_all() if shared_market_data else {}
        sample_data = {}
        for exchange in ["binance", "okx"]:
            if exchange in current_data:
                symbols = list(current_data[exchange].keys())
                if symbols:
                    # 取前3个币种作为样本
                    sample_symbols = symbols[:3]
                    sample_data[exchange] = {
                        symbol: current_data[exchange][symbol]
                        for symbol in sample_symbols
                    }
        
        return {
            "success": True,
            "timestamp": time.time(),
            "manager": {
                "running": websocket_manager.running,
                "clients_count": len(websocket_manager.clients) if hasattr(websocket_manager, 'clients') else 0,
                "tasks_count": len(websocket_manager.tasks) if hasattr(websocket_manager, 'tasks') else 0,
                "status": status
            },
            "clients": client_status,
            "shared_data": {
                "stats": shared_stats,
                "sample_data": sample_data,
                "total_symbols": {
                    "binance": len(current_data.get("binance", {})),
                    "okx": len(current_data.get("okx", {}))
                }
            }
        }
        
    except Exception as e:
        logger.error(f"获取WebSocket状态失败: {e}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": time.time()
        }

@router.get("/test-exchange-connection")
async def test_exchange_connection(
    exchange: str = "binance",
    request: Request = None
):
    """测试交易所API连接"""
    try:
        # 创建测试用的共享数据
        test_shared_data = UnlimitedSharedData()
        
        if exchange == "binance":
            client = BinanceWebSocketClient(test_shared_data)
        elif exchange == "okx":
            client = OKXWebSocketClient(test_shared_data)
        else:
            return {
                "success": False,
                "error": f"不支持的交易所: {exchange}",
                "timestamp": time.time()
            }
        
        # 测试CCXT客户端（获取永续合约列表）
        logger.info(f"测试 {exchange} 连接...")
        
        # 获取永续合约列表
        await client.initialize()  # 这会获取所有USDT永续合约
        
        # 测试WebSocket连接
        connection_result = False
        if client.usdt_perpetual_symbols:
            try:
                # 尝试连接WebSocket
                connection_result = await client.connect()
                if connection_result:
                    await client.disconnect()
            except Exception as conn_error:
                logger.warning(f"WebSocket连接测试失败: {conn_error}")
        
        return {
            "success": True,
            "exchange": exchange,
            "symbols_discovered": len(client.usdt_perpetual_symbols),
            "symbols_subscribed": client.stats.get("symbols_subscribed", 0),
            "sample_symbols": client.usdt_perpetual_symbols[:10] if client.usdt_perpetual_symbols else [],
            "websocket_connection_test": connection_result,
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"测试交易所连接失败: {e}")
        return {
            "success": False,
            "error": str(e),
            "exchange": exchange,
            "timestamp": time.time()
        }

@router.post("/restart-websocket")
async def restart_websocket_manager(request: Request):
    """重新启动WebSocket管理器"""
    try:
        from app import websocket_manager
        
        if not websocket_manager:
            return {
                "success": False,
                "error": "WebSocket管理器未初始化",
                "timestamp": time.time()
            }
        
        logger.info("正在重新启动WebSocket管理器...")
        
        # 停止当前的WebSocket管理器
        if websocket_manager.running:
            await websocket_manager.stop()
        
        # 等待一小段时间
        await asyncio.sleep(1)
        
        # 重新初始化
        await websocket_manager.initialize()
        
        # 重新启动
        await websocket_manager.start()
        
        # 检查状态
        status = websocket_manager.get_status()
        
        return {
            "success": True,
            "message": "WebSocket管理器已重新启动",
            "status": status,
            "timestamp": time.time()
        }
        
    except Exception as e:
        logger.error(f"重新启动WebSocket管理器失败: {e}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": time.time()
        }

@router.get("/exchange-urls")
async def get_exchange_urls():
    """获取交易所WebSocket地址信息"""
    from websocket_client import CONFIG
    
    return {
        "success": True,
        "urls": CONFIG["exchanges"],
        "timestamp": time.time(),
        "note": "这些是当前配置的WebSocket地址，如果连接失败可能是地址变更或网络问题"
    }

@router.get("/server-info")
async def get_server_info(request: Request):
    """获取服务器详细信息"""
    try:
        # 直接导入需要的模块，不检查auth_manager
        from app import (
            connection_manager, system_monitor, keep_alive_manager,
            auth_manager, trade_manager, http_client_manager, data_bridge,
            websocket_manager, shared_market_data
        )
        
        # 收集各个模块的状态
        info = {
            "timestamp": time.time(),
            "modules": {
                "connection_manager": bool(connection_manager),
                "system_monitor": bool(system_monitor),
                "keep_alive_manager": bool(keep_alive_manager),
                "auth_manager": bool(auth_manager),
                "trade_manager": bool(trade_manager),
                "http_client_manager": bool(http_client_manager),
                "data_bridge": bool(data_bridge),
                "websocket_manager": bool(websocket_manager),
                "shared_market_data": bool(shared_market_data)
            },
            "environment": {
                "port": request.url.port,
                "host": request.url.hostname,
                "scheme": request.url.scheme,
                "render_environment": bool("RENDER" in os.environ),
                "railway_environment": bool("RAILWAY" in os.environ)
            }
        }
        
        return {
            "success": True,
            "info": info
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "timestamp": time.time()
        }

@router.get("/check-data-flow")
async def check_data_flow():
    """检查数据流是否正常"""
    try:
        from app import shared_market_data, websocket_manager
        
        if not shared_market_data or not websocket_manager:
            return {
                "success": False,
                "error": "必要模块未初始化",
                "timestamp": time.time()
            }
        
        # 获取统计信息
        stats = shared_market_data.get_stats()
        ws_status = websocket_manager.get_status()
        
        # 计算数据新鲜度
        now = time.time()
        fresh_count = 0
        total_count = 0
        
        data = shared_market_data.get_all()
        for exchange in ["binance", "okx"]:
            if exchange in data:
                for symbol, symbol_data in data[exchange].items():
                    total_count += 1
                    if now - symbol_data.get("_ts", 0) < 10:  # 10秒内算新鲜
                        fresh_count += 1
        
        freshness_percent = round((fresh_count / total_count * 100) if total_count > 0 else 0, 1)
        
        return {
            "success": True,
            "data_flow": {
                "total_symbols": stats.get("total_symbols", 0),
                "total_updates": stats.get("updates_received", 0),
                "active_symbols": stats.get("symbols_count", {}),
                "update_rate_per_min": stats.get("update_rate_per_min", 0),
                "broadcast_rate_per_min": stats.get("broadcast_rate_per_min", 0),
                "data_freshness_percent": freshness_percent,
                "fresh_data_count": fresh_count,
                "total_data_count": total_count,
                "websocket_status": ws_status.get("exchanges", {})
            },
            "timestamp": time.time()
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "timestamp": time.time()
        }

# 健康检查端点
@router.get("/health")
async def debug_health():
    """调试健康检查"""
    return {
        "status": "healthy",
        "service": "debug_api",
        "timestamp": time.time()
    }