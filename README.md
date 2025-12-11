# rai.yincangshuang
Railway隐藏式双服务，webs隐藏在http之内

# 🏦 加密货币套利交易系统

实时监控所有USDT永续合约，支持资金费率套利和止损管理。

## ✨ 功能特点
- **无限制监控**：订阅币安、OKX所有USDT永续合约
- **实时数据流**：WebSocket实时推送价格、资金费率数据
- **套利发现**：自动发现资金费率套利机会
- **交易执行**：支持创建套利配对和止损单
- **完整监控**：内置系统监控和调试API
- **跨平台**：专为Railway部署优化

## 🚀 快速部署到Railway

### 方法一：一键部署（推荐）
[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template/你的模板链接)

### 方法二：手动部署
1. Fork此仓库
2. 在 [Railway](https://railway.app) 新建项目
3. 选择 "Deploy from GitHub repo"
4. 选择此仓库
5. Railway会自动部署

## 📡 API接口

### 主要端点
- `GET /` - 服务器信息
- `GET /health` - 健康检查
- `WebSocket /ws` - 实时数据流
- `GET /admin/monitor` - 监控面板

### 调试API
- `GET /api/debug/ws-status` - WebSocket状态
- `GET /api/debug/test-exchange-connection` - 测试交易所连接
- `POST /api/debug/restart-websocket` - 重启WebSocket

## 🏗️ 项目结构
arbitrage-trading-system/
├──app.py              # 主应用
├──websocket_client.py # WebSocket客户端（无限制版本）
├──http_client.py      # 交易模块
├──debug_api.py        # 调试API
├──requirements.txt    # Python依赖
├──railway.json        # Railway配置
├──Procfile           # 启动配置
└──arbitrage.db       # SQLite数据库（自动创建）

## 🔧 技术栈
- **后端**: Python 3.11, FastAPI, Uvicorn
- **数据源**: Binance, OKX WebSocket API
- **数据库**: SQLite3
- **部署**: Railway
- **实时通信**: WebSocket

## 📊 监控和调试
系统包含完整的监控功能：
1. **实时数据统计** - 监控币种数量、更新频率
2. **系统资源监控** - CPU、内存使用情况
3. **连接统计** - 前端连接数、消息速率
4. **调试API** - 在线诊断WebSocket问题

## ⚠️ 注意事项
1. **免费版限制**：Railway免费版有512MB内存限制
2. **API限制**：交易所API有调用频率限制
3. **数据延迟**：网络延迟可能影响套利机会
4. **交易风险**：加密货币交易存在风险

## 📄 许可证
私有项目 - 仅供个人使用

## 🤝 支持
如有问题，请检查：
1. Railway日志
2. 调试API端点
3. 环境变量配置
