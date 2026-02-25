# 自动交易系统 VPS 全新部署指南

## 目录
1. [系统要求](#系统要求)
2. [一键快速部署](#一键快速部署)
3. [手动分步部署](#手动分步部署)
4. [Tiger API 配置](#tiger-api-配置)
5. [配置环境变量](#配置环境变量)
6. [Systemd 服务管理](#systemd-服务管理)
7. [Nginx 反向代理 + SSL](#nginx-反向代理--ssl)
8. [防火墙配置](#防火墙配置)
9. [Webhook 端点](#webhook-端点)
10. [日常运维](#日常运维)
11. [常见问题](#常见问题)
12. [升级更新](#升级更新)

---

## 系统要求

| 项目 | 要求 |
|------|------|
| 操作系统 | Ubuntu 22.04 LTS (推荐) / Debian 12 / CentOS 9 |
| Python | 3.11+ |
| PostgreSQL | 14+ |
| 内存 | >= 1GB RAM |
| 磁盘 | >= 5GB 可用空间 |
| 网络 | 需要访问 Tiger API 服务器 (openapi.itigerup.com) |

---

## 一键快速部署

上传 `trading_system_deploy.tar.gz` 到 VPS 后执行：

```bash
# 1. 上传部署包到 VPS
scp trading_system_deploy.tar.gz root@your_server_ip:/root/

# 2. SSH 登录 VPS
ssh root@your_server_ip

# 3. 解压
cd /root
tar xzf trading_system_deploy.tar.gz
cd trading_system

# 4. 运行一键部署脚本
chmod +x deploy/setup_vps.sh
sudo bash deploy/setup_vps.sh

# 5. 按提示配置 Tiger API 和环境变量（见后面章节）

# 6. 启动服务
sudo systemctl start trading
sudo systemctl status trading
```

---

## 手动分步部署

### 第一步：安装系统依赖

```bash
# Ubuntu/Debian
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3.11 python3.11-venv python3-pip \
    postgresql postgresql-contrib \
    nginx certbot python3-certbot-nginx \
    git curl unzip

# 如果 python3.11 不可用，添加 deadsnakes PPA
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install -y python3.11 python3.11-venv python3.11-dev
```

### 第二步：创建系统用户

```bash
sudo useradd -r -m -s /bin/bash trading
```

### 第三步：创建 PostgreSQL 数据库

```bash
sudo -u postgres psql << 'SQL'
CREATE DATABASE trading_system;
CREATE USER trading_user WITH ENCRYPTED PASSWORD 'your_secure_password_here';
GRANT ALL PRIVILEGES ON DATABASE trading_system TO trading_user;
ALTER DATABASE trading_system OWNER TO trading_user;
\c trading_system
GRANT ALL ON SCHEMA public TO trading_user;
SQL
```

> 请将 `your_secure_password_here` 替换为你自己的强密码。

### 第四步：部署应用代码

```bash
# 解压到目标目录
sudo mkdir -p /opt/trading
sudo tar xzf trading_system_deploy.tar.gz -C /opt/trading --strip-components=1
sudo chown -R trading:trading /opt/trading

# 切换到 trading 用户
sudo -u trading bash
cd /opt/trading

# 创建 Python 虚拟环境
python3.11 -m venv venv
source venv/bin/activate

# 安装依赖
pip install --upgrade pip
pip install -r requirements.txt
```

### 第五步：配置 Tiger API

```bash
cd /opt/trading

# 从模板复制配置文件
cp deploy/tiger_openapi_config.properties.example tiger_openapi_config.properties

# 编辑配置文件，填入你的 Tiger API 凭证
nano tiger_openapi_config.properties
```

需要填写的字段：

| 字段 | 说明 | 示例 |
|------|------|------|
| `tiger_id` | Tiger 开发者ID | `20156340` |
| `private_key_pk1` | PKCS#1 格式私钥 | 从 Tiger 开发者后台获取 |
| `private_key_pk8` | PKCS#8 格式私钥 (必需) | 从 Tiger 开发者后台获取 |
| `account` | 真实交易账户号 | `50904193` |
| `license` | 牌照类型 | `TBSG` (新加坡) 或 `TBUS` (美国) |
| `device_id` | 设备标识符 (任意唯一字符串) | `my-vps-001` |

> 私钥从 Tiger Trade 开发者后台 (https://developer.itigerup.com) 获取。

### 第六步：配置环境变量

```bash
cd /opt/trading

cat > .env << 'EOF'
# ========== 数据库配置 ==========
DATABASE_URL=postgresql://trading_user:your_secure_password_here@localhost:5432/trading_system

# ========== Flask 配置 ==========
SESSION_SECRET=这里填一个随机字符串用于加密session

# ========== Discord 通知 (可选) ==========
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/your_webhook_url
DISCORD_TTS_WEBHOOK_URL=https://discord.com/api/webhooks/your_tts_webhook_url

# ========== Alpaca API (可选) ==========
# ALPACA_API_KEY=your_alpaca_key
# ALPACA_SECRET_KEY=your_alpaca_secret
EOF

# 设置权限（仅 trading 用户可读）
chmod 600 .env
```

> 生成随机 SESSION_SECRET: `python3 -c "import secrets; print(secrets.token_hex(32))"`

### 第七步：初始化数据库表

```bash
cd /opt/trading
source venv/bin/activate
source .env  # 或: export $(cat .env | grep -v '^#' | xargs)

python3 -c "
from app import app, db
with app.app_context():
    db.create_all()
    print('数据库表创建成功！')
"
```

### 第八步：测试启动

```bash
cd /opt/trading
source venv/bin/activate
source .env

# 先手动测试一下
gunicorn --bind 0.0.0.0:5000 --workers 1 --reload main:app

# 浏览器访问 http://your_server_ip:5000 确认正常
# Ctrl+C 停止
```

---

## Systemd 服务管理

### 创建服务文件

```bash
sudo tee /etc/systemd/system/trading.service << 'EOF'
[Unit]
Description=Automated Trading System
After=network.target postgresql.service
Wants=postgresql.service

[Service]
Type=simple
User=trading
Group=trading
WorkingDirectory=/opt/trading
EnvironmentFile=/opt/trading/.env
ExecStart=/opt/trading/venv/bin/gunicorn \
    --bind 127.0.0.1:5000 \
    --workers 2 \
    --timeout 120 \
    --access-logfile /opt/trading/logs/access.log \
    --error-logfile /opt/trading/logs/error.log \
    main:app
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF
```

### 创建日志目录

```bash
sudo mkdir -p /opt/trading/logs
sudo chown trading:trading /opt/trading/logs
```

### 启用并启动服务

```bash
sudo systemctl daemon-reload
sudo systemctl enable trading
sudo systemctl start trading

# 确认状态
sudo systemctl status trading
```

---

## Nginx 反向代理 + SSL

### 配置 Nginx

```bash
sudo tee /etc/nginx/sites-available/trading << 'EOF'
server {
    listen 80;
    server_name your_domain.com;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 120s;
        proxy_connect_timeout 120s;
    }

    # Webhook 端点增大请求体限制
    location /webhook {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        client_max_body_size 1m;
    }

    location /webhook_paper {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        client_max_body_size 1m;
    }
}
EOF

# 启用站点
sudo ln -sf /etc/nginx/sites-available/trading /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl reload nginx
```

### 配置 SSL (Let's Encrypt)

```bash
sudo certbot --nginx -d your_domain.com
# 按提示操作，选择自动重定向 HTTP -> HTTPS

# 自动续期测试
sudo certbot renew --dry-run
```

---

## 防火墙配置

```bash
# 使用 ufw
sudo ufw allow ssh
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw enable
sudo ufw status

# 注意：不要开放 5000 端口到公网，通过 Nginx 代理即可
```

---

## Webhook 端点

部署完成后，在 TradingView 和 TradersPost 中配置：

| 用途 | URL |
|------|-----|
| 真实账户 Webhook | `https://your_domain.com/webhook` |
| 模拟账户 Webhook | `https://your_domain.com/webhook_paper` |

---

## 日常运维

### 查看服务状态和日志

```bash
# 服务状态
sudo systemctl status trading

# 实时日志
sudo journalctl -u trading -f

# 最近1小时日志
sudo journalctl -u trading --since "1 hour ago"

# 应用日志文件
tail -f /opt/trading/logs/error.log
tail -f /opt/trading/logs/access.log
```

### 重启服务

```bash
sudo systemctl restart trading
```

### 数据库备份

```bash
# 手动备份
sudo -u postgres pg_dump trading_system > /opt/trading/backup/trading_$(date +%Y%m%d_%H%M%S).sql

# 设置每日自动备份 (crontab)
sudo crontab -e
# 添加以下行 (每天凌晨3点备份):
# 0 3 * * * sudo -u postgres pg_dump trading_system > /opt/trading/backup/trading_$(date +\%Y\%m\%d).sql && find /opt/trading/backup -name "*.sql" -mtime +7 -delete
```

### 数据库进入 psql 交互

```bash
sudo -u postgres psql -d trading_system

# 常用查询
SELECT COUNT(*) FROM closed_position;
SELECT COUNT(*) FROM entry_signal_record;
SELECT COUNT(*) FROM tiger_filled_order;
SELECT COUNT(*) FROM reconciliation_run ORDER BY created_at DESC LIMIT 5;
```

---

## 升级更新

当代码有更新时：

```bash
# 1. 停止服务
sudo systemctl stop trading

# 2. 备份当前代码和数据库
sudo -u postgres pg_dump trading_system > /opt/trading/backup/pre_upgrade_$(date +%Y%m%d).sql
cp -r /opt/trading /opt/trading_backup_$(date +%Y%m%d)

# 3. 上传新的部署包并解压（保留配置文件）
cd /tmp
tar xzf trading_system_deploy.tar.gz

# 4. 更新代码文件（不覆盖配置）
cd /tmp/trading_system
for f in *.py templates/*.html static/css/*.css static/js/*.js; do
    cp "$f" "/opt/trading/$f"
done

# 5. 更新依赖（如果 requirements.txt 有变化）
sudo -u trading bash -c "cd /opt/trading && source venv/bin/activate && pip install -r requirements.txt"

# 6. 执行数据库迁移（表结构变更会自动处理）
sudo -u trading bash -c "cd /opt/trading && source venv/bin/activate && source .env && python3 -c \"from app import app, db; app.app_context().push(); db.create_all(); print('Done')\""

# 7. 重启服务
sudo systemctl start trading
sudo systemctl status trading
```

---

## 常见问题

### 数据库连接失败
```bash
# 检查 PostgreSQL 运行状态
sudo systemctl status postgresql

# 测试连接
sudo -u postgres psql -c "SELECT 1;"

# 检查 .env 中 DATABASE_URL 是否正确
cat /opt/trading/.env | grep DATABASE_URL
```

### Tiger API 连接失败
```bash
# 检查网络连通性
curl -v https://openapi.itigerup.com

# 检查 tiger_openapi_config.properties 是否存在且配置正确
ls -la /opt/trading/tiger_openapi_config.properties

# 查看具体错误日志
sudo journalctl -u trading --since "10 min ago" | grep -i tiger
```

### 后台调度器不工作
```bash
# 确认日志中有调度器启动信息
sudo journalctl -u trading | grep -i "scheduler\|trailing"

# 注意：gunicorn 多 worker 模式下，调度器只在主进程启动
# 如果调度器异常，可以设置 --workers 1 测试
```

### WebSocket 连接问题
```bash
# Tiger WebSocket 需要持久连接，检查 Nginx 超时设置
# 确保 proxy_read_timeout 足够长（建议 120s+）
```

### 内存不足
```bash
# 查看内存使用
free -h

# 减少 gunicorn worker 数量
# 编辑 /etc/systemd/system/trading.service 中的 --workers 参数
```

---

## 文件结构说明

```
/opt/trading/
├── .env                              # 环境变量（敏感信息）
├── tiger_openapi_config.properties   # Tiger API 密钥配置
├── main.py                           # 入口文件
├── app.py                            # Flask 应用初始化
├── config.py                         # 配置管理
├── models.py                         # 数据库模型
├── routes.py                         # 路由和 API 端点
├── tiger_client.py                   # Tiger API 客户端
├── tiger_push_client.py              # Tiger WebSocket 推送
├── signal_parser.py                  # TradingView 信号解析
├── signal_analyzer.py                # 信号分析
├── oca_service.py                    # OCA 订单管理
├── order_tracker_service.py          # 统一订单追踪
├── trailing_stop_engine.py           # 移动止损引擎
├── trailing_stop_scheduler.py        # 止损后台调度器
├── reconciliation_service.py         # 对账服务
├── closed_position_service.py        # 平仓记录管理
├── position_cost_manager.py          # 持仓成本管理
├── push_event_handlers.py            # 推送事件处理
├── discord_notifier.py               # Discord 通知
├── cleanup_production_data.py        # 数据清理工具
├── requirements.txt                  # Python 依赖
├── templates/                        # HTML 模板
│   ├── base.html
│   ├── index.html                    # 仪表盘
│   ├── trades.html                   # 交易记录
│   ├── positions.html                # 持仓管理
│   ├── closed_trades.html            # 已平仓 + 溯源
│   ├── trade_analytics.html          # 交易分析
│   ├── trailing_stop.html            # 移动止损
│   ├── trailing_stop_config.html     # 止损配置
│   ├── config.html                   # 系统配置
│   ├── signal_logs.html              # 信号日志
│   ├── trade_detail.html             # 交易详情
│   └── tiger_closed_history.html     # Tiger 历史记录
├── static/
│   ├── css/custom.css
│   └── js/app.js
├── logs/                             # 日志目录
└── deploy/                           # 部署相关文件
    ├── README.md                     # 本文档
    ├── setup_vps.sh                  # 一键部署脚本
    ├── start.sh                      # 启动脚本
    └── tiger_openapi_config.properties.example
```
