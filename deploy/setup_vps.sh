#!/bin/bash
set -e

echo "========================================"
echo "  自动交易系统 - VPS 一键部署脚本"
echo "========================================"
echo ""

INSTALL_DIR="/opt/trading"
DB_NAME="trading_system"
DB_USER="trading_user"
SERVICE_USER="trading"

read -p "请输入数据库密码 (trading_user): " DB_PASSWORD
if [ -z "$DB_PASSWORD" ]; then
    echo "错误: 数据库密码不能为空"
    exit 1
fi

echo ""
echo "[1/7] 安装系统依赖..."
apt update
apt install -y software-properties-common curl unzip \
    postgresql postgresql-contrib \
    nginx certbot python3-certbot-nginx 2>/dev/null || true

apt install -y python3.11 python3.11-venv python3.11-dev python3-pip 2>/dev/null || {
    echo "python3.11 不可用，尝试添加 deadsnakes PPA..."
    apt install -y software-properties-common
    add-apt-repository ppa:deadsnakes/ppa -y
    apt update
    apt install -y python3.11 python3.11-venv python3.11-dev python3-pip
}

echo ""
echo "[2/7] 创建系统用户..."
if id "$SERVICE_USER" &>/dev/null; then
    echo "用户 $SERVICE_USER 已存在，跳过"
else
    useradd -r -m -s /bin/bash $SERVICE_USER
    echo "用户 $SERVICE_USER 创建成功"
fi

echo ""
echo "[3/7] 配置 PostgreSQL 数据库..."
sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname='$DB_USER'" | grep -q 1 || {
    sudo -u postgres psql -c "CREATE USER $DB_USER WITH ENCRYPTED PASSWORD '$DB_PASSWORD';"
}
sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'" | grep -q 1 || {
    sudo -u postgres psql -c "CREATE DATABASE $DB_NAME OWNER $DB_USER;"
}
sudo -u postgres psql -d $DB_NAME -c "GRANT ALL ON SCHEMA public TO $DB_USER;" 2>/dev/null
echo "数据库配置完成"

echo ""
echo "[4/7] 部署应用代码..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

mkdir -p $INSTALL_DIR
cp -r "$PROJECT_DIR"/* $INSTALL_DIR/
cp -r "$PROJECT_DIR"/deploy $INSTALL_DIR/
chown -R $SERVICE_USER:$SERVICE_USER $INSTALL_DIR

echo ""
echo "[5/7] 创建 Python 虚拟环境并安装依赖..."
sudo -u $SERVICE_USER bash -c "
cd $INSTALL_DIR
python3.11 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
"

echo ""
echo "[6/7] 创建 .env 环境变量文件..."
SESSION_SECRET=$(python3 -c "import secrets; print(secrets.token_hex(32))")

cat > $INSTALL_DIR/.env << ENVEOF
DATABASE_URL=postgresql://$DB_USER:$DB_PASSWORD@localhost:5432/$DB_NAME
SESSION_SECRET=$SESSION_SECRET
# DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/your_webhook
# DISCORD_TTS_WEBHOOK_URL=https://discord.com/api/webhooks/your_tts_webhook
# ALPACA_API_KEY=your_alpaca_key
# ALPACA_SECRET_KEY=your_alpaca_secret
ENVEOF

chown $SERVICE_USER:$SERVICE_USER $INSTALL_DIR/.env
chmod 600 $INSTALL_DIR/.env

echo ""
echo "[7/7] 初始化数据库表 & 创建 systemd 服务..."
mkdir -p $INSTALL_DIR/logs
chown $SERVICE_USER:$SERVICE_USER $INSTALL_DIR/logs

sudo -u $SERVICE_USER bash -c "
cd $INSTALL_DIR
source venv/bin/activate
export \$(cat .env | grep -v '^#' | xargs)
python3 -c 'from app import app, db; app.app_context().push(); db.create_all(); print(\"数据库表创建成功\")'
"

cat > /etc/systemd/system/trading.service << 'SVCEOF'
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

[Install]
WantedBy=multi-user.target
SVCEOF

systemctl daemon-reload
systemctl enable trading

echo ""
echo "========================================"
echo "  部署完成！"
echo "========================================"
echo ""
echo "下一步操作："
echo ""
echo "1. 配置 Tiger API 密钥："
echo "   cp $INSTALL_DIR/deploy/tiger_openapi_config.properties.example $INSTALL_DIR/tiger_openapi_config.properties"
echo "   nano $INSTALL_DIR/tiger_openapi_config.properties"
echo ""
echo "2. (可选) 编辑 Discord 和其他配置："
echo "   nano $INSTALL_DIR/.env"
echo ""
echo "3. 启动服务："
echo "   sudo systemctl start trading"
echo "   sudo systemctl status trading"
echo ""
echo "4. (可选) 配置 Nginx + SSL："
echo "   参考 deploy/README.md 中的说明"
echo ""
echo "5. 查看日志："
echo "   sudo journalctl -u trading -f"
echo ""
