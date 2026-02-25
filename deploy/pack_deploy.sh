#!/bin/bash

# 打包部署脚本 - 生成 VPS 部署包
# 用法: bash deploy/pack_deploy.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
PACKAGE_NAME="trading_system_deploy"
BUILD_DIR="/tmp/${PACKAGE_NAME}"

echo "========================================"
echo "  打包 VPS 部署包"
echo "========================================"

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR/trading_system"

TARGET="$BUILD_DIR/trading_system"

echo "[1/4] 复制 Python 源码..."
cp "$PROJECT_DIR"/main.py "$TARGET/"
cp "$PROJECT_DIR"/app.py "$TARGET/"
cp "$PROJECT_DIR"/config.py "$TARGET/"
cp "$PROJECT_DIR"/models.py "$TARGET/"
cp "$PROJECT_DIR"/routes.py "$TARGET/"
cp "$PROJECT_DIR"/tiger_client.py "$TARGET/"
cp "$PROJECT_DIR"/tiger_push_client.py "$TARGET/"
cp "$PROJECT_DIR"/signal_parser.py "$TARGET/"
cp "$PROJECT_DIR"/signal_analyzer.py "$TARGET/"
cp "$PROJECT_DIR"/oca_service.py "$TARGET/"
cp "$PROJECT_DIR"/order_tracker_service.py "$TARGET/"
cp "$PROJECT_DIR"/trailing_stop_engine.py "$TARGET/"
cp "$PROJECT_DIR"/trailing_stop_scheduler.py "$TARGET/"
cp "$PROJECT_DIR"/reconciliation_service.py "$TARGET/"
cp "$PROJECT_DIR"/closed_position_service.py "$TARGET/"
cp "$PROJECT_DIR"/position_cost_manager.py "$TARGET/"
cp "$PROJECT_DIR"/push_event_handlers.py "$TARGET/"
cp "$PROJECT_DIR"/discord_notifier.py "$TARGET/"
cp "$PROJECT_DIR"/cleanup_production_data.py "$TARGET/"

echo "[2/4] 复制前端文件..."
mkdir -p "$TARGET/templates"
cp "$PROJECT_DIR"/templates/*.html "$TARGET/templates/"

mkdir -p "$TARGET/static/css" "$TARGET/static/js"
cp "$PROJECT_DIR"/static/css/custom.css "$TARGET/static/css/"
cp "$PROJECT_DIR"/static/js/app.js "$TARGET/static/js/"

echo "[3/4] 复制部署配置..."
mkdir -p "$TARGET/deploy"
cp "$PROJECT_DIR"/deploy/README.md "$TARGET/deploy/"
cp "$PROJECT_DIR"/deploy/setup_vps.sh "$TARGET/deploy/"
cp "$PROJECT_DIR"/deploy/start.sh "$TARGET/deploy/"
cp "$PROJECT_DIR"/deploy/tiger_openapi_config.properties.example "$TARGET/deploy/"

cp "$PROJECT_DIR"/requirements.txt "$TARGET/"

echo "[4/4] 生成压缩包..."
cd "$BUILD_DIR"
tar czf "$PROJECT_DIR/${PACKAGE_NAME}.tar.gz" trading_system/

rm -rf "$BUILD_DIR"

FILESIZE=$(du -h "$PROJECT_DIR/${PACKAGE_NAME}.tar.gz" | cut -f1)
echo ""
echo "========================================"
echo "  打包完成！"
echo "========================================"
echo ""
echo "  文件: ${PACKAGE_NAME}.tar.gz"
echo "  大小: $FILESIZE"
echo "  位置: $PROJECT_DIR/${PACKAGE_NAME}.tar.gz"
echo ""
echo "  上传到 VPS:"
echo "  scp ${PACKAGE_NAME}.tar.gz root@your_server_ip:/root/"
echo ""
echo "  在 VPS 上解压:"
echo "  tar xzf ${PACKAGE_NAME}.tar.gz"
echo "  cd trading_system"
echo "  sudo bash deploy/setup_vps.sh"
echo ""
