#!/bin/bash

# Trading System Startup Script
# Usage: ./start.sh [dev|prod]

MODE=${1:-prod}
PORT=${PORT:-5000}
WORKERS=${WORKERS:-2}

cd "$(dirname "$0")/.." || exit 1

if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

if [ -d "venv" ]; then
    source venv/bin/activate
fi

echo "========================================="
echo "  Trading System - Starting in $MODE mode"
echo "  Port: $PORT | Workers: $WORKERS"
echo "========================================="

if [ "$MODE" = "prod" ]; then
    mkdir -p logs
    gunicorn --bind 127.0.0.1:$PORT \
             --workers $WORKERS \
             --timeout 120 \
             --access-logfile logs/access.log \
             --error-logfile logs/error.log \
             main:app
else
    gunicorn --bind 0.0.0.0:$PORT \
             --workers 1 \
             --reload \
             --timeout 120 \
             main:app
fi
