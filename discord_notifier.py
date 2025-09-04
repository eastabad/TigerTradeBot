import requests
import logging
from datetime import datetime
from config import get_config

logger = logging.getLogger(__name__)

class DiscordNotifier:
    def __init__(self):
        self.webhook_url = None
        self._load_config()
    
    def _load_config(self):
        """加载Discord webhook配置"""
        self.webhook_url = get_config('DISCORD_WEBHOOK_URL', '')
        if not self.webhook_url:
            logger.warning("Discord webhook URL未配置，通知将被禁用")
    
    def send_notification(self, content, title=None):
        """发送Discord通知"""
        if not self.webhook_url:
            logger.debug("Discord webhook未配置，跳过通知")
            return False
        
        try:
            # 构建Discord消息
            embed = {
                "title": title or "交易系统通知",
                "description": content,
                "color": 0x00ff00,  # 绿色
                "timestamp": datetime.utcnow().isoformat(),
                "footer": {
                    "text": "老虎证券自动交易系统"
                }
            }
            
            payload = {
                "embeds": [embed]
            }
            
            # 发送到Discord
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 204:
                logger.info(f"Discord通知发送成功: {content}")
                return True
            else:
                logger.error(f"Discord通知发送失败: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"发送Discord通知时发生错误: {str(e)}")
            return False
    
    def send_order_notification(self, trade, status, is_close=False):
        """发送订单状态通知"""
        if not trade:
            return False
        
        try:
            # 获取股票中文名称（如果可用）
            symbol_name = self._get_stock_chinese_name(trade.symbol)
            
            # 构建消息内容
            if is_close:
                if status == 'filled':
                    content = f"🔸 **平仓完成**\n股票: {symbol_name} ({trade.symbol})\n数量: {trade.quantity}股\n结果: **完全成交**"
                    color = 0xff9500  # 橙色
                elif status == 'partially_filled':
                    filled_qty = getattr(trade, 'filled_quantity', 0) or 0
                    content = f"🔸 **平仓进行中**\n股票: {symbol_name} ({trade.symbol})\n数量: {trade.quantity}股\n结果: **部分成交** ({filled_qty}股)"
                    color = 0xffff00  # 黄色
                else:
                    content = f"🔸 **平仓状态**\n股票: {symbol_name} ({trade.symbol})\n数量: {trade.quantity}股\n结果: {status}"
                    color = 0x888888  # 灰色
                title = "持仓平仓通知"
            else:
                if status == 'filled':
                    action = "买入" if trade.side.value == 'buy' else "卖出"
                    content = f"✅ **订单完成**\n股票: {symbol_name} ({trade.symbol})\n{action}数量: {trade.quantity}股\n结果: **完全成交**"
                    if trade.filled_price:
                        content += f"\n成交价: ${trade.filled_price:.2f}"
                    color = 0x00ff00  # 绿色
                elif status == 'partially_filled':
                    action = "买入" if trade.side.value == 'buy' else "卖出"
                    filled_qty = getattr(trade, 'filled_quantity', 0) or 0
                    content = f"⏳ **订单部分成交**\n股票: {symbol_name} ({trade.symbol})\n{action}数量: {trade.quantity}股\n结果: **部分成交** ({filled_qty}股)"
                    if trade.filled_price:
                        content += f"\n成交价: ${trade.filled_price:.2f}"
                    color = 0xffff00  # 黄色
                else:
                    action = "买入" if trade.side.value == 'buy' else "卖出"
                    content = f"📊 **订单状态更新**\n股票: {symbol_name} ({trade.symbol})\n{action}数量: {trade.quantity}股\n状态: {status}"
                    color = 0x888888  # 灰色
                title = "交易订单通知"
            
            # 构建Discord embed
            embed = {
                "title": title,
                "description": content,
                "color": color,
                "timestamp": datetime.utcnow().isoformat(),
                "footer": {
                    "text": "老虎证券自动交易系统"
                }
            }
            
            payload = {
                "embeds": [embed]
            }
            
            # 发送通知
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 204:
                logger.info(f"订单状态Discord通知发送成功: {trade.symbol} - {status}")
                return True
            else:
                logger.error(f"订单状态Discord通知发送失败: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"发送订单状态Discord通知时发生错误: {str(e)}")
            return False
    
    def _get_stock_chinese_name(self, symbol):
        """获取股票中文名称"""
        # 常见股票中文名称映射
        name_map = {
            'AAPL': '苹果',
            'TSLA': '特斯拉', 
            'GOOGL': '谷歌',
            'AMZN': '亚马逊',
            'MSFT': '微软',
            'NVDA': '英伟达',
            'META': 'Meta',
            'NFLX': '奈飞',
            'BABA': '阿里巴巴',
            'PLTR': 'Palantir',
            'AMD': '超微半导体',
            'COIN': 'Coinbase',
            'SHOP': 'Shopify'
        }
        
        return name_map.get(symbol, symbol)

# 全局实例
discord_notifier = DiscordNotifier()