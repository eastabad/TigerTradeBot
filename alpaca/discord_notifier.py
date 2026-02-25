import requests
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

STOCK_NAMES = {
    'AAPL': '苹果', 'TSLA': '特斯拉', 'GOOGL': '谷歌', 'AMZN': '亚马逊',
    'MSFT': '微软', 'NVDA': '英伟达', 'META': 'Meta', 'NFLX': '奈飞',
    'BABA': '阿里巴巴', 'PLTR': 'Palantir', 'AMD': '超微半导体',
    'COIN': 'Coinbase', 'SHOP': 'Shopify', 'MSTR': 'MicroStrategy',
    'SPY': 'SPY指数', 'QQQ': 'QQQ指数', 'TQQQ': 'TQQQ三倍做多',
    'SQQQ': 'SQQQ三倍做空', 'SOXL': 'SOXL三倍做多芯片',
}


class AlpacaDiscordNotifier:
    def __init__(self):
        self.webhook_url = None
        self.tts_webhook_url = None
        self._load_config()

    def _load_config(self):
        self.webhook_url = os.environ.get('DISCORD_WEBHOOK_URL', '')
        self.tts_webhook_url = os.environ.get('DISCORD_TTS_WEBHOOK_URL', '')

    def _stock_name(self, symbol):
        clean = (symbol or '').replace('[ALPACA]', '').strip()
        return STOCK_NAMES.get(clean, clean)

    def _send_embed(self, embed):
        if not self.webhook_url:
            return False
        try:
            resp = requests.post(
                self.webhook_url,
                json={"embeds": [embed]},
                timeout=10
            )
            if resp.status_code == 204:
                logger.info(f"Alpaca Discord通知发送成功")
                return True
            else:
                logger.error(f"Alpaca Discord通知发送失败: {resp.status_code}")
                return False
        except Exception as e:
            logger.error(f"Alpaca Discord通知异常: {e}")
            return False

    def _send_tts(self, content):
        if not self.tts_webhook_url:
            return False
        try:
            resp = requests.post(
                self.tts_webhook_url,
                json={"content": content},
                timeout=10
            )
            return resp.status_code == 204
        except Exception as e:
            logger.error(f"Alpaca TTS通知异常: {e}")
            return False

    def send_order_notification(self, trade, status, is_close=False):
        if not trade:
            return False
        try:
            symbol = trade.symbol
            name = self._stock_name(symbol)
            qty = int(trade.quantity or 0)
            price_str = f"${trade.filled_price:.2f}" if trade.filled_price else ""
            side = (trade.side.value if hasattr(trade.side, 'value') else str(trade.side)).lower()

            if is_close:
                if status == 'filled':
                    pos_type = "做多仓位" if side == 'sell' else "做空仓位"
                    content = f"🔸 **[Alpaca] 平仓完成**\n股票: {name} ({symbol})\n数量: {qty}股\n结果: **完全成交**"
                    if price_str:
                        content += f"\n成交价: {price_str}"
                    tts = f"Alpaca {name}平仓{pos_type}{qty}股完全成交{price_str}"
                    color = 0xff9500
                elif status == 'partially_filled':
                    filled_qty = int(getattr(trade, 'filled_quantity', 0) or 0)
                    content = f"🔸 **[Alpaca] 平仓进行中**\n股票: {name} ({symbol})\n数量: {qty}股\n部分成交: {filled_qty}股"
                    if price_str:
                        content += f"\n成交价: {price_str}"
                    tts = f"Alpaca {name}平仓{qty}股部分成交{filled_qty}股"
                    color = 0xffff00
                else:
                    content = f"🔸 **[Alpaca] 平仓状态**\n股票: {name} ({symbol})\n数量: {qty}股\n状态: {status}"
                    tts = f"Alpaca {name}平仓{qty}股状态{status}"
                    color = 0x888888
                title = "[Alpaca] 平仓通知"
            else:
                action = "买入" if side == 'buy' else "卖出"
                if status == 'filled':
                    open_type = "做多开仓买入" if side == 'buy' else "做空开仓卖出"
                    content = f"✅ **[Alpaca] 订单完成**\n股票: {name} ({symbol})\n{action}数量: {qty}股\n结果: **完全成交**"
                    if price_str:
                        content += f"\n成交价: {price_str}"
                    tts = f"Alpaca {name}{open_type}{qty}股完全成交{price_str}"
                    color = 0x00ff00
                elif status == 'partially_filled':
                    filled_qty = int(getattr(trade, 'filled_quantity', 0) or 0)
                    content = f"⏳ **[Alpaca] 订单部分成交**\n股票: {name} ({symbol})\n{action}数量: {qty}股\n部分成交: {filled_qty}股"
                    if price_str:
                        content += f"\n成交价: {price_str}"
                    tts = f"Alpaca {name}{action}{qty}股部分成交{filled_qty}股"
                    color = 0xffff00
                elif status == 'rejected' or status == 'canceled':
                    content = f"❌ **[Alpaca] 订单{status}**\n股票: {name} ({symbol})\n{action}数量: {qty}股"
                    tts = f"Alpaca {name}{action}{qty}股订单{status}"
                    color = 0xff0000
                else:
                    content = f"📊 **[Alpaca] 订单状态**\n股票: {name} ({symbol})\n{action}数量: {qty}股\n状态: {status}"
                    tts = f"Alpaca {name}{action}{qty}股状态{status}"
                    color = 0x888888
                title = "[Alpaca] 交易订单通知"

            embed = {
                "title": title,
                "description": content,
                "color": color,
                "timestamp": datetime.utcnow().isoformat(),
                "footer": {"text": "Alpaca Paper Trading System"}
            }

            success_main = self._send_embed(embed)
            success_tts = self._send_tts(tts)
            return success_main or success_tts

        except Exception as e:
            logger.error(f"Alpaca订单Discord通知异常: {e}")
            return False

    def send_trailing_stop_notification(self, symbol, event_type, current_price,
                                         entry_price, profit_pct, details):
        try:
            name = self._stock_name(symbol)
            profit_display = (profit_pct or 0) * 100

            if event_type == 'switch':
                title = "[Alpaca] 移动止损切换"
                color = 0x3498db
                content = (f"🔄 **切换至移动止损**\n"
                          f"股票: {name} ({symbol})\n"
                          f"入场价: ${entry_price:.2f}\n"
                          f"当前价: ${current_price:.2f}\n"
                          f"当前盈利: {profit_display:.2f}%\n"
                          f"原因: {details}")
                tts = f"Alpaca {name}切换至移动止损,盈利{profit_display:.1f}%"

            elif event_type == 'trigger':
                title = "[Alpaca] 移动止损触发"
                color = 0xe74c3c
                content = (f"🛑 **移动止损触发**\n"
                          f"股票: {name} ({symbol})\n"
                          f"入场价: ${entry_price:.2f}\n"
                          f"触发价: ${current_price:.2f}\n"
                          f"最终盈利: {profit_display:.2f}%\n"
                          f"原因: {details}")
                tts = f"Alpaca {name}移动止损触发,盈利{profit_display:.1f}%"

            elif event_type == 'update':
                title = "[Alpaca] 止损价更新"
                color = 0x2ecc71
                content = (f"📈 **止损价上移**\n"
                          f"股票: {name} ({symbol})\n"
                          f"当前价: ${current_price:.2f}\n"
                          f"盈利: {profit_display:.2f}%\n"
                          f"详情: {details}")
                tts = None
            else:
                return False

            embed = {
                "title": title,
                "description": content,
                "color": color,
                "timestamp": datetime.utcnow().isoformat(),
                "footer": {"text": "Alpaca Trailing Stop System"}
            }

            success = self._send_embed(embed)
            if tts:
                self._send_tts(tts)
            return success

        except Exception as e:
            logger.error(f"Alpaca移动止损Discord通知异常: {e}")
            return False

    def send_position_close_notification(self, position):
        try:
            name = self._stock_name(position.symbol)
            pnl = position.realized_pnl or 0
            entry_price = position.avg_entry_price or 0
            exit_price = position.avg_exit_price or 0
            qty = int(position.total_entry_quantity or 0)
            pnl_pct = (pnl / (entry_price * qty) * 100) if entry_price and qty else 0

            if pnl >= 0:
                emoji = "💰"
                color = 0x00ff00
                result_text = "盈利"
            else:
                emoji = "📉"
                color = 0xff0000
                result_text = "亏损"

            content = (f"{emoji} **[Alpaca] 仓位关闭**\n"
                      f"股票: {name} ({position.symbol})\n"
                      f"方向: {'做多' if position.side == 'long' else '做空'}\n"
                      f"数量: {qty}股\n"
                      f"入场均价: ${entry_price:.2f}\n"
                      f"出场均价: ${exit_price:.2f}\n"
                      f"{result_text}: **${pnl:.2f}** ({pnl_pct:+.2f}%)")

            tts = f"Alpaca {name}仓位关闭,{result_text}{abs(pnl):.2f}美元"

            embed = {
                "title": f"[Alpaca] 仓位关闭 - {result_text}",
                "description": content,
                "color": color,
                "timestamp": datetime.utcnow().isoformat(),
                "footer": {"text": "Alpaca Paper Trading System"}
            }

            success = self._send_embed(embed)
            self._send_tts(tts)
            return success

        except Exception as e:
            logger.error(f"Alpaca仓位关闭Discord通知异常: {e}")
            return False

    def send_oco_notification(self, symbol, event_type, details=""):
        try:
            name = self._stock_name(symbol)

            if event_type == 'created':
                title = "[Alpaca] OCO保护已创建"
                color = 0x3498db
                content = f"🛡️ **OCO保护订单已创建**\n股票: {name} ({symbol})\n{details}"
            elif event_type == 'triggered_stop':
                title = "[Alpaca] OCO止损触发"
                color = 0xe74c3c
                content = f"🛑 **OCO止损触发**\n股票: {name} ({symbol})\n{details}"
            elif event_type == 'triggered_tp':
                title = "[Alpaca] OCO止盈触发"
                color = 0x2ecc71
                content = f"🎯 **OCO止盈触发**\n股票: {name} ({symbol})\n{details}"
            elif event_type == 'cancelled':
                title = "[Alpaca] OCO已取消"
                color = 0x888888
                content = f"⚪ **OCO订单已取消**\n股票: {name} ({symbol})\n{details}"
            else:
                return False

            embed = {
                "title": title,
                "description": content,
                "color": color,
                "timestamp": datetime.utcnow().isoformat(),
                "footer": {"text": "Alpaca OCO System"}
            }
            return self._send_embed(embed)

        except Exception as e:
            logger.error(f"Alpaca OCO Discord通知异常: {e}")
            return False

    def send_signal_notification(self, signal_data, result_status, error=None):
        try:
            symbol = signal_data.get('symbol', 'N/A')
            action = signal_data.get('action', 'N/A')
            name = self._stock_name(symbol)

            if result_status == 'executed':
                title = "[Alpaca] 信号执行成功"
                color = 0x00ff00
                content = f"📡 **信号已执行**\n股票: {name} ({symbol})\n动作: {action}\n"
                if signal_data.get('quantity'):
                    content += f"数量: {signal_data['quantity']}股\n"
                if signal_data.get('price'):
                    content += f"价格: ${signal_data['price']}\n"
            elif result_status == 'error':
                title = "[Alpaca] 信号执行失败"
                color = 0xff0000
                content = f"❌ **信号执行失败**\n股票: {name} ({symbol})\n动作: {action}\n"
                if error:
                    content += f"错误: {error}\n"
            else:
                title = "[Alpaca] 信号接收"
                color = 0x888888
                content = f"📡 **信号已接收**\n股票: {name} ({symbol})\n动作: {action}\n状态: {result_status}"

            embed = {
                "title": title,
                "description": content,
                "color": color,
                "timestamp": datetime.utcnow().isoformat(),
                "footer": {"text": "Alpaca Signal System"}
            }
            return self._send_embed(embed)

        except Exception as e:
            logger.error(f"Alpaca信号Discord通知异常: {e}")
            return False


alpaca_discord = AlpacaDiscordNotifier()
