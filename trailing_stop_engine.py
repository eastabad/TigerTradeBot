import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from app import db
from models import TrailingStopPosition, TrailingStopConfig, TrailingStopLog, TrailingStopMode, CompletedTrade, ExitMethod, Trade, OrderTracker, OrderRole

logger = logging.getLogger(__name__)

_api_error_backoff: Dict[str, datetime] = {}
_api_error_backoff_lock = threading.Lock()
API_ERROR_BACKOFF_SECONDS = 60


def _is_regular_trading_hours_static() -> bool:
    try:
        import pytz
        from datetime import time as dt_time
        et_tz = pytz.timezone('America/New_York')
        now_et = datetime.now(et_tz)
        if now_et.weekday() > 4:
            return False
        market_open = dt_time(9, 30)
        market_close = dt_time(16, 0)
        current_time = now_et.time()
        is_regular = market_open <= current_time <= market_close
        logger.debug(f"Market hours check: {now_et.strftime('%H:%M %Z')}, regular={is_regular}")
        return is_regular
    except Exception as e:
        logger.warning(f"Error checking trading hours: {e}, defaulting to extended hours (limit order)")
        return False


def _is_api_backed_off(symbol: str) -> bool:
    with _api_error_backoff_lock:
        if symbol in _api_error_backoff:
            if datetime.now() < _api_error_backoff[symbol]:
                return True
            del _api_error_backoff[symbol]
    return False


def _set_api_backoff(symbol: str, seconds: int = API_ERROR_BACKOFF_SECONDS):
    with _api_error_backoff_lock:
        _api_error_backoff[symbol] = datetime.now() + timedelta(seconds=seconds)
    logger.info(f"📊 {symbol} API backoff set for {seconds}s due to repeated errors")


class BarsCache:
    """
    K线数据缓存，5分钟更新一次
    减少API调用频率，满足5分钟级别K线的最小更新需求
    """
    
    def __init__(self, cache_duration_seconds: int = 300):
        self._cache: Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self._cache_duration = timedelta(seconds=cache_duration_seconds)
    
    def _get_cache_key(self, symbol: str, timeframe: str) -> str:
        return f"{symbol}_{timeframe}"
    
    def get(self, symbol: str, timeframe: str) -> Optional[List[Dict]]:
        """获取缓存的K线数据，如果未过期"""
        key = self._get_cache_key(symbol, timeframe)
        with self._lock:
            if key in self._cache:
                entry = self._cache[key]
                if datetime.now() - entry['timestamp'] < self._cache_duration:
                    logger.debug(f"📊 K线缓存命中: {symbol} {timeframe}")
                    return entry['bars']
                else:
                    logger.debug(f"📊 K线缓存过期: {symbol} {timeframe}")
                    del self._cache[key]
        return None
    
    def set(self, symbol: str, timeframe: str, bars: List[Dict]):
        """缓存K线数据"""
        key = self._get_cache_key(symbol, timeframe)
        with self._lock:
            self._cache[key] = {
                'bars': bars,
                'timestamp': datetime.now()
            }
            logger.debug(f"📊 K线缓存更新: {symbol} {timeframe}, {len(bars)}根K线")
    
    def invalidate(self, symbol: str = None, timeframe: str = None):
        """清除缓存"""
        with self._lock:
            if symbol and timeframe:
                key = self._get_cache_key(symbol, timeframe)
                if key in self._cache:
                    del self._cache[key]
            elif symbol:
                keys_to_delete = [k for k in self._cache if k.startswith(f"{symbol}_")]
                for k in keys_to_delete:
                    del self._cache[k]
            else:
                self._cache.clear()
    
    def get_cache_info(self) -> Dict:
        """获取缓存状态信息"""
        with self._lock:
            now = datetime.now()
            return {
                'total_entries': len(self._cache),
                'entries': {
                    key: {
                        'bars_count': len(entry['bars']),
                        'age_seconds': (now - entry['timestamp']).total_seconds(),
                        'expires_in_seconds': max(0, (self._cache_duration - (now - entry['timestamp'])).total_seconds())
                    }
                    for key, entry in self._cache.items()
                }
            }


_bars_cache = BarsCache(cache_duration_seconds=300)


def get_bars_cache_info() -> Dict:
    """获取K线缓存状态信息，供调试使用"""
    from atr_cache_service import get_atr_cache_info
    from kline_service import get_kline_stats
    return {
        'atr_memory_cache': get_atr_cache_info(),
        'kline_db_stats': get_kline_stats(),
        'legacy_memory_cache': _bars_cache.get_cache_info(),
    }


def invalidate_bars_cache(symbol: str = None):
    """清除K线缓存"""
    from atr_cache_service import invalidate_atr_cache
    invalidate_atr_cache(symbol)
    _bars_cache.invalidate(symbol)


def _sync_trade_stop_loss_cleared(position: 'TrailingStopPosition', old_order_id: str = None, reason: str = 'cleared', commit: bool = False):
    """
    Sync stop loss order ID cleared to Trade table and mark OrderTracker as cancelled.
    
    Called when stop_loss_order_id is set to None (cancelled, rejected, not found, orphaned).
    
    Args:
        position: The TrailingStopPosition being updated
        old_order_id: The old stop loss order ID being cleared
        reason: Reason for clearing (cancelled, rejected, not_found, orphaned)
        commit: Whether to commit immediately (health-check caller handles its own commit)
    """
    try:
        if not position:
            return
        
        trade = None
        if position.trade_id:
            trade = Trade.query.get(position.trade_id)
        if trade and trade.stop_loss_order_id:
            logger.info(f"📋 Clearing Trade.stop_loss_order_id: {trade.stop_loss_order_id} (reason: {reason})")
            trade.stop_loss_order_id = None
            trade.updated_at = datetime.utcnow()
        
        # Mark old OrderTracker as cancelled/invalid
        if old_order_id:
            old_tracker = OrderTracker.query.filter_by(tiger_order_id=str(old_order_id)).first()
            if old_tracker and old_tracker.status not in ['filled', 'cancelled']:
                old_tracker.status = 'cancelled'
                old_tracker.updated_at = datetime.utcnow()
                old_tracker.notes = f"Cleared: {reason}"
                logger.info(f"📋 Marked OrderTracker {old_order_id} as cancelled ({reason})")
        
        if commit:
            db.session.commit()
        
    except Exception as e:
        logger.error(f"❌ Failed to sync stop loss cleared to Trade/OrderTracker: {str(e)}")
        if commit:
            try:
                db.session.rollback()
            except:
                pass


def sync_stop_loss_order_to_trade(position: 'TrailingStopPosition', new_order_id: str, new_stop_price: float, old_order_id: str = None, commit: bool = True, create_tracker: bool = True):
    """
    Sync stop loss order ID update to Trade table and optionally create OrderTracker record.
    
    This is called whenever Trailing Stop modifies/creates a new stop loss order,
    ensuring Trade table and OrderTracker stay in sync.
    
    Args:
        position: The TrailingStopPosition being updated
        new_order_id: The new stop loss order ID (STP_LMT)
        new_stop_price: The new stop loss price (aux_price/trigger price)
        old_order_id: The old order ID being replaced (optional)
        commit: Whether to commit immediately (False allows caller to batch commits)
        create_tracker: Whether to create OrderTracker record (False for discovery-only updates)
    """
    try:
        if not position or not new_order_id:
            return
        
        # Calculate proper limit_price for STP_LMT order
        # For sell STP_LMT: limit_price should be below stop (0.5% slippage)
        # For buy STP_LMT: limit_price should be above stop (0.5% slippage)
        is_sell = position.side == 'long'  # Long position -> sell to exit
        if is_sell:
            limit_price = round(new_stop_price * 0.995, 2)  # 0.5% below stop
        else:
            limit_price = round(new_stop_price * 1.005, 2)  # 0.5% above stop
        
        trade = None
        if position.trade_id:
            trade = Trade.query.get(position.trade_id)
        if trade:
            old_sl_id = trade.stop_loss_order_id
            trade.stop_loss_order_id = str(new_order_id)
            trade.stop_loss_price = new_stop_price
            trade.updated_at = datetime.utcnow()
            logger.info(f"📋 Synced Trade.stop_loss_order_id: {old_sl_id} → {new_order_id} for trade_id={position.trade_id}")
        
        # Create or update OrderTracker record
        existing_tracker = OrderTracker.query.filter_by(tiger_order_id=str(new_order_id)).first()
        
        if existing_tracker:
            existing_tracker.stop_price = new_stop_price
            existing_tracker.limit_price = limit_price
            existing_tracker.updated_at = datetime.utcnow()
            logger.info(f"📋 Updated OrderTracker {new_order_id} prices: stop=${new_stop_price:.2f} limit=${limit_price:.2f}")
        elif create_tracker:
            account_type = 'paper' if '[PAPER]' in (position.symbol or '') else 'real'
            tracker = OrderTracker(
                tiger_order_id=str(new_order_id),
                symbol=position.symbol.replace('[PAPER]', ''),
                account_type=account_type,
                role=OrderRole.STOP_LOSS,
                order_type='STP_LMT',
                side='SELL' if is_sell else 'BUY',
                quantity=position.quantity,
                limit_price=limit_price,
                stop_price=new_stop_price,
                status='PENDING',
                parent_order_id=str(trade.tiger_order_id) if trade and trade.tiger_order_id else None,
                trailing_stop_id=position.id if position.id else None,
                created_at=datetime.utcnow(),
            )
            db.session.add(tracker)
            logger.info(f"📋 Created OrderTracker for stop loss order {new_order_id}")
        
        # Mark old OrderTracker as cancelled if exists
        if old_order_id and old_order_id != new_order_id:
            old_tracker = OrderTracker.query.filter_by(tiger_order_id=str(old_order_id)).first()
            if old_tracker:
                old_tracker.status = 'CANCELLED'
                old_tracker.updated_at = datetime.utcnow()
                logger.info(f"📋 Marked OrderTracker {old_order_id} as cancelled")
        
        if commit:
            db.session.commit()
        
    except Exception as e:
        logger.error(f"❌ Failed to sync stop loss order to Trade/OrderTracker: {str(e)}")
        if commit:
            try:
                db.session.rollback()
            except:
                pass


def get_realtime_price_with_websocket_fallback(symbol: str, quote_client=None) -> Optional[Dict]:
    """
    Get realtime price, preferring WebSocket cached data, falling back to API call
    
    During pre-market/post-market hours, skip WebSocket cache and use API directly
    because WebSocket only pushes regular session prices.
    
    Args:
        symbol: Stock symbol (clean, without [PAPER] prefix)
        quote_client: Tiger quote client for fallback
        
    Returns:
        Dict with 'price', 'session', 'source' keys, or None if failed
    """
    # Check current market session
    current_session = 'regular'
    try:
        import pytz
        eastern = pytz.timezone('America/New_York')
        now_et = datetime.now(eastern)
        hour = now_et.hour
        minute = now_et.minute
        weekday = now_et.weekday()
        
        if weekday < 5:  # Weekday
            current_minutes = hour * 60 + minute
            if 240 <= current_minutes < 570:  # 04:00 - 09:30 ET
                current_session = 'pre_market'
            elif 570 <= current_minutes < 960:  # 09:30 - 16:00 ET
                current_session = 'regular'
            elif 960 <= current_minutes < 1200:  # 16:00 - 20:00 ET
                current_session = 'post_market'
            else:
                current_session = 'closed'
        else:
            current_session = 'closed'
    except Exception as e:
        logger.debug(f"Error determining market session: {str(e)}")
    
    use_websocket = current_session == 'regular'
    
    if use_websocket:
        try:
            from tiger_push_client import get_push_manager
            
            push_manager = get_push_manager()
            if push_manager.is_connected:
                max_age = push_manager.get_adaptive_cache_max_age()
                cached_quote = push_manager.get_cached_quote_if_fresh(symbol, max_age_seconds=max_age)
                if cached_quote and cached_quote.get('latest_price'):
                    price = cached_quote['latest_price']
                    session = cached_quote.get('session', 'unknown')
                    hour_trading = cached_quote.get('hour_trading', False)
                    source = cached_quote.get('source', 'websocket')
                    
                    logger.debug(f"📊 {symbol} WebSocket price: ${price:.2f} (session: {session}, source: {source})")
                    return {
                        'price': price,
                        'session': 'pre_market' if hour_trading else 'regular',
                        'source': source if source else 'websocket'
                    }
                else:
                    logger.debug(f"📊 {symbol} WebSocket cache stale or missing, falling back to API")
        except ImportError:
            pass
        except Exception as e:
            logger.debug(f"WebSocket price not available for {symbol}: {str(e)}")
    else:
        logger.debug(f"📊 {symbol} Skipping WebSocket (session: {current_session}), using API")
    
    if quote_client and not _is_api_backed_off(symbol):
        try:
            trade_data = quote_client.get_smart_price(symbol)
            if trade_data:
                try:
                    from tiger_push_client import get_push_manager
                    pm = get_push_manager()
                    pm.update_cache_from_api(symbol, trade_data['price'], trade_data.get('session', 'regular'))
                    pm.record_api_call()
                except Exception:
                    pass
                return trade_data
        except Exception as e:
            error_msg = str(e)
            if 'permission denied' in error_msg.lower() or 'code=4' in error_msg.lower():
                _set_api_backoff(symbol, 120)
            else:
                logger.error(f"API price fetch failed for {symbol}: {error_msg}")

    try:
        from eodhd_price_service import get_eodhd_smart_price
        eodhd_result = get_eodhd_smart_price(symbol)
        if eodhd_result and eodhd_result.get('price', 0) > 0:
            logger.debug(f"📊 {symbol} price from EODHD: ${eodhd_result['price']:.2f} (source: {eodhd_result.get('source', 'eodhd')})")
            return eodhd_result
    except Exception as e:
        logger.debug(f"EODHD price fallback error for {symbol}: {e}")

    return None


def batch_refresh_stale_prices(symbols: list, quote_client=None) -> Dict[str, Dict]:
    """Batch refresh prices for symbols with stale WebSocket cache.
    Uses one API call (get_batch_smart_prices) to get all prices at once.
    Falls back to EODHD for symbols Tiger API missed.
    Updates the WebSocket cache with API results.
    
    Returns:
        {symbol: {'price': float, 'session': str, 'source': str}}
    """
    if not symbols:
        return {}

    results = {}

    if quote_client:
        symbols_to_fetch = [s for s in symbols if not _is_api_backed_off(s)]
        if symbols_to_fetch:
            try:
                batch_results = quote_client.get_batch_smart_prices(symbols_to_fetch)

                if batch_results:
                    results.update(batch_results)
                    try:
                        from tiger_push_client import get_push_manager
                        pm = get_push_manager()
                        pm.record_api_call()
                        for sym, data in batch_results.items():
                            pm.update_cache_from_api(sym, data['price'], data.get('session', 'regular'))
                    except Exception:
                        pass

                    logger.info(f"📊 Batch API refresh: {len(batch_results)}/{len(symbols_to_fetch)} symbols updated: {list(batch_results.keys())}")
            except Exception as e:
                error_msg = str(e)
                if 'permission denied' in error_msg.lower() or 'code=4' in error_msg.lower():
                    for sym in symbols_to_fetch:
                        _set_api_backoff(sym, 120)
                else:
                    logger.error(f"Batch price refresh failed: {e}")

    still_missing = [s for s in symbols if s not in results]
    if still_missing:
        try:
            from eodhd_price_service import get_eodhd_batch_prices
            eodhd_results = get_eodhd_batch_prices(still_missing)
            if eodhd_results:
                results.update(eodhd_results)
                eodhd_count = len(eodhd_results)
                logger.info(f"📊 EODHD batch fallback: {eodhd_count}/{len(still_missing)} symbols: {list(eodhd_results.keys())}")
        except Exception as e:
            logger.debug(f"EODHD batch fallback error: {e}")

    return results


def get_actual_stop_order_id(tiger_client, symbol: str, stored_order_id: str) -> str:
    """
    查询该股票当前的open orders，找到实际的止损订单ID
    加仓后止损订单会产生新的order_id，所以需要查询当前挂单
    
    Args:
        tiger_client: Tiger API client instance
        symbol: Stock symbol (without [PAPER] prefix)
        stored_order_id: The order_id stored in database
    
    Returns:
        The actual stop order ID (may differ from stored_order_id after scaling)
    """
    try:
        clean_symbol = symbol.replace('[PAPER]', '').strip()
        open_orders_result = tiger_client.get_open_orders_for_symbol(clean_symbol)
        
        if open_orders_result.get('success'):
            for order in open_orders_result.get('orders', []):
                order_type = getattr(order, 'order_type', '')
                order_type_str = str(order_type).upper()
                # Stop orders have type 'STP' or 'STOP' or similar
                if 'STP' in order_type_str or 'STOP' in order_type_str:
                    actual_id = str(order.id)
                    if actual_id != stored_order_id:
                        logger.info(f"📋 {symbol} 止损订单ID已更新: {stored_order_id} -> {actual_id}")
                    return actual_id
        
        # Fallback to stored order_id
        return stored_order_id
        
    except Exception as e:
        logger.warning(f"查询{symbol}当前止损订单失败: {e}, 使用存储的ID")
        return stored_order_id


def get_actual_take_profit_order_id(tiger_client, symbol: str, stored_order_id: str) -> str:
    """
    查询该股票当前的open orders，找到实际的止盈订单ID
    加仓后止盈订单会产生新的order_id，所以需要查询当前挂单
    
    Args:
        tiger_client: Tiger API client instance
        symbol: Stock symbol (without [PAPER] prefix)
        stored_order_id: The order_id stored in database
    
    Returns:
        The actual take profit order ID (may differ from stored_order_id after scaling)
    """
    try:
        clean_symbol = symbol.replace('[PAPER]', '').strip()
        open_orders_result = tiger_client.get_open_orders_for_symbol(clean_symbol)
        
        if open_orders_result.get('success'):
            for order in open_orders_result.get('orders', []):
                order_type = getattr(order, 'order_type', '')
                order_type_str = str(order_type).upper()
                # Limit orders (take profit) have type 'LMT' or 'LIMIT'
                if 'LMT' in order_type_str or 'LIMIT' in order_type_str:
                    actual_id = str(order.id)
                    if actual_id != stored_order_id:
                        logger.info(f"📋 {symbol} 止盈订单ID已更新: {stored_order_id} -> {actual_id}")
                    return actual_id
        
        # Fallback to stored order_id
        return stored_order_id
        
    except Exception as e:
        logger.warning(f"查询{symbol}当前止盈订单失败: {e}, 使用存储的ID")
        return stored_order_id


def create_or_recover_stop_order(
    tiger_client,
    position,
    stop_price: float,
    side: str
) -> dict:
    """
    创建或恢复止损订单（当找不到现有止损订单时调用）
    
    Args:
        tiger_client: Tiger API client
        position: TrailingStopPosition对象
        stop_price: 止损价格
        side: 'sell' (多头) 或 'buy' (空头)
        
    Returns:
        dict with success, order_id, message
    """
    import time
    clean_symbol = position.symbol.replace('[PAPER]', '').strip()
    
    is_long = position.side == 'long'
    validated_sl = _validate_stop_loss_price(stop_price, position.entry_price, position.side, clean_symbol)
    if validated_sl is None:
        logger.error(f"❌ {clean_symbol} 止损价 ${stop_price:.2f} 对于{'多头' if is_long else '空头'}"
                     f"(入场价 ${position.entry_price:.2f}) 方向不合理，跳过创建止损订单")
        return {
            'success': False,
            'order_id': None,
            'message': f'止损价方向不合理: stop=${stop_price:.2f} vs entry=${position.entry_price:.2f}'
        }
    
    logger.warning(f"⚠️ {position.symbol} 止损订单丢失，尝试创建新订单")
    
    outside_rth = (position.account_type != 'paper')
    
    new_order_result = tiger_client.place_stop_limit_order(
        symbol=clean_symbol,
        action=side.upper(),
        quantity=abs(position.quantity),
        stop_price=stop_price,
        outside_rth=outside_rth
    )
    
    if new_order_result.get('success'):
        new_order_id = new_order_result['order_id']
        position.stop_loss_order_id = new_order_id
        position.fixed_stop_loss = stop_price
        
        sync_stop_loss_order_to_trade(position, new_order_id, stop_price, None, commit=False)
        db.session.commit()
        
        logger.info(f"✅ {position.symbol} 自动创建止损订单成功: {new_order_id} @ ${stop_price:.2f}")
        
        try:
            from discord_notifier import discord_notifier
            discord_notifier.send_notification(
                f"⚠️ **止损订单自动恢复**\n"
                f"股票: {position.symbol}\n"
                f"止损价: ${stop_price:.2f}\n"
                f"新订单ID: {new_order_id}",
                title="止损订单恢复通知"
            )
        except Exception as e:
            logger.warning(f"发送恢复通知失败: {e}")
        
        return {
            'success': True,
            'order_id': new_order_id,
            'message': f'自动创建止损订单成功: {new_order_id}'
        }
    else:
        error_msg = new_order_result.get('error', 'Unknown error')
        logger.error(f"❌ {position.symbol} 创建止损订单失败: {error_msg}")
        
        try:
            from discord_notifier import discord_notifier
            discord_notifier.send_notification(
                f"🚨 **紧急: 止损订单创建失败**\n"
                f"股票: {position.symbol}\n"
                f"错误: {error_msg}\n"
                f"⚠️ 仓位目前无止损保护！",
                title="紧急警报"
            )
        except Exception as e:
            logger.warning(f"发送紧急通知失败: {e}")
        
        return {
            'success': False,
            'order_id': None,
            'message': f'创建止损订单失败: {error_msg}'
        }


def modify_stop_with_retry(
    tiger_client,
    position,
    actual_stop_order_id: str,
    new_stop_price: float,
    side: str,
    max_retries: int = 3
) -> dict:
    """
    带重试机制的止损订单修改
    
    如果修改失败，尝试取消旧订单并创建新订单
    
    Args:
        tiger_client: Tiger API client
        position: TrailingStopPosition对象
        actual_stop_order_id: 当前止损订单ID
        new_stop_price: 新止损价格
        side: 'sell' (多头) 或 'buy' (空头)
        max_retries: 最大重试次数
        
    Returns:
        dict with success, new_order_id, message
    """
    import time
    clean_symbol = position.symbol.replace('[PAPER]', '').strip()
    
    for attempt in range(max_retries):
        modify_result = tiger_client.modify_stop_loss_price(
            old_order_id=actual_stop_order_id,
            symbol=clean_symbol,
            quantity=abs(position.quantity),
            new_stop_price=new_stop_price,
            side=side
        )
        
        if modify_result.get('success'):
            new_order_id = modify_result['new_order_id']
            old_order_id = actual_stop_order_id
            position.stop_loss_order_id = new_order_id
            position.fixed_stop_loss = new_stop_price
            
            sync_stop_loss_order_to_trade(position, new_order_id, new_stop_price, old_order_id, commit=False)
            db.session.commit()
            
            return {
                'success': True,
                'new_order_id': new_order_id,
                'message': f'止损修改成功 (尝试{attempt+1}次)'
            }
        
        error_msg = modify_result.get('message', 'Unknown error')
        logger.warning(f"⚠️ {position.symbol} 止损修改失败 (尝试{attempt+1}/{max_retries}): {error_msg}")
        
        if attempt < max_retries - 1:
            time.sleep(0.5)
    
    logger.warning(f"⚠️ {position.symbol} 修改重试用尽，尝试取消重建")
    
    cancel_result = tiger_client.cancel_order(actual_stop_order_id)
    
    if cancel_result.get('success'):
        logger.info(f"✅ {position.symbol} 旧止损订单已取消: {actual_stop_order_id}")
        
        return create_or_recover_stop_order(
            tiger_client=tiger_client,
            position=position,
            stop_price=new_stop_price,
            side=side
        )
    else:
        cancel_error = cancel_result.get('error', 'Unknown')
        order_status = tiger_client.get_order_status(actual_stop_order_id) if hasattr(tiger_client, 'get_order_status') else {}
        status = order_status.get('status', 'UNKNOWN')
        
        if status in ['FILLED', 'CANCELLED', 'EXPIRED']:
            logger.info(f"📋 {position.symbol} 止损订单状态: {status}，创建新订单")
            return create_or_recover_stop_order(
                tiger_client=tiger_client,
                position=position,
                stop_price=new_stop_price,
                side=side
            )
        else:
            logger.error(f"❌ {position.symbol} 取消止损失败且订单仍活跃: {cancel_error}")
            return {
                'success': False,
                'new_order_id': None,
                'message': f'取消重建失败: {cancel_error}'
            }


def safe_cancel_order_for_close(
    tiger_client,
    order_id: str,
    symbol: str,
    order_type: str = 'stop_loss'
) -> dict:
    """
    平仓时安全取消订单
    
    会检查订单状态确保安全取消
    
    Args:
        tiger_client: Tiger API client
        order_id: 订单ID
        symbol: 股票代码
        order_type: 'stop_loss' 或 'take_profit'
        
    Returns:
        dict with success, can_proceed (是否可以继续平仓), message
    """
    import time
    
    if not order_id:
        return {'success': True, 'can_proceed': True, 'message': '无订单需要取消'}
    
    for attempt in range(2):
        cancel_result = tiger_client.cancel_order(order_id)
        
        if cancel_result.get('success'):
            logger.info(f"✅ {symbol} {order_type}订单已取消: {order_id}")
            return {'success': True, 'can_proceed': True, 'message': '取消成功'}
        
        if attempt < 1:
            time.sleep(0.3)
    
    if hasattr(tiger_client, 'get_order_status'):
        try:
            order_status = tiger_client.get_order_status(order_id)
            status = order_status.get('status', 'UNKNOWN')
            
            if status in ['FILLED', 'CANCELLED', 'EXPIRED']:
                logger.info(f"📋 {symbol} {order_type}订单状态: {status}，可以继续")
                return {'success': True, 'can_proceed': True, 'message': f'订单已{status}'}
            elif status in ['PENDING', 'SUBMITTED', 'ACCEPTED']:
                logger.warning(f"⚠️ {symbol} {order_type}订单仍活跃({status})，暂缓平仓")
                return {'success': False, 'can_proceed': False, 'message': f'订单仍活跃: {status}'}
            else:
                logger.warning(f"⚠️ {symbol} {order_type}订单状态未知({status})，尝试继续")
                return {'success': False, 'can_proceed': True, 'message': f'状态未知: {status}'}
        except Exception as e:
            logger.warning(f"⚠️ {symbol} 获取订单状态失败: {e}，尝试继续")
            return {'success': False, 'can_proceed': True, 'message': f'状态查询失败: {e}'}
    else:
        logger.warning(f"⚠️ {symbol} 无法检查订单状态，尝试继续")
        return {'success': False, 'can_proceed': True, 'message': '无法检查状态'}


def get_trailing_stop_config() -> TrailingStopConfig:
    config = TrailingStopConfig.query.first()
    if not config:
        config = TrailingStopConfig()
        db.session.add(config)
        db.session.commit()
    return config


def calculate_atr(bars: List[Dict], period: int = 14) -> float:
    """
    计算ATR (Average True Range) 使用RMA平滑方法
    与TradingView官方ATR计算方法一致
    
    RMA公式: RMA = (previous_RMA * (period - 1) + current_value) / period
    """
    if len(bars) < period + 1:
        logger.warning(f"Not enough bars for ATR calculation: {len(bars)} < {period + 1}")
        return 0.0
    
    true_ranges = []
    for i in range(1, len(bars)):
        high = bars[i]['high']
        low = bars[i]['low']
        prev_close = bars[i-1]['close']
        
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )
        true_ranges.append(tr)
    
    if len(true_ranges) < period:
        return sum(true_ranges) / len(true_ranges) if true_ranges else 0.0
    
    rma = sum(true_ranges[:period]) / period
    
    for i in range(period, len(true_ranges)):
        rma = (rma * (period - 1) + true_ranges[i]) / period
    
    return rma


def calculate_volatility_pct(atr: float, close_price: float) -> float:
    if close_price <= 0:
        return 0.0
    return atr / close_price


def get_profit_tier(profit_pct: float, config: TrailingStopConfig) -> int:
    if profit_pct < config.tier_0_threshold:
        return 0
    elif profit_pct < config.tier_1_threshold:
        return 1
    else:
        return 2


def get_base_multiplier(tier: int, config: TrailingStopConfig) -> float:
    if tier == 0:
        return config.tier_0_multiplier
    elif tier == 1:
        return config.tier_1_multiplier
    else:
        return config.tier_2_multiplier


def get_volatility_factor(volatility_pct: float, config: TrailingStopConfig) -> float:
    if volatility_pct > config.high_volatility_threshold:
        return config.high_volatility_factor
    elif volatility_pct > config.low_volatility_threshold:
        return config.mid_volatility_factor
    else:
        return config.low_volatility_factor


def calculate_dynamic_percent(profit_pct: float, config: TrailingStopConfig, is_switched: bool = False) -> float:
    """
    动态百分比止损计算 - 分段线性公式
    
    切换后使用更宽松的百分比表（给趋势更多空间）:
    - 浮盈 0% ~ 3%:   追踪距离 0.5% ~ 0.6%
    - 浮盈 3% ~ 5%:   追踪距离 0.6% ~ 0.8%
    - 浮盈 5% ~ 8%:   追踪距离 0.8% ~ 1.0%
    - 浮盈 8% ~ 10%:  追踪距离 1.0% ~ 1.2%
    - 浮盈 10% ~ 15%: 追踪距离 1.2% ~ 1.6%
    - 浮盈 > 15%:     追踪距离 2.0% (固定)
    
    切换前使用原有的较紧百分比（阶梯止损阶段备用）
    """
    if is_switched:
        if profit_pct < 0.03:
            percent = 0.005 + (profit_pct / 0.03) * 0.001
        elif profit_pct < 0.05:
            percent = 0.006 + ((profit_pct - 0.03) / 0.02) * 0.002
        elif profit_pct < 0.08:
            percent = 0.008 + ((profit_pct - 0.05) / 0.03) * 0.002
        elif profit_pct < 0.10:
            percent = 0.010 + ((profit_pct - 0.08) / 0.02) * 0.002
        elif profit_pct < 0.15:
            percent = 0.012 + ((profit_pct - 0.10) / 0.05) * 0.004
        else:
            percent = 0.020
    else:
        tier1_upper = config.dynamic_pct_tier1_upper
        tier2_upper = config.dynamic_pct_tier2_upper
        tier1_pct = config.dynamic_pct_tier1_percent
        tier2_pct = config.dynamic_pct_tier2_percent
        max_pct = config.max_percent_stop
        
        if profit_pct < tier1_upper:
            percent = tier1_pct * (profit_pct / tier1_upper) if tier1_upper > 0 else 0
        elif profit_pct < tier2_upper:
            range_width = tier2_upper - tier1_upper
            if range_width > 0:
                percent = tier1_pct + (tier2_pct - tier1_pct) * ((profit_pct - tier1_upper) / range_width)
            else:
                percent = tier1_pct
        else:
            extra_profit = profit_pct - tier2_upper
            extra_range = tier2_upper - tier1_upper
            if extra_range > 0:
                percent = tier2_pct + (max_pct - tier2_pct) * min(1.0, extra_profit / extra_range)
            else:
                percent = tier2_pct
            percent = min(percent, max_pct)
    
    return max(0, percent)


def get_progressive_stop_tier(profit_pct: float, config: TrailingStopConfig) -> int:
    """
    根据盈利百分比确定应该在哪个阶梯止损级别
    
    返回 0-8 对应:
    0: 保持原始止损 (profit < tier1)
    1-8: 移到对应tier止损位
    
    加密间距版本：每1%利润上移一次止损
    """
    if not getattr(config, 'progressive_stop_enabled', True):
        return 0
    
    tier_profits = [
        getattr(config, 'prog_tier1_profit', 0.01),  # 1%
        getattr(config, 'prog_tier2_profit', 0.02),  # 2%
        getattr(config, 'prog_tier3_profit', 0.03),  # 3%
        getattr(config, 'prog_tier4_profit', 0.04),  # 4%
        getattr(config, 'prog_tier5_profit', 0.05),  # 5%
        getattr(config, 'prog_tier6_profit', 0.06),  # 6%
        getattr(config, 'prog_tier7_profit', 0.07),  # 7%
        getattr(config, 'prog_tier8_profit', 0.08),  # 8%
    ]
    
    for i in range(len(tier_profits) - 1, -1, -1):
        if profit_pct >= tier_profits[i]:
            return i + 1
    
    return 0


def calculate_progressive_stop_price(
    position: TrailingStopPosition,
    target_tier: int,
    config: TrailingStopConfig
) -> Optional[float]:
    """
    根据目标阶梯计算新的止损价格
    
    加密间距版本 - 每个阶梯对应的止损位:
    tier1: 0% (保本)
    tier2: 0.5%
    tier3: 1.5%
    tier4: 2.5%
    tier5: 3.5%
    tier6: 4.5%
    tier7: 5.5%
    tier8: 6.5%
    """
    if target_tier <= 0:
        return None
    
    stop_at_map = {
        1: getattr(config, 'prog_tier1_stop_at', 0.0),
        2: getattr(config, 'prog_tier2_stop_at', 0.005),
        3: getattr(config, 'prog_tier3_stop_at', 0.015),
        4: getattr(config, 'prog_tier4_stop_at', 0.025),
        5: getattr(config, 'prog_tier5_stop_at', 0.035),
        6: getattr(config, 'prog_tier6_stop_at', 0.045),
        7: getattr(config, 'prog_tier7_stop_at', 0.055),
        8: getattr(config, 'prog_tier8_stop_at', 0.065),
    }
    
    stop_at_pct = stop_at_map.get(target_tier, 0.0)
    entry_price = position.entry_price
    is_long = position.side == 'long'
    
    if is_long:
        new_stop = entry_price * (1 + stop_at_pct)
    else:
        new_stop = entry_price * (1 - stop_at_pct)
    
    return new_stop


def check_and_adjust_progressive_stop(
    position: TrailingStopPosition,
    current_price: float,
    config: TrailingStopConfig
) -> Dict:
    """
    检查并执行阶梯止损上移
    
    返回:
    {
        'should_adjust': bool,
        'current_tier': int,
        'new_tier': int,
        'new_stop_price': float,
        'reason': str
    }
    """
    result = {
        'should_adjust': False,
        'current_tier': getattr(position, 'progressive_stop_tier', 0) or 0,
        'new_tier': 0,
        'new_stop_price': None,
        'reason': ''
    }
    
    # 检查是否启用阶梯止损
    if not getattr(config, 'progressive_stop_enabled', True):
        result['reason'] = '阶梯止损未启用'
        return result
    
    # 注意: 即使stop_loss_order_id为空，execute_progressive_stop_adjustment函数
    # 也会尝试查询Tiger API获取实际的订单ID，所以这里不再检查order_id
    
    # 计算当前盈利百分比
    is_long = position.side == 'long'
    if is_long:
        profit_pct = (current_price - position.entry_price) / position.entry_price
    else:
        profit_pct = (position.entry_price - current_price) / position.entry_price
    
    # 确定当前应该在哪个阶梯
    target_tier = get_progressive_stop_tier(profit_pct, config)
    current_tier = result['current_tier']
    
    result['new_tier'] = target_tier
    
    # 只向上移动，不回撤
    if target_tier <= current_tier:
        result['reason'] = f'当前tier={current_tier}, 目标tier={target_tier}, 无需调整'
        return result
    
    # 计算新止损价格
    new_stop_price = calculate_progressive_stop_price(position, target_tier, config)
    
    if new_stop_price is None:
        result['reason'] = f'无法计算tier {target_tier}的止损价格'
        return result
    
    # 检查新止损价是否比当前止损更有利
    # 使用current_trailing_stop（如果有）或fixed_stop_loss作为参考
    current_stop = position.current_trailing_stop or position.fixed_stop_loss or 0
    if is_long:
        if new_stop_price <= current_stop:
            result['reason'] = f'新止损${new_stop_price:.2f} <= 当前止损${current_stop:.2f}'
            return result
    else:
        if new_stop_price >= current_stop:
            result['reason'] = f'新止损${new_stop_price:.2f} >= 当前止损${current_stop:.2f}'
            return result
    
    # 需要调整
    result['should_adjust'] = True
    result['new_stop_price'] = new_stop_price
    tier_names = {
        1: '保本', 2: '0.5%利润', 3: '1.5%利润', 4: '2.5%利润',
        5: '3.5%利润', 6: '4.5%利润', 7: '5.5%利润', 8: '6.5%利润'
    }
    result['reason'] = f'盈利{profit_pct*100:.1f}%触发tier{target_tier}({tier_names.get(target_tier, "")}), 止损上移至${new_stop_price:.2f}'
    
    return result


def execute_progressive_stop_adjustment(
    position: TrailingStopPosition,
    new_stop_price: float,
    new_tier: int,
    account_type: str = 'real'
) -> Dict:
    """
    执行阶梯止损上移 - 通过Tiger API修改止损订单
    
    同时发送外部webhook信号:
    - 如果止盈订单还存在: cancel + exit(SL+TP)
    - 如果止盈已取消(切换到移动止损): cancel + exit(SL only)
    
    返回:
    {
        'success': bool,
        'new_order_id': str,
        'message': str,
        'webhook_result': dict
    }
    """
    from tiger_client import TigerClient, TigerPaperClient
    
    result = {
        'success': False,
        'new_order_id': None,
        'message': '',
        'webhook_result': None
    }
    
    try:
        # 选择正确的客户端
        if account_type == 'paper':
            client = TigerPaperClient()
        else:
            client = TigerClient()
        
        # 确定止损方向
        side = 'sell' if position.side == 'long' else 'buy'
        
        # 查询实际的止损订单ID（即使stored_order_id为空也尝试查询）
        actual_stop_order_id = get_actual_stop_order_id(
            client, position.symbol, position.stop_loss_order_id or ''
        )
        
        if not actual_stop_order_id:
            logger.warning(f"⚠️ [{account_type.upper()}] {position.symbol} 没有找到止损订单，尝试创建新止损订单")
            action = 'SELL' if side == 'sell' else 'BUY'
            stop_result = client.place_stop_limit_order(
                symbol=position.symbol,
                action=action,
                quantity=abs(position.quantity),
                stop_price=new_stop_price
            )
            if stop_result.get('success'):
                new_order_id = stop_result.get('order_id')
                position.stop_loss_order_id = str(new_order_id)
                position.progressive_stop_tier = new_tier
                position.last_stop_adjustment_price = new_stop_price
                position.stop_adjustment_count = (position.stop_adjustment_count or 0) + 1
                position.fixed_stop_loss = new_stop_price
                position.current_trailing_stop = new_stop_price
                sync_stop_loss_order_to_trade(position, new_order_id, new_stop_price, None, commit=False)
                db.session.commit()
                result['success'] = True
                result['new_order_id'] = str(new_order_id)
                result['message'] = f"✅ 创建新止损订单成功 (无现有订单): tier{new_tier}, 止损${new_stop_price:.2f}"
                logger.info(result['message'])
            else:
                result['message'] = f"没有找到止损订单且创建失败: {stop_result.get('error')}"
                logger.error(f"⚠️ [{account_type.upper()}] {position.symbol} {result['message']}")
            return result
        
        if actual_stop_order_id != position.stop_loss_order_id:
            logger.info(f"📋 {position.symbol} 发现止损订单ID: {actual_stop_order_id}")
            old_discovered_id = position.stop_loss_order_id
            position.stop_loss_order_id = actual_stop_order_id
            # Sync discovered order ID to Trade table (no new tracker needed)
            sync_stop_loss_order_to_trade(position, actual_stop_order_id, position.fixed_stop_loss or position.entry_price, old_discovered_id, commit=False, create_tracker=False)
        
        # 调用修改止损订单
        modify_result = client.modify_stop_loss_price(
            old_order_id=actual_stop_order_id,
            symbol=position.symbol,
            quantity=position.quantity,
            new_stop_price=new_stop_price,
            side=side
        )
        
        if modify_result['success']:
            # 更新position记录
            old_order_id = actual_stop_order_id
            position.stop_loss_order_id = modify_result['new_order_id']
            position.progressive_stop_tier = new_tier
            position.last_stop_adjustment_price = new_stop_price
            position.stop_adjustment_count = (position.stop_adjustment_count or 0) + 1
            position.fixed_stop_loss = new_stop_price  # 更新固定止损价格
            position.current_trailing_stop = new_stop_price  # 同步更新移动止损显示
            
            # Sync to Trade table and OrderTracker (before final commit)
            sync_stop_loss_order_to_trade(position, modify_result['new_order_id'], new_stop_price, old_order_id, commit=False)
            
            db.session.commit()
            
            result['success'] = True
            result['new_order_id'] = modify_result['new_order_id']
            result['message'] = f'止损已上移至${new_stop_price:.2f} (tier {new_tier})'
            
            logger.info(f"📈 [{account_type.upper()}] {position.symbol} 止损上移成功: ${new_stop_price:.2f}, 新订单ID={modify_result['new_order_id']}")
            
            # 止损已通过Tiger API修改完成，无需发送外部webhook
            
        else:
            result['message'] = f"修改止损订单失败: {modify_result.get('error', 'Unknown error')}"
            logger.error(f"❌ [{account_type.upper()}] {position.symbol} 止损上移失败: {result['message']}")
        
    except Exception as e:
        result['message'] = f"执行止损上移异常: {str(e)}"
        logger.error(f"❌ [{account_type.upper()}] {position.symbol} {result['message']}")
    
    return result


def calculate_trend_strength(
    bars: List[Dict],
    current_price: float,
    entry_price: float,
    side: str,
    atr: float,
    config: TrailingStopConfig
) -> Dict:
    """
    计算综合趋势强度评分 (0-100)
    
    三个指标:
    1. ATR收敛度 - 当前ATR vs 平均ATR，越小趋势越稳定
    2. 动量评分 - 价格移动距离 / ATR，越大趋势越强
    3. 连续创新高/低 - 连续多少根K线创新高或新低
    """
    result = {
        'trend_strength': 0.0,
        'atr_convergence': 1.0,
        'momentum_score': 0.0,
        'consecutive_highs': 0,
        'atr_convergence_score': 0.0,
        'momentum_normalized': 0.0,
        'consecutive_score': 0.0
    }
    
    if len(bars) < config.momentum_lookback + 1 or atr <= 0:
        return result
    
    is_long = side == 'long'
    
    recent_bars = bars[-(config.momentum_lookback + 5):]
    atrs = []
    for i in range(1, len(recent_bars)):
        high = recent_bars[i]['high']
        low = recent_bars[i]['low']
        prev_close = recent_bars[i-1]['close']
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        atrs.append(tr)
    
    if len(atrs) >= 2:
        avg_atr = sum(atrs) / len(atrs)
        if avg_atr > 0:
            atr_convergence = atr / avg_atr
            result['atr_convergence'] = atr_convergence
            if atr_convergence < 0.7:
                result['atr_convergence_score'] = 100
            elif atr_convergence < 0.85:
                result['atr_convergence_score'] = 70
            elif atr_convergence < 1.0:
                result['atr_convergence_score'] = 50
            elif atr_convergence < 1.2:
                result['atr_convergence_score'] = 30
            else:
                result['atr_convergence_score'] = 0
    
    lookback_bars = bars[-config.momentum_lookback:]
    if len(lookback_bars) >= 2:
        start_price = lookback_bars[0]['close']
        price_move = current_price - start_price
        if not is_long:
            price_move = -price_move
        
        momentum = price_move / atr if atr > 0 else 0
        result['momentum_score'] = momentum
        
        if momentum >= 3.0:
            result['momentum_normalized'] = 100
        elif momentum >= 2.0:
            result['momentum_normalized'] = 80
        elif momentum >= 1.0:
            result['momentum_normalized'] = 60
        elif momentum >= 0.5:
            result['momentum_normalized'] = 40
        elif momentum > 0:
            result['momentum_normalized'] = 20
        else:
            result['momentum_normalized'] = 0
    
    consecutive = 0
    recent_check = bars[-min(10, len(bars)):]
    
    if is_long:
        prev_high = None
        for bar in recent_check:
            if prev_high is not None and bar['high'] > prev_high:
                consecutive += 1
            else:
                consecutive = 0
            prev_high = bar['high']
    else:
        prev_low = None
        for bar in recent_check:
            if prev_low is not None and bar['low'] < prev_low:
                consecutive += 1
            else:
                consecutive = 0
            prev_low = bar['low']
    
    result['consecutive_highs'] = consecutive
    
    if consecutive >= 5:
        result['consecutive_score'] = 100
    elif consecutive >= 4:
        result['consecutive_score'] = 80
    elif consecutive >= 3:
        result['consecutive_score'] = 60
    elif consecutive >= 2:
        result['consecutive_score'] = 40
    elif consecutive >= 1:
        result['consecutive_score'] = 20
    else:
        result['consecutive_score'] = 0
    
    trend_strength = (
        result['atr_convergence_score'] * config.atr_convergence_weight +
        result['momentum_normalized'] * config.momentum_weight +
        result['consecutive_score'] * config.consecutive_weight
    )
    result['trend_strength'] = min(100, max(0, trend_strength))
    
    return result


def calculate_trailing_stop(
    position: TrailingStopPosition,
    current_price: float,
    atr: float,
    config: TrailingStopConfig
) -> Tuple[float, Dict]:
    """
    计算移动止损价格
    
    切换前: 使用阶梯止损（Progressive Stop）
    切换后: 使用ATR和动态百分比，取更宽松的值
    """
    is_long = position.side == 'long'
    is_switched = position.has_switched_to_trailing
    
    if is_long:
        if position.highest_price is None or current_price > position.highest_price:
            position.highest_price = current_price
        reference_price = position.highest_price
        profit_pct = (current_price - position.entry_price) / position.entry_price
    else:
        if position.lowest_price is None or current_price < position.lowest_price:
            position.lowest_price = current_price
        reference_price = position.lowest_price
        profit_pct = (position.entry_price - current_price) / position.entry_price
    
    position.current_profit_pct = profit_pct
    
    volatility_pct = calculate_volatility_pct(atr, current_price)
    position.volatility_pct = volatility_pct
    position.current_atr = atr
    
    tier = get_profit_tier(max(0, profit_pct), config)
    position.profit_tier = tier
    
    prog_tier = get_progressive_stop_tier(max(0, profit_pct), config)
    
    if is_switched:
        # Check if we should tighten trailing due to cost being close to current price
        # 成本距离比 = (当前价 - 平均成本) / 当前价 (做多)
        if is_long:
            cost_distance_ratio = (current_price - position.entry_price) / current_price if current_price > 0 else 0
        else:
            cost_distance_ratio = (position.entry_price - current_price) / current_price if current_price > 0 else 0
        
        # Get tighten thresholds from config (with defaults)
        tighten_threshold = getattr(config, 'tighten_threshold', 0.02)
        tighten_atr_multiplier = getattr(config, 'tighten_atr_multiplier', 0.6)
        tighten_trail_pct = getattr(config, 'tighten_trail_pct', 0.005)
        
        # Determine if tightening is needed
        should_tighten = cost_distance_ratio < tighten_threshold and cost_distance_ratio > 0
        
        if should_tighten:
            effective_multiplier = tighten_atr_multiplier
            dynamic_percent = tighten_trail_pct
            logger.debug(f"🔧 收紧trailing: 成本距离{cost_distance_ratio*100:.2f}% < {tighten_threshold*100}%, "
                        f"ATR倍数{effective_multiplier}, trail%={dynamic_percent*100}%")
        else:
            effective_multiplier = config.post_switch_multiplier
            dynamic_percent = calculate_dynamic_percent(max(0, profit_pct), config, is_switched=True)
        
        if is_long:
            stop_atr = reference_price - (atr * effective_multiplier)
        else:
            stop_atr = reference_price + (atr * effective_multiplier)
        
        if is_long:
            stop_percent = reference_price * (1 - dynamic_percent)
        else:
            stop_percent = reference_price * (1 + dynamic_percent)
        
        if is_long:
            new_stop = min(stop_atr, stop_percent)
        else:
            new_stop = max(stop_atr, stop_percent)
        
        if position.current_trailing_stop is not None:
            if is_long:
                new_stop = max(new_stop, position.current_trailing_stop)
            else:
                new_stop = min(new_stop, position.current_trailing_stop)
    else:
        stop_atr = 0
        dynamic_percent = 0
        stop_percent = 0
        effective_multiplier = 0
        
        if prog_tier > 0:
            prog_stop = calculate_progressive_stop_price(position, prog_tier, config)
            if prog_stop is not None:
                new_stop = prog_stop
                if position.current_trailing_stop is not None:
                    if is_long:
                        new_stop = max(new_stop, position.current_trailing_stop)
                    else:
                        new_stop = min(new_stop, position.current_trailing_stop)
            else:
                new_stop = position.fixed_stop_loss or position.current_trailing_stop or position.entry_price
        else:
            new_stop = position.fixed_stop_loss or position.current_trailing_stop or position.entry_price
    
    # Include tightening info in details (only available when switched)
    cost_dist = cost_distance_ratio if is_switched else None
    is_tightened = should_tighten if is_switched else False
    
    calculation_details = {
        'reference_price': reference_price,
        'profit_pct': profit_pct,
        'tier': tier,
        'prog_tier': prog_tier,
        'effective_multiplier': effective_multiplier,
        'atr': atr,
        'volatility_pct': volatility_pct,
        'stop_atr': stop_atr,
        'dynamic_percent': dynamic_percent,
        'stop_percent': stop_percent,
        'new_stop': new_stop,
        'is_switched': is_switched,
        'cost_distance_ratio': cost_dist,
        'is_tightened': is_tightened
    }
    
    return new_stop, calculation_details


def calculate_inverse_protection_stop(
    position: TrailingStopPosition,
    current_price: float,
    trend_strength: float,
    config: TrailingStopConfig
) -> Tuple[Optional[float], Dict]:
    """
    反向保护止损计算
    
    当价格反向移动（亏损方向），根据趋势强度决定是否收紧止损
    
    触发条件: 亏损 >= 止损距离的50%
    
    收紧规则:
    - 弱趋势(0-30): 收紧至止损距离的60% (快速止损)
    - 中趋势(30-60): 收紧至止损距离的70%
    - 强趋势(60+): 保持原止损不变
    """
    is_long = position.side == 'long'
    original_stop = position.fixed_stop_loss
    
    if original_stop is None:
        return None, {'reason': 'No original stop loss set'}
    
    if is_long:
        stop_distance = position.entry_price - original_stop
        current_loss = position.entry_price - current_price
    else:
        stop_distance = original_stop - position.entry_price
        current_loss = current_price - position.entry_price
    
    if stop_distance <= 0:
        return None, {'reason': 'Invalid stop distance'}
    
    loss_ratio = current_loss / stop_distance
    
    trigger_threshold = 0.50
    
    if loss_ratio < trigger_threshold:
        return None, {
            'reason': 'Loss ratio below threshold',
            'loss_ratio': loss_ratio,
            'threshold': trigger_threshold
        }
    
    if trend_strength >= 60:
        return None, {
            'reason': 'Strong trend - keep original stop',
            'loss_ratio': loss_ratio,
            'trend_strength': trend_strength,
            'tightening_factor': 1.0
        }
    elif trend_strength >= 30:
        tightening_factor = 0.70
    else:
        tightening_factor = 0.60
    
    if is_long:
        new_stop = position.entry_price - (stop_distance * tightening_factor)
    else:
        new_stop = position.entry_price + (stop_distance * tightening_factor)
    
    current_stop = position.current_trailing_stop or original_stop
    
    should_update = False
    if is_long:
        if new_stop > current_stop:
            should_update = True
    else:
        if new_stop < current_stop:
            should_update = True
    
    if not should_update:
        return None, {
            'reason': 'New stop not tighter than current',
            'loss_ratio': loss_ratio,
            'trend_strength': trend_strength,
            'new_stop': new_stop,
            'current_stop': current_stop
        }
    
    return new_stop, {
        'action': 'tighten',
        'loss_ratio': loss_ratio,
        'trend_strength': trend_strength,
        'tightening_factor': tightening_factor,
        'original_stop': original_stop,
        'new_stop': new_stop,
        'stop_distance': stop_distance,
        'current_loss': current_loss
    }


def check_switch_condition(
    position: TrailingStopPosition,
    current_price: float,
    config: TrailingStopConfig,
    trend_strength: float = 0.0
) -> Tuple[bool, str]:
    """
    检查是否应该切换到动态移动止损
    
    三种触发条件（满足任一即切换）：
    条件A (原有): 达到止盈比例 (90%/95%计划利润)
    条件B (新增): 利润>=5% 且 趋势强度>=60
    条件C (新增): 利润>=10% 强制切换
    """
    if position.has_switched_to_trailing:
        return False, "Already switched"
    
    is_long = position.side == 'long'
    
    if is_long:
        current_profit = current_price - position.entry_price
    else:
        current_profit = position.entry_price - current_price
    
    profit_pct = current_profit / position.entry_price if position.entry_price > 0 else 0
    
    switch_profit_threshold = getattr(config, 'switch_profit_threshold', 0.05)
    switch_force_profit = getattr(config, 'switch_force_profit', 0.10)
    
    if profit_pct >= switch_force_profit:
        return True, f"[条件C] 利润{profit_pct*100:.1f}% >= {switch_force_profit*100:.0f}% 强制切换"
    
    if profit_pct >= switch_profit_threshold and trend_strength >= config.trend_strength_threshold:
        return True, f"[条件B] 利润{profit_pct*100:.1f}% + 趋势强度{trend_strength:.0f} 触发切换"
    
    if position.fixed_take_profit is not None:
        planned_profit = abs(position.fixed_take_profit - position.entry_price)
        
        if planned_profit > 0:
            profit_achieved_ratio = current_profit / planned_profit
            
            if trend_strength >= config.trend_strength_threshold:
                switch_ratio = config.switch_profit_ratio_strong
                trend_label = "强势趋势"
            else:
                switch_ratio = config.switch_profit_ratio
                trend_label = "普通趋势"
            
            if profit_achieved_ratio >= switch_ratio:
                return True, f"[条件A-{trend_label}] 达到 {profit_achieved_ratio*100:.1f}% 计划利润"
            
            return False, f"[{trend_label}] 利润{profit_pct*100:.1f}%, 计划利润进度{profit_achieved_ratio*100:.1f}%"
    
    return False, f"利润{profit_pct*100:.1f}%, 趋势{trend_strength:.0f}, 未达切换条件"


def _direct_api_holdings_check(position, clean_sym: str) -> bool:
    """Check broker API directly to verify if a position still exists.
    
    Verifies:
    1. Symbol exists in broker holdings
    2. Quantity > 0 (position not fully closed)
    3. Direction matches (long=positive qty, short=negative qty)
    
    Used as fallback when WebSocket cache is unavailable or doesn't contain the symbol.
    Returns True if matching position found at broker, False otherwise.
    """
    try:
        if position.account_type == 'paper':
            direct_tiger = TigerPaperClient()
        else:
            direct_tiger = TigerClient()
        direct_positions = direct_tiger.get_positions()
        if direct_positions.get('success'):
            for p in direct_positions.get('positions', []):
                if p.get('symbol', '') != clean_sym:
                    continue
                qty = p.get('quantity', 0)
                if qty == 0:
                    logger.warning(f"⚠️ {position.symbol} found at broker but quantity=0")
                    return False
                is_long_at_broker = qty > 0
                is_long_in_ts = position.side == 'long'
                if is_long_at_broker != is_long_in_ts:
                    logger.warning(f"⚠️ {position.symbol} direction mismatch: "
                                  f"broker={'long' if is_long_at_broker else 'short'} "
                                  f"vs trailing_stop={position.side}")
                    return False
                logger.info(f"✅ {position.symbol} confirmed at broker: qty={qty}, side={position.side}")
                return True
            logger.warning(f"⚠️ {position.symbol} NOT found in direct API positions")
            return False
        else:
            logger.warning(f"⚠️ Direct position API call failed for {position.symbol}")
            return False
    except Exception as e:
        logger.warning(f"⚠️ Direct position check error for {position.symbol}: {e}")
        return False


def check_stop_triggered(
    position: TrailingStopPosition,
    current_price: float
) -> Tuple[bool, str]:
    
    is_long = position.side == 'long'
    trailing_stop = position.current_trailing_stop
    fixed_stop = position.fixed_stop_loss
    
    if trailing_stop is None and fixed_stop is None:
        return False, "No stop set"
    
    if is_long:
        candidates = []
        if trailing_stop is not None:
            candidates.append(trailing_stop)
        if fixed_stop is not None:
            candidates.append(fixed_stop)
        effective_stop = max(candidates)
        
        if current_price <= effective_stop:
            source = "trailing" if trailing_stop and effective_stop == trailing_stop else "fixed"
            return True, f"Price ${current_price:.2f} <= {source} stop ${effective_stop:.2f}"
    else:
        candidates = []
        if trailing_stop is not None:
            candidates.append(trailing_stop)
        if fixed_stop is not None:
            candidates.append(fixed_stop)
        effective_stop = min(candidates)
        
        if current_price >= effective_stop:
            source = "trailing" if trailing_stop and effective_stop == trailing_stop else "fixed"
            return True, f"Price ${current_price:.2f} >= {source} stop ${effective_stop:.2f}"
    
    return False, "Stop not triggered"


def execute_trailing_stop_close(
    tiger,
    symbol: str,
    side: str,
    quantity: float,
    account_type: str,
    trigger_price: float,
    entry_price: float,
    profit_pct: float,
    timeframe: str = '15'
) -> Dict:
    """
    直接调用Tiger API执行平仓，不发送外部webhook
    """
    result = {
        'success': False,
        'tiger_order': None,
        'message': ''
    }
    
    try:
        if tiger and tiger.client:
            try:
                pos_result = tiger.get_positions(symbol=symbol)
                if pos_result.get('success'):
                    positions = pos_result.get('positions', [])
                    if not positions:
                        result['success'] = True
                        result['message'] = f"Position already closed (止损单已触发，无需再次平仓)"
                        logger.info(f"⚠️ {symbol} 仓位已不存在，止损单可能已触发成交，跳过重复平仓")
                        return result

                    actual_qty = positions[0].get('quantity', 0)
                    abs_qty = abs(actual_qty)
                    if abs_qty == 0:
                        result['success'] = True
                        result['message'] = f"Position quantity is 0 (止损单已触发)"
                        logger.info(f"⚠️ {symbol} 仓位数量为0，止损单可能已触发成交，跳过重复平仓")
                        return result

                    position_is_long = actual_qty > 0
                    expect_long = side.upper() in ('SELL', 'SELL_CLOSE')

                    if position_is_long != expect_long:
                        result['success'] = True
                        result['message'] = (f"Position direction mismatch: holding={'long' if position_is_long else 'short'}, "
                                             f"trying to close={'long' if expect_long else 'short'}, skipping to prevent reverse trade")
                        logger.warning(f"⚠️ {symbol} 持仓方向不匹配: 实际={'多仓' if position_is_long else '空仓'}, "
                                       f"平仓方向={'平多' if expect_long else '平空'}, 跳过以防止反向交易(AMD bug)")
                        return result

                    quantity = abs_qty
                    logger.info(f"✅ {symbol} 确认仓位存在，方向={'多仓' if position_is_long else '空仓'}，数量: {quantity}")
            except Exception as pos_err:
                logger.warning(f"⚠️ {symbol} Tiger API检查仓位失败: {str(pos_err)}, 尝试WebSocket缓存验证")
                
                try:
                    from push_event_handlers import get_cached_position
                    ws_pos = get_cached_position(symbol, account_type, max_age_seconds=30)
                    if ws_pos and ws_pos.get('is_fresh'):
                        ws_qty = ws_pos.get('quantity', 0)
                        ws_age = ws_pos.get('age_seconds', 999)
                        if ws_qty == 0:
                            result['success'] = True
                            result['message'] = f"Position confirmed closed via WebSocket cache (age={ws_age:.1f}s)"
                            logger.info(f"✅ {symbol} WebSocket缓存确认仓位已平 (数据新鲜度={ws_age:.1f}s)，跳过平仓")
                            return result
                        
                        ws_is_long = ws_qty > 0
                        expect_long = side.upper() in ('SELL', 'SELL_CLOSE')
                        if ws_is_long != expect_long:
                            result['success'] = True
                            result['message'] = (f"WebSocket cache direction mismatch: holding={'long' if ws_is_long else 'short'}, "
                                                 f"trying to close={'long' if expect_long else 'short'}, skipping")
                            logger.warning(f"⚠️ {symbol} WebSocket缓存方向不匹配，跳过以防反向交易")
                            return result
                        
                        quantity = abs(ws_qty)
                        logger.info(f"✅ {symbol} WebSocket缓存确认仓位存在 (qty={quantity}, age={ws_age:.1f}s)，继续平仓")
                    else:
                        cache_status = "无缓存" if not ws_pos else f"数据过期({ws_pos.get('age_seconds', 0):.0f}s)"
                        result['success'] = False
                        result['message'] = f"Position check failed: API error + WebSocket cache unavailable ({cache_status})"
                        logger.error(f"🚨 {symbol} API查仓位失败且WebSocket缓存不可用({cache_status})，"
                                    f"中止平仓，等待重试")
                        return result
                except Exception as ws_err:
                    logger.error(f"🚨 {symbol} WebSocket缓存查询也失败: {ws_err}，中止平仓，等待重试")
                    result['success'] = False
                    result['message'] = f"Position check failed: API error ({pos_err}) + cache error ({ws_err})"
                    return result
        
        logger.info(f"🔴 Tiger close order: symbol={symbol}, side={side}, quantity={quantity}, trigger_price={trigger_price}")
        
        if tiger and tiger.client:
            try:
                from tigeropen.common.util.contract_utils import stock_contract
                from tigeropen.common.util.order_utils import market_order as tiger_market_order, limit_order
                from tigeropen.common.consts import Currency
                
                contract = stock_contract(symbol=symbol, currency=Currency.USD)

                is_regular = _is_regular_trading_hours_static()

                if is_regular:
                    logger.info(f"🔴 盘中时段 → 使用市价单: symbol={symbol}, side={side}, quantity={quantity}")
                    order = tiger_market_order(
                        account=tiger.client_config.account,
                        contract=contract,
                        action=side,
                        quantity=int(quantity),
                    )
                    order.outside_rth = False
                    order.time_in_force = 'DAY'
                    logger.info(f"📅 Market order: outside_rth=False, time_in_force=DAY")
                else:
                    if side.upper() == 'SELL':
                        limit_price = round(trigger_price * 0.995, 2)
                    else:
                        limit_price = round(trigger_price * 1.005, 2)

                    logger.info(f"🔴 盘前盘后 → 使用限价单: symbol={symbol}, side={side}, quantity={quantity}, limit=${limit_price}")
                    order = limit_order(
                        account=tiger.client_config.account,
                        contract=contract,
                        action=side,
                        quantity=int(quantity),
                        limit_price=limit_price
                    )
                    order.outside_rth = True
                    if account_type == 'paper':
                        order.time_in_force = 'DAY'
                        logger.info(f"📅 Limit order: outside_rth=True, time_in_force=DAY (Paper)")
                    else:
                        order.time_in_force = 'GTC'
                        logger.info(f"📅 Limit order: outside_rth=True, time_in_force=GTC")
                
                tiger_result = tiger.client.place_order(order)
                
                if tiger_result:
                    order_type_str = 'market' if is_regular else 'limit'
                    result['tiger_order'] = {
                        'order_id': getattr(tiger_result, 'id', None),
                        'status': getattr(tiger_result, 'status', None)
                    }
                    result['success'] = True
                    logger.info(f"🔴 Tiger {order_type_str} order placed for {symbol}: {result['tiger_order']}")
                else:
                    result['message'] = "Tiger order returned None"
                    logger.warning(f"🔴 Tiger order returned None for {symbol}")
                    
            except Exception as e:
                logger.error(f"🔴 Error placing Tiger order for {symbol}: {str(e)}")
                result['message'] = str(e)
        else:
            tiger_exists = tiger is not None
            client_exists = getattr(tiger, 'client', None) is not None if tiger else False
            result['message'] = f"Tiger client not available (tiger={tiger_exists}, client={client_exists})"
            logger.warning(f"🔴 {result['message']}")
            
    except Exception as e:
        logger.error(f"Error in execute_trailing_stop_close: {str(e)}")
        result['message'] = str(e)
    
    return result


def log_trailing_stop_event(
    position: TrailingStopPosition,
    event_type: str,
    current_price: float = None,
    details: str = None
):
    try:
        log_entry = TrailingStopLog(
            trailing_stop_id=position.id,
            event_type=event_type,
            current_price=current_price,
            highest_price=position.highest_price,
            trailing_stop_price=position.current_trailing_stop,
            atr_value=position.current_atr,
            profit_pct=position.current_profit_pct,
            details=details
        )
        db.session.add(log_entry)
        db.session.commit()
    except Exception as e:
        logger.error(f"Error logging trailing stop event: {str(e)}")


def _cancel_pending_exit_on_recovery_tiger(position, current_price):
    """Price recovered above stop while exit order is pending. Cancel exit and resume trailing."""
    pending_order_id = position.trigger_reason.replace('pending_exit:', '')
    logger.info(f"[{position.symbol}] Price ${current_price:.2f} recovered above stop, "
                f"cancelling pending exit order {pending_order_id[:12] if len(pending_order_id) > 12 else pending_order_id}... and resuming trailing")

    from models import OrderTracker, OrderRole
    pending_exit = OrderTracker.query.filter(
        OrderTracker.symbol == position.symbol,
        OrderTracker.account_type == position.account_type,
        OrderTracker.role.in_([OrderRole.EXIT_TRAILING, OrderRole.EXIT_SIGNAL]),
        OrderTracker.status.in_(['PENDING', 'SUBMITTED', 'INITIAL', 'NEW', 'HELD'])
    ).first()

    if pending_exit:
        partial_filled = float(pending_exit.filled_quantity or 0)
        if partial_filled > 0:
            old_qty = position.quantity
            remaining = old_qty - partial_filled
            if remaining <= 0.001:
                logger.info(f"[{position.symbol}] Partial fill {partial_filled} >= position qty {old_qty}, "
                           f"exit effectively complete despite price recovery")
                return
            position.quantity = remaining
            logger.info(f"[{position.symbol}] Adjusted TS qty for partial fill: {old_qty} -> {remaining}")

        try:
            from tiger_client import TigerClient, TigerPaperClient
            if position.account_type == 'paper':
                tiger = TigerPaperClient()
            else:
                tiger = TigerClient()
            cancel_result = tiger.cancel_order(pending_exit.tiger_order_id)
            if cancel_result.get('success'):
                logger.info(f"[{position.symbol}] 已取消退出订单 {pending_exit.tiger_order_id}")
                pending_exit.status = 'CANCELLED'
            else:
                logger.warning(f"[{position.symbol}] 取消退出订单失败: {cancel_result.get('error')}")
        except Exception as e:
            logger.warning(f"[{position.symbol}] 取消退出订单异常: {e}")

    position.trigger_reason = None
    position.triggered_at = None
    position.triggered_price = None
    position.trigger_retry_count = 0

    log_trailing_stop_event(position, 'exit_cancelled_recovery', current_price,
                           f"Price recovered to ${current_price:.2f}, cancelled exit, resumed trailing")

    logger.info(f"✅ [{position.account_type.upper()}] {position.symbol} price recovered, "
               f"cancelled exit order, trailing resumed")


def _handle_stop_trigger_and_exit(position, current_price, trigger_reason, result):
    """Handle stop trigger: OCA check, breach detection, grace window, holdings verification, and exit order.
    Returns (should_return_from_caller, result_dict).
    """
    from oca_service import verify_oca_stop_protection
    from tiger_client import TigerClient, TigerPaperClient
    from models import OrderTracker, OrderRole, OCAGroup, OCAStatus
    
    oca_check = verify_oca_stop_protection(position.id, position.account_type)
    
    if oca_check['protected']:
        GRACE_WINDOW_SECONDS = 30
        now = datetime.utcnow()
        
        if position.breach_detected_at is None:
            position.breach_detected_at = now
            position.breach_price = current_price
            db.session.commit()
            logger.info(f"🔔 [{position.account_type.upper()}] {position.symbol} stop breached "
                       f"(price=${current_price:.2f}, {trigger_reason}), "
                       f"OCA stop still live ({oca_check['reason']}). "
                       f"Starting {GRACE_WINDOW_SECONDS}s grace window.")
            return True, {'success': True, 'action': 'breach_detected_awaiting_oca',
                    'message': f'{position.symbol} breach detected, OCA stop live, waiting for broker execution'}
        
        elapsed = (now - position.breach_detected_at).total_seconds()
        
        if elapsed < GRACE_WINDOW_SECONDS:
            logger.info(f"⏳ [{position.account_type.upper()}] {position.symbol} breach ongoing "
                       f"({elapsed:.0f}s/{GRACE_WINDOW_SECONDS}s), "
                       f"OCA stop still live, waiting for broker.")
            db.session.commit()
            return True, {'success': True, 'action': 'breach_awaiting_oca',
                    'message': f'{position.symbol} grace window {elapsed:.0f}s/{GRACE_WINDOW_SECONDS}s'}
        
        logger.warning(f"🚨 [{position.account_type.upper()}] {position.symbol} OCA stop failed to execute "
                      f"after {elapsed:.0f}s grace window! "
                      f"Breach price=${position.breach_price:.2f}, current=${current_price:.2f}. "
                      f"Taking over with software exit.")
        trigger_reason = (f"Software takeover: OCA stop failed after {elapsed:.0f}s "
                         f"(breach=${position.breach_price:.2f}, now=${current_price:.2f})")
        
        if position.account_type == 'paper':
            oca_check_again = verify_oca_stop_protection(position.id, position.account_type)
            if oca_check_again.get('oca_group_id'):
                try:
                    from oca_service import trigger_soft_stop
                    soft_result, soft_status = trigger_soft_stop(
                        oca_group_id=oca_check_again['oca_group_id'],
                        current_price=current_price
                    )
                    if soft_result:
                        position.breach_detected_at = None
                        position.breach_price = None
                        db.session.commit()
                        logger.info(f"✅ {position.symbol} software takeover via soft stop: {soft_status}")
                        return True, {'success': True, 'action': 'software_takeover_soft_stop',
                                'message': f'{position.symbol} OCA timed out, soft stop executed'}
                except Exception as e:
                    logger.error(f"Soft stop failed for {position.symbol}: {e}, falling through to direct exit")
        logger.info(f"🔔 [{position.account_type.upper()}] {position.symbol} grace window expired, "
                   f"proceeding to direct exit via trailing stop close.")
    else:
        if position.breach_detected_at:
            logger.info(f"🔔 [{position.account_type.upper()}] {position.symbol} OCA no longer protecting "
                       f"({oca_check['reason']}), proceeding with software exit.")
        else:
            logger.info(f"🔔 [{position.account_type.upper()}] {position.symbol} stop breached "
                       f"and no OCA protection ({oca_check['reason']}), proceeding with software exit.")
        trigger_reason = f"No OCA protection ({oca_check['reason']}): {trigger_reason}"
    
    position.breach_detected_at = None
    position.breach_price = None
    db.session.flush()
    
    holdings_verified = False
    clean_sym = position.symbol.replace('[PAPER]', '').strip()
    try:
        cached_positions = get_cached_tiger_positions(force_refresh=False)
        account_key = position.account_type
        if cached_positions.get(f'{account_key}_success', False):
            account_positions = cached_positions.get(account_key, {})
            cached_pos = account_positions.get(clean_sym)
            if cached_pos:
                cached_qty = cached_pos.get('quantity', 0)
                if cached_qty == 0:
                    logger.warning(f"⚠️ {position.symbol} in cache but qty=0. Direct API check...")
                    holdings_verified = _direct_api_holdings_check(position, clean_sym)
                else:
                    cached_is_long = cached_qty > 0
                    ts_is_long = position.side == 'long'
                    if cached_is_long == ts_is_long:
                        holdings_verified = True
                    else:
                        logger.warning(f"⚠️ {position.symbol} direction mismatch in cache: "
                                      f"broker={'long' if cached_is_long else 'short'} "
                                      f"vs trailing_stop={position.side}. Direct API check...")
                        holdings_verified = _direct_api_holdings_check(position, clean_sym)
            else:
                logger.warning(f"⚠️ {position.symbol} stop triggered but NO holdings in cache. Direct API check...")
                holdings_verified = _direct_api_holdings_check(position, clean_sym)
        else:
            logger.warning(f"⚠️ {position.symbol} cache unavailable. Direct API check...")
            holdings_verified = _direct_api_holdings_check(position, clean_sym)
    except Exception as hv_err:
        logger.warning(f"⚠️ Holdings verification error for {position.symbol}: {hv_err}. Direct API check...")
        try:
            holdings_verified = _direct_api_holdings_check(position, clean_sym)
        except Exception:
            pass
    
    if not holdings_verified:
        logger.warning(f"🚫 [{position.account_type.upper()}] {position.symbol} NO holdings found at broker. "
                      f"Position already closed. Deactivating trailing stop.")
        position.is_triggered = True
        position.triggered_at = datetime.utcnow()
        position.trigger_reason = f"Deactivated: no holdings at broker (original: {trigger_reason})"
        position.is_active = False
        db.session.commit()
        return True, {'success': True, 'action': 'deactivated_no_holdings',
                'message': f'{position.symbol} no holdings found, trailing stop deactivated'}
    
    recent_oca_fill = OCAGroup.query.filter(
        OCAGroup.trailing_stop_id == position.id,
        OCAGroup.account_type == position.account_type,
        OCAGroup.status.in_([OCAStatus.TRIGGERED_STOP, OCAStatus.TRIGGERED_TP, OCAStatus.SOFT_STOP])
    ).first()
    if recent_oca_fill:
        logger.info(f"⏸️ [{position.account_type.upper()}] {position.symbol} OCA already triggered "
                   f"(status={recent_oca_fill.status.value}). Skipping software exit.")
        position.is_active = False
        position.is_triggered = True
        position.triggered_at = datetime.utcnow()
        position.triggered_price = current_price
        position.trigger_reason = f"OCA {recent_oca_fill.status.value} already handled"
        db.session.commit()
        return True, {'success': True, 'action': 'oca_already_triggered',
                'message': f'{position.symbol} OCA {recent_oca_fill.status.value}, no software exit needed'}
    
    pending_exit = OrderTracker.query.filter(
        OrderTracker.symbol == position.symbol,
        OrderTracker.account_type == position.account_type,
        OrderTracker.role.in_([OrderRole.EXIT_TRAILING, OrderRole.EXIT_SIGNAL]),
        OrderTracker.status.in_(['PENDING', 'SUBMITTED', 'INITIAL', 'NEW', 'HELD'])
    ).first()
    if pending_exit:
        needs_replace = False
        is_regular = _is_regular_trading_hours_static()

        if is_regular and pending_exit.order_type in ('LIMIT', 'LMT'):
            needs_replace = True
            logger.info(f"[{position.symbol}] 交易时段变为盘中，需将限价退出单转为市价单")
        elif not is_regular and pending_exit.limit_price:
            exit_side_long = pending_exit.side in ('SELL', 'sell')
            if exit_side_long:
                new_limit = round(current_price * 0.998, 2)
            else:
                new_limit = round(current_price * 1.002, 2)
            old_limit = float(pending_exit.limit_price)
            price_diff_pct = abs(new_limit - old_limit) / old_limit * 100 if old_limit > 0 else 100
            if price_diff_pct > 0.3:
                needs_replace = True
                logger.info(f"[{position.symbol}] 限价偏离 {price_diff_pct:.1f}%: old=${old_limit} → new=${new_limit}")

        if needs_replace:
            try:
                if position.account_type == 'paper':
                    cancel_tiger = TigerPaperClient()
                else:
                    cancel_tiger = TigerClient()
                cancel_result = cancel_tiger.cancel_order(pending_exit.tiger_order_id)
                if cancel_result.get('success'):
                    logger.info(f"[{position.symbol}] 取消旧退出订单 {pending_exit.tiger_order_id}")
                    pending_exit.status = 'CANCELLED'
                    db.session.flush()
                else:
                    logger.warning(f"[{position.symbol}] 取消旧退出订单失败: {cancel_result.get('error')}")
                    db.session.commit()
                    return True, {'success': True, 'action': 'cancel_failed_keep_existing',
                            'message': f'{position.symbol} cancel failed, keeping existing exit order'}
            except Exception as cancel_err:
                logger.warning(f"[{position.symbol}] 取消旧退出订单异常: {cancel_err}")
                db.session.commit()
                return True, {'success': True, 'action': 'cancel_failed_keep_existing',
                        'message': f'{position.symbol} cancel error, keeping existing exit order'}
        else:
            logger.debug(f"[{position.symbol}] 现有退出订单价格仍合理，保持不变: {pending_exit.tiger_order_id}")
            db.session.commit()
            return True, {'success': True, 'action': 'exit_price_ok',
                    'message': f'{position.symbol} exit order {pending_exit.tiger_order_id} price still ok'}

    position.triggered_price = current_price
    
    log_trailing_stop_event(position, 'trigger', current_price, trigger_reason)
    
    if position.account_type == 'paper':
        tiger = TigerPaperClient()
    else:
        tiger = TigerClient()
    
    actual_stop_order_id = get_actual_stop_order_id(
        tiger, position.symbol, position.stop_loss_order_id or ''
    )
    cancel_sl_result = safe_cancel_order_for_close(
        tiger_client=tiger,
        order_id=actual_stop_order_id,
        symbol=position.symbol,
        order_type='stop_loss'
    )
    
    if not cancel_sl_result['can_proceed']:
        logger.error(f"🚨 {position.symbol} 止损订单取消失败且仍活跃，需要人工干预")
        
        try:
            from discord_notifier import discord_notifier
            discord_notifier.send_notification(
                f"🚨 **紧急: 平仓取消订单失败**\n"
                f"股票: {position.symbol}\n"
                f"止损订单ID: {actual_stop_order_id}\n"
                f"状态: {cancel_sl_result.get('message')}\n"
                f"⚠️ 需要手动取消订单后再平仓！",
                title="紧急警报"
            )
        except Exception as e:
            logger.warning(f"发送紧急通知失败: {e}")
        
        position.trigger_retry_count = (position.trigger_retry_count or 0) + 1
        if position.trigger_retry_count >= 3:
            logger.warning(f"⚠️ {position.symbol} 取消重试超过3次，再次验证状态后决定是否强制平仓")
            should_force_close = True
            if hasattr(tiger, 'get_order_status') and actual_stop_order_id:
                try:
                    final_status = tiger.get_order_status(actual_stop_order_id)
                    status = final_status.get('status', 'UNKNOWN')
                    if status in ['FILLED']:
                        logger.error(f"🚨 {position.symbol} 止损订单已成交，取消平仓避免重复")
                        position.is_triggered = True
                        position.trigger_reason = "止损订单已成交"
                        position.trigger_retry_count = 0
                        db.session.commit()
                        return True, {'action': 'stop_filled', 'message': '止损订单已成交'}
                    elif status in ['ACTIVE', 'NEW', 'HELD', 'PENDING', 'SUBMITTED']:
                        logger.error(f"🚨 {position.symbol} 止损订单仍活跃({status})，需要人工干预")
                        should_force_close = False
                        try:
                            from discord_notifier import discord_notifier
                            discord_notifier.send_notification(
                                f"🚨🚨🚨 **紧急: 需要人工干预**\n"
                                f"股票: {position.symbol}\n"
                                f"止损订单ID: {actual_stop_order_id}\n"
                                f"状态: {status}\n"
                                f"⚠️ 3次取消重试后订单仍活跃！\n"
                                f"请手动取消订单或检查账户！",
                                title="需要人工干预"
                            )
                        except Exception as e:
                            logger.warning(f"发送紧急通知失败: {e}")
                        result['action'] = 'manual_intervention_required'
                        result['message'] = f"止损订单仍活跃({status})，需要人工干预"
                        position.is_active = True
                        db.session.commit()
                        return True, result
                except Exception as e:
                    logger.warning(f"⚠️ 最终状态检查失败: {e}")
                    should_force_close = False
                    try:
                        from discord_notifier import discord_notifier
                        discord_notifier.send_notification(
                            f"🚨🚨🚨 **紧急: 状态检查失败**\n"
                            f"股票: {position.symbol}\n"
                            f"无法确认订单状态\n"
                            f"⚠️ 请手动检查账户！",
                            title="需要人工干预"
                        )
                    except:
                        pass
                    result['action'] = 'manual_intervention_required'
                    result['message'] = f"无法确认订单状态，需要人工干预"
                    position.is_active = True
                    db.session.commit()
                    return True, result
            
            if should_force_close:
                try:
                    from discord_notifier import discord_notifier
                    discord_notifier.send_notification(
                        f"🚨🚨 **高严重性: 强制平仓**\n"
                        f"股票: {position.symbol}\n"
                        f"止损订单ID: {actual_stop_order_id}\n"
                        f"警告: 已尝试3次取消失败，强制继续平仓！\n"
                        f"请立即检查是否有重复订单！",
                        title="高严重性警报"
                    )
                except Exception as e:
                    logger.warning(f"发送高严重性通知失败: {e}")
        else:
            result['action'] = 'defer_close'
            result['message'] = f"止损订单取消失败 (重试 {position.trigger_retry_count}/3): {cancel_sl_result.get('message')}"
            position.is_active = True
            db.session.commit()
            return True, result
    else:
        if position.trigger_retry_count and position.trigger_retry_count > 0:
            position.trigger_retry_count = 0
    
    actual_tp_order_id = get_actual_take_profit_order_id(
        tiger, position.symbol, position.take_profit_order_id or ''
    )
    cancel_tp_result = safe_cancel_order_for_close(
        tiger_client=tiger,
        order_id=actual_tp_order_id,
        symbol=position.symbol,
        order_type='take_profit'
    )
    
    if not cancel_tp_result['success']:
        logger.warning(f"⚠️ {position.symbol} 止盈订单取消失败，继续平仓: {cancel_tp_result.get('message')}")
    
    close_side = 'SELL' if position.side == 'long' else 'BUY'
    clean_symbol = position.symbol.replace('[PAPER]', '').strip()
    
    close_order_result = execute_trailing_stop_close(
        tiger=tiger,
        symbol=clean_symbol,
        side=close_side,
        quantity=position.quantity,
        account_type=position.account_type,
        trigger_price=current_price,
        entry_price=position.entry_price,
        profit_pct=position.current_profit_pct,
        timeframe=position.timeframe or '15'
    )
    
    result['close_order'] = close_order_result
    
    if close_order_result.get('success'):
        position.trigger_retry_count = 0
        
        close_order_id = None
        tiger_order_info = close_order_result.get('tiger_order')
        if tiger_order_info:
            close_order_id = tiger_order_info.get('order_id')
        
        if close_order_id:
            try:
                from order_tracker_service import register_order
                register_order(
                    tiger_order_id=str(close_order_id),
                    symbol=clean_symbol,
                    account_type=position.account_type,
                    role='exit_trailing',
                    side=close_side,
                    quantity=position.quantity,
                    order_type='LIMIT',
                    trailing_stop_id=position.id,
                    trade_id=position.trade_id,
                )
                logger.info(f"📋 Registered trailing stop close order {close_order_id} as EXIT_TRAILING")
            except Exception as reg_err:
                logger.error(f"❌ Failed to register trailing stop order: {reg_err}")
        else:
            logger.warning(f"⚠️ Trailing stop close order placed but no order_id returned")
        
        from discord_notifier import send_trailing_stop_notification
        send_trailing_stop_notification(
            position.symbol,
            'trigger',
            current_price,
            position.entry_price,
            position.current_profit_pct,
            trigger_reason
        )
        
        position.trigger_reason = f"pending_exit:{close_order_id or 'unknown'}"
        position.triggered_at = datetime.utcnow()
        logger.info(f"[{position.symbol}] TS stays ACTIVE with pending exit order {close_order_id}, will re-check each cycle")

        result['action'] = 'trigger'
        result['message'] = trigger_reason
        result['close_side'] = close_side
        result['success'] = True
        db.session.commit()
        
        return True, result
    else:
        MAX_EXIT_RETRIES = 5
        position.trigger_retry_count = (position.trigger_retry_count or 0) + 1
        retry_count = position.trigger_retry_count
        fail_msg = close_order_result.get('message', 'unknown error')
        
        logger.error(f"🚨 [{position.account_type.upper()}] {position.symbol} exit order FAILED "
                     f"(attempt {retry_count}/{MAX_EXIT_RETRIES}): {fail_msg}")
        
        try:
            filled_exit = OrderTracker.query.filter(
                OrderTracker.symbol.in_([position.symbol, clean_symbol]),
                OrderTracker.account_type == position.account_type,
                OrderTracker.role.in_([OrderRole.EXIT_TRAILING, OrderRole.EXIT_SIGNAL]),
                OrderTracker.status == 'FILLED'
            ).order_by(OrderTracker.created_at.desc()).first()
            if filled_exit and filled_exit.created_at and filled_exit.created_at >= (position.created_at or datetime.min):
                logger.info(f"✅ {position.symbol} exit already FILLED via order {filled_exit.tiger_order_id}, "
                           f"no retry needed. Deactivating.")
                position.is_active = False
                position.is_triggered = True
                position.triggered_at = datetime.utcnow()
                position.trigger_reason = f"Exit already filled (order {filled_exit.tiger_order_id})"
                position.trigger_retry_count = 0
                db.session.commit()
                return True, {'success': True, 'action': 'exit_already_filled',
                        'message': f'{position.symbol} exit order {filled_exit.tiger_order_id} already filled'}
        except Exception as check_err:
            logger.warning(f"⚠️ Error checking filled exit orders for {position.symbol}: {check_err}")
        
        if retry_count < MAX_EXIT_RETRIES:
            RETRY_COOLDOWN_SECONDS = 60
            position.is_active = True
            position.is_triggered = False
            position.triggered_at = datetime.utcnow()
            position.triggered_price = None
            position.trigger_reason = None
            
            logger.info(f"🔄 {position.symbol} reactivated for retry (attempt {retry_count}/{MAX_EXIT_RETRIES}), "
                        f"cooldown {RETRY_COOLDOWN_SECONDS}s before next attempt")
            
            try:
                from discord_notifier import discord_notifier
                discord_notifier.send_notification(
                    f"🚨 **出场订单失败 (重试 {retry_count}/{MAX_EXIT_RETRIES})**\n"
                    f"股票: {position.symbol}\n"
                    f"账户: {position.account_type}\n"
                    f"错误: {fail_msg}\n"
                    f"将在{RETRY_COOLDOWN_SECONDS}秒后自动重试",
                    title="出场订单失败"
                )
            except Exception:
                pass
            
            db.session.commit()
            return True, {'success': False, 'action': 'exit_failed_will_retry',
                    'message': f'{position.symbol} exit order failed ({fail_msg}), retry {retry_count}/{MAX_EXIT_RETRIES}'}
        else:
            logger.error(f"🚨🚨 [{position.account_type.upper()}] {position.symbol} exit order failed "
                        f"{MAX_EXIT_RETRIES} times! Deactivating trailing stop permanently.")
            
            position.is_active = False
            position.is_triggered = True
            position.triggered_at = datetime.utcnow()
            position.triggered_price = current_price
            position.trigger_reason = f"Exit order failed {MAX_EXIT_RETRIES} times: {fail_msg}"
            
            try:
                from discord_notifier import discord_notifier
                discord_notifier.send_notification(
                    f"🚨🚨🚨 **出场订单彻底失败**\n"
                    f"股票: {position.symbol}\n"
                    f"账户: {position.account_type}\n"
                    f"已重试 {MAX_EXIT_RETRIES} 次均失败\n"
                    f"最后错误: {fail_msg}\n"
                    f"⚠️ 仓位可能仍在券商，需要手动处理！",
                    title="紧急: 出场订单彻底失败"
                )
            except Exception:
                pass
            
            db.session.commit()
            return True, {'success': False, 'action': 'exit_failed_max_retries',
                    'message': f'{position.symbol} exit order failed {MAX_EXIT_RETRIES} times, deactivated'}


def process_trailing_stop_check(position: TrailingStopPosition, tiger_positions: Dict = None) -> Dict:
    from tiger_client import get_tiger_quote_client, TigerClient, TigerPaperClient
    
    result = {
        'success': False,
        'position_id': position.id,
        'symbol': position.symbol,
        'action': None,
        'message': ''
    }
    
    try:
        config = get_trailing_stop_config()
        
        if not config.is_enabled:
            result['message'] = "Trailing stop system is disabled"
            return result
        
        RETRY_COOLDOWN_SECONDS = 60
        if (position.trigger_retry_count or 0) > 0 and position.triggered_at:
            seconds_since_last_fail = (datetime.utcnow() - position.triggered_at).total_seconds()
            if seconds_since_last_fail < RETRY_COOLDOWN_SECONDS:
                remaining = RETRY_COOLDOWN_SECONDS - seconds_since_last_fail
                result['message'] = f"Retry cooldown: {remaining:.0f}s remaining (attempt {position.trigger_retry_count})"
                result['success'] = True
                logger.debug(f"⏳ {position.symbol} retry cooldown: {remaining:.0f}s remaining "
                           f"(attempt {position.trigger_retry_count}, last fail {seconds_since_last_fail:.0f}s ago)")
                return result
            else:
                logger.info(f"🔄 {position.symbol} retry cooldown expired, proceeding with attempt {position.trigger_retry_count + 1}")
        
        clean_symbol = position.symbol.replace('[PAPER]', '').strip()
        
        if tiger_positions:
            account_positions = tiger_positions.get(position.account_type, {})
            tiger_pos = account_positions.get(clean_symbol)
            
            if tiger_pos:
                new_avg_cost = tiger_pos.get('average_cost')
                raw_quantity = tiger_pos.get('quantity')
                new_quantity = abs(raw_quantity) if raw_quantity else None
                
                if new_avg_cost and new_avg_cost != position.entry_price:
                    logger.info(f"📊 Syncing avg cost for {position.symbol}: {position.entry_price:.2f} -> {new_avg_cost:.2f}")
                    position.entry_price = new_avg_cost
                
                if new_quantity and new_quantity != position.quantity:
                    logger.info(f"📊 Syncing quantity for {position.symbol}: {position.quantity} -> {new_quantity}")
                    position.quantity = new_quantity
        
        quote_client = get_tiger_quote_client()
        
        # 优先使用WebSocket推送价格，fallback到API调用
        trade_data = get_realtime_price_with_websocket_fallback(clean_symbol, quote_client)
        if not trade_data:
            result['message'] = f"Could not get price for {position.symbol}"
            return result
        
        current_price = trade_data['price']
        price_session = trade_data.get('session', 'unknown')
        price_source = trade_data.get('source', 'websocket')
        
        if price_source == 'websocket':
            logger.debug(f"📊 {clean_symbol} WebSocket价格: ${current_price:.2f} (session: {price_session})")
        elif price_session in ['pre_market', 'post_market']:
            logger.debug(f"📊 {clean_symbol} 使用{price_session}价格: ${current_price:.2f} (source: {price_source})")
        
        from atr_cache_service import get_atr_and_bars, resolve_timeframe
        timeframe_str = resolve_timeframe(position.timeframe)
        
        atr, bars = get_atr_and_bars(clean_symbol, position.timeframe, config.atr_period)
        
        if atr == 0 or not bars or len(bars) < config.atr_period:
            logger.warning(f"Insufficient data for ATR calculation for {position.symbol}, skipping trailing stop update but still checking stops")
            position.last_check_at = datetime.utcnow()
            is_triggered, trigger_reason = check_stop_triggered(position, current_price)
            if is_triggered:
                logger.warning(f"🚨 [{position.account_type.upper()}] {position.symbol} stop TRIGGERED despite no ATR data: {trigger_reason}")
                should_return, exit_result = _handle_stop_trigger_and_exit(position, current_price, trigger_reason, result)
                if should_return:
                    return exit_result
                result = exit_result
            else:
                if position.breach_detected_at is not None:
                    logger.info(f"✅ [{position.account_type.upper()}] {position.symbol} price recovered above stop (no ATR), "
                               f"clearing breach state (was breached at ${position.breach_price:.2f})")
                    position.breach_detected_at = None
                    position.breach_price = None
                if position.trigger_reason and position.trigger_reason.startswith('pending_exit:'):
                    _cancel_pending_exit_on_recovery_tiger(position, current_price)
            db.session.commit()
            result['message'] = "Insufficient data for ATR calculation, stop check performed"
            result['success'] = True
            return result
        
        trend_data = calculate_trend_strength(
            bars, current_price, position.entry_price, 
            position.side, atr, config
        )
        position.trend_strength = trend_data['trend_strength']
        position.atr_convergence = trend_data['atr_convergence']
        position.momentum_score = trend_data['momentum_score']
        position.consecutive_highs = trend_data['consecutive_highs']
        
        logger.info(f"[{position.symbol}] 趋势强度: {trend_data['trend_strength']:.1f} "
                   f"(ATR收敛: {trend_data['atr_convergence']:.2f}, "
                   f"动量: {trend_data['momentum_score']:.2f}, "
                   f"连续创新高: {trend_data['consecutive_highs']})")
        
        should_switch, switch_reason = check_switch_condition(
            position, current_price, config, trend_data['trend_strength']
        )
        
        if should_switch:
            position.has_switched_to_trailing = True
            position.switch_triggered_at = datetime.utcnow()
            position.switch_reason = switch_reason
            
            # 取消止盈订单 - 即使order_id为空也尝试查询获取
            try:
                if position.account_type == 'paper':
                    tiger = TigerPaperClient()
                else:
                    tiger = TigerClient()
                
                # 查询实际的止盈订单ID（即使stored_order_id为空也尝试查询）
                actual_tp_order_id = get_actual_take_profit_order_id(
                    tiger, position.symbol, position.take_profit_order_id or ''
                )
                
                if actual_tp_order_id:
                    if actual_tp_order_id != position.take_profit_order_id:
                        logger.info(f"📋 {position.symbol} 发现止盈订单ID: {actual_tp_order_id}")
                        position.take_profit_order_id = actual_tp_order_id
                    
                    cancel_result = tiger.cancel_order(actual_tp_order_id)
                    if cancel_result.get('success'):
                        logger.info(f"✅ Cancelled take profit order {actual_tp_order_id} for {position.symbol}")
                        position.take_profit_order_id = None
                    else:
                        logger.warning(f"⚠️ Failed to cancel TP order {actual_tp_order_id}: {cancel_result.get('error')}")
                else:
                    logger.info(f"📋 {position.symbol} 没有找到止盈订单，可能已取消或不存在")
            except Exception as e:
                logger.error(f"Error cancelling take profit order: {str(e)}")
            
            # 止损已通过Tiger API修改完成，无需发送外部webhook
            
            log_trailing_stop_event(position, 'switch', current_price, switch_reason)
            
            from discord_notifier import send_trailing_stop_notification
            send_trailing_stop_notification(
                position.symbol,
                'switch',
                current_price,
                position.entry_price,
                position.current_profit_pct,
                switch_reason
            )
            
            result['action'] = 'switch'
            result['message'] = switch_reason
        
        # 阶梯止损调整 (未切换到动态trailing时执行)
        if not position.has_switched_to_trailing:
            prog_result = check_and_adjust_progressive_stop(position, current_price, config)
            
            if prog_result['should_adjust']:
                exec_result = execute_progressive_stop_adjustment(
                    position=position,
                    new_stop_price=prog_result['new_stop_price'],
                    new_tier=prog_result['new_tier'],
                    account_type=position.account_type
                )
                
                if exec_result['success']:
                    result['action'] = 'progressive_adjust'
                    result['message'] = prog_result['reason']
                    result['new_tier'] = prog_result['new_tier']
                else:
                    logger.error(f"阶梯止损调整失败: {exec_result.get('message')}")
        
        new_stop, calc_details = calculate_trailing_stop(position, current_price, atr, config)
        
        inverse_stop, inverse_details = calculate_inverse_protection_stop(
            position, current_price, trend_data['trend_strength'], config
        )
        
        if inverse_stop is not None and inverse_details.get('action') == 'tighten':
            is_long = position.side == 'long'
            if is_long:
                if inverse_stop > new_stop:
                    new_stop = inverse_stop
            else:
                if inverse_stop < new_stop:
                    new_stop = inverse_stop
            
            loss_pct = inverse_details['current_loss'] / position.entry_price * 100
            
            logger.info(f"🛡️ [{position.symbol}] 反向保护触发: 亏损{loss_pct:.2f}%, "
                       f"趋势强度{trend_data['trend_strength']:.0f}, "
                       f"止损从${position.fixed_stop_loss:.2f}收紧至${inverse_stop:.2f} "
                       f"(收紧系数{inverse_details['tightening_factor']})")
            
            if position.current_trailing_stop is None or \
               (is_long and inverse_stop > position.current_trailing_stop) or \
               (not is_long and inverse_stop < position.current_trailing_stop):
                
                # 通过Tiger API修改止损订单 - 即使order_id为空也尝试查询获取
                if position.account_type == 'paper':
                    tiger_client = TigerPaperClient()
                else:
                    tiger_client = TigerClient()
                
                side = 'sell' if position.side == 'long' else 'buy'
                clean_symbol = position.symbol.replace('[PAPER]', '').strip()
                
                # 查询实际的止损订单ID（即使stored_order_id为空也尝试查询）
                actual_stop_order_id = get_actual_stop_order_id(
                    tiger_client, position.symbol, position.stop_loss_order_id or ''
                )
                
                if actual_stop_order_id:
                    if actual_stop_order_id != position.stop_loss_order_id:
                        logger.info(f"📋 {position.symbol} 发现止损订单ID: {actual_stop_order_id}")
                        old_discovered_id = position.stop_loss_order_id
                        position.stop_loss_order_id = actual_stop_order_id
                        sync_stop_loss_order_to_trade(position, actual_stop_order_id, position.fixed_stop_loss or position.entry_price, old_discovered_id, commit=False, create_tracker=False)
                    
                    # 使用带重试的修改函数（漏洞2修复）
                    modify_result = modify_stop_with_retry(
                        tiger_client=tiger_client,
                        position=position,
                        actual_stop_order_id=actual_stop_order_id,
                        new_stop_price=inverse_stop,
                        side=side,
                        max_retries=3
                    )
                    
                    if modify_result['success']:
                        logger.info(f"📈 [{position.account_type.upper()}] {position.symbol} 反向保护止损修改成功: ${inverse_stop:.2f}, 新订单ID={modify_result.get('new_order_id')}")
                    else:
                        logger.error(f"❌ {position.symbol} 反向保护止损修改失败: {modify_result.get('message', 'Unknown error')}")
                else:
                    # 漏洞1修复：找不到止损订单时自动创建
                    recover_result = create_or_recover_stop_order(
                        tiger_client=tiger_client,
                        position=position,
                        stop_price=inverse_stop,
                        side=side
                    )
                    if recover_result['success']:
                        logger.info(f"✅ {position.symbol} 反向保护止损订单已恢复: {recover_result.get('order_id')}")
                    else:
                        logger.error(f"❌ {position.symbol} 反向保护止损订单恢复失败: {recover_result.get('message')}")
                
                # 止损已通过Tiger API修改完成，无需发送外部webhook
                
                log_trailing_stop_event(
                    position, 
                    'inverse_protection', 
                    current_price, 
                    f"反向保护: 止损收紧至${inverse_stop:.2f} (趋势{trend_data['trend_strength']:.0f})"
                )
                
                from discord_notifier import send_trailing_stop_notification
                send_trailing_stop_notification(
                    position.symbol,
                    'inverse_protection',
                    current_price,
                    position.entry_price,
                    position.current_profit_pct,
                    f"反向保护触发: 止损收紧至${inverse_stop:.2f}"
                )
                
                position.stop_adjustment_count = (position.stop_adjustment_count or 0) + 1
                position.last_stop_adjustment_price = current_price
        
        position.previous_trailing_stop = position.current_trailing_stop
        position.current_trailing_stop = new_stop
        position.current_price = current_price  # Update with WebSocket/API price
        position.last_check_at = datetime.utcnow()
        
        # 动态trailing阶段：如果止损价格有变化且已切换，更新Tiger止损订单
        # 即使stop_loss_order_id为空也尝试查询获取实际的订单ID
        if position.has_switched_to_trailing:
            prev_stop = position.previous_trailing_stop
            if prev_stop is None or \
               (position.side == 'long' and new_stop > prev_stop) or \
               (position.side == 'short' and new_stop < prev_stop):
                
                if position.account_type == 'paper':
                    tiger_client = TigerPaperClient()
                else:
                    tiger_client = TigerClient()
                
                side = 'sell' if position.side == 'long' else 'buy'
                clean_symbol = position.symbol.replace('[PAPER]', '').strip()
                
                # 查询实际的止损订单ID（即使stored_order_id为空也尝试查询）
                actual_stop_order_id = get_actual_stop_order_id(
                    tiger_client, position.symbol, position.stop_loss_order_id or ''
                )
                
                if not actual_stop_order_id:
                    # 漏洞1修复：找不到止损订单时自动创建
                    recover_result = create_or_recover_stop_order(
                        tiger_client=tiger_client,
                        position=position,
                        stop_price=new_stop,
                        side=side
                    )
                    if recover_result['success']:
                        logger.info(f"✅ {position.symbol} 动态trailing止损订单已恢复: {recover_result.get('order_id')}")
                    else:
                        logger.error(f"❌ {position.symbol} 动态trailing止损订单恢复失败: {recover_result.get('message')}")
                else:
                    if actual_stop_order_id != position.stop_loss_order_id:
                        logger.info(f"📋 {position.symbol} 发现止损订单ID: {actual_stop_order_id}")
                        old_discovered_id = position.stop_loss_order_id
                        position.stop_loss_order_id = actual_stop_order_id
                        sync_stop_loss_order_to_trade(position, actual_stop_order_id, position.fixed_stop_loss or position.entry_price, old_discovered_id, commit=False, create_tracker=False)
                    
                    # 使用带重试的修改函数（漏洞2修复）
                    prev_stop_str = f"${prev_stop:.2f}" if prev_stop else "N/A"
                    modify_result = modify_stop_with_retry(
                        tiger_client=tiger_client,
                        position=position,
                        actual_stop_order_id=actual_stop_order_id,
                        new_stop_price=new_stop,
                        side=side,
                        max_retries=3
                    )
                    
                    if modify_result['success']:
                        logger.info(f"📈 [{position.account_type.upper()}] {position.symbol} 动态trailing止损上移: {prev_stop_str} -> ${new_stop:.2f}, 新订单ID={modify_result.get('new_order_id')}")
                    else:
                        logger.error(f"❌ {position.symbol} 动态trailing止损修改失败: {modify_result.get('message', 'Unknown error')}")
        
        is_triggered, trigger_reason = check_stop_triggered(position, current_price)
        
        if is_triggered:
            should_return, exit_result = _handle_stop_trigger_and_exit(position, current_price, trigger_reason, result)
            if should_return:
                return exit_result
            result = exit_result
        else:
            if position.breach_detected_at is not None:
                logger.info(f"✅ [{position.account_type.upper()}] {position.symbol} price recovered above stop, "
                           f"clearing breach state (was breached at ${position.breach_price:.2f})")
                position.breach_detected_at = None
                position.breach_price = None

            if position.trigger_reason and position.trigger_reason.startswith('pending_exit:'):
                _cancel_pending_exit_on_recovery_tiger(position, current_price)

            log_trailing_stop_event(position, 'check', current_price, f"Stop: ${new_stop:.2f}")
        
        db.session.commit()
        result['success'] = True
        
        result['current_price'] = current_price
        result['trailing_stop'] = new_stop
        result['profit_pct'] = position.current_profit_pct
        result['atr'] = atr
        
        return result
        
    except Exception as e:
        logger.error(f"Error processing trailing stop for {position.symbol}: {str(e)}")
        result['message'] = str(e)
        return result


def deactivate_trailing_stop_for_symbol(symbol: str, account_type: str, reason: str):
    clean_symbol = symbol.replace('[PAPER]', '').strip()
    active_ts = TrailingStopPosition.query.filter_by(
        is_active=True, account_type=account_type
    ).all()
    for ts in active_ts:
        ts_clean = ts.symbol.replace('[PAPER]', '').strip()
        if ts_clean == clean_symbol:
            ts.is_active = False
            ts.trigger_reason = reason
            ts.triggered_at = datetime.utcnow()
            db.session.commit()
            logger.info(f"🛑 Deactivated trailing stop #{ts.id} for {symbol} ({account_type}): {reason}")
            return True
    return False


def check_and_deactivate_closed_positions() -> List[Dict]:
    """
    检查Tiger持仓数据（API → WebSocket → DB三级回退），
    如果追踪列表中的仓位已不存在，自动停用追踪。
    不再单独调用API，使用get_cached_tiger_positions统一管理配额。
    
    Returns:
        停用的仓位列表
    """
    ANOMALY_THRESHOLD = 3
    
    deactivated = []
    
    active_positions = TrailingStopPosition.query.filter_by(is_active=True).all()
    if not active_positions:
        return deactivated
    
    tiger_positions = get_cached_tiger_positions(force_refresh=False)
    
    real_active_count = sum(1 for p in active_positions if p.account_type == 'real')
    paper_active_count = sum(1 for p in active_positions if p.account_type == 'paper')
    
    GRACE_PERIOD_SECONDS = 60
    
    for pos in active_positions:
        clean_symbol = pos.symbol.replace('[PAPER]', '').strip()
        
        if pos.created_at and (datetime.utcnow() - pos.created_at).total_seconds() < GRACE_PERIOD_SECONDS:
            logger.debug(f"⏳ Skipping deactivation check for {pos.symbol}: created {(datetime.utcnow() - pos.created_at).total_seconds():.0f}s ago (grace period)")
            continue
        
        api_success = tiger_positions.get(f'{pos.account_type}_success', False)
        if not api_success:
            logger.warning(f"⚠️ Skipping deactivation check for {pos.symbol} ({pos.account_type}): no reliable position data")
            continue
        
        account_positions = tiger_positions.get(pos.account_type, {})
        active_count = real_active_count if pos.account_type == 'real' else paper_active_count
        
        if not account_positions and active_count >= ANOMALY_THRESHOLD:
            logger.warning(f"⚠️ Empty positions for {pos.account_type} with {active_count} active - possible anomaly, skipping")
            continue
        
        if clean_symbol not in account_positions:
            pos.is_active = False
            pos.trigger_reason = "Position closed externally (not in Tiger positions)"
            pos.triggered_at = datetime.utcnow()
            deactivated.append({
                'symbol': pos.symbol,
                'account_type': pos.account_type,
                'reason': 'Position not found in Tiger positions'
            })
            logger.info(f"🛑 Auto-deactivated {pos.symbol} ({pos.account_type}): position closed externally")
    
    if deactivated:
        db.session.commit()
    
    return deactivated


_cached_positions = {'real': {}, 'paper': {}, 'timestamp': None, 'real_success': False, 'paper_success': False}

def _get_holdings_from_db(account_type: str) -> Dict:
    """Fallback: get positions from TigerHolding database table."""
    try:
        from models import TigerHolding
        holdings = TigerHolding.query.filter(
            TigerHolding.account_type == account_type,
            TigerHolding.quantity != 0
        ).all()
        result = {}
        for h in holdings:
            result[h.symbol] = {
                'symbol': h.symbol,
                'quantity': h.quantity,
                'average_cost': h.average_cost,
                'market_value': h.market_value,
                'unrealized_pnl': h.unrealized_pnl,
                'latest_price': h.latest_price,
            }
        return result
    except Exception as e:
        logger.debug(f"Could not get holdings from DB for {account_type}: {e}")
        return {}


def _try_fallback_positions(account_type: str) -> tuple:
    """Try WebSocket cache then DB holdings as fallback for position data.
    Returns (positions_dict, success_bool, source_name)."""
    from push_event_handlers import get_all_cached_positions
    
    ws_positions = get_all_cached_positions(account_type, max_age_seconds=120)
    if ws_positions:
        filtered = {s: p for s, p in ws_positions.items() if p.get('quantity', 0) != 0}
        logger.info(f"⚡ {account_type}: using WebSocket cache ({len(filtered)} positions)")
        return filtered, True, 'websocket'
    
    db_positions = _get_holdings_from_db(account_type)
    if db_positions:
        logger.info(f"📦 {account_type}: using DB holdings fallback ({len(db_positions)} positions)")
        return db_positions, True, 'database'
    
    return {}, False, 'none'


def get_cached_tiger_positions(force_refresh=False) -> Dict:
    """
    获取Tiger持仓数据，带缓存（30秒有效期）避免频繁API调用
    
    Three-tier data source: Tiger API → WebSocket cache (120s) → TigerHolding DB
    This ensures position deactivation works even when API is rate-limited.
    
    Returns dict with:
    - 'real': dict of symbol -> position for real account
    - 'paper': dict of symbol -> position for paper account
    - 'real_success': True if data source (API, WebSocket, or DB) is reliable
    - 'paper_success': True if data source (API, WebSocket, or DB) is reliable
    - 'timestamp': last successful fetch time
    """
    from tiger_client import TigerClient, TigerPaperClient
    import time
    
    global _cached_positions
    
    current_time = time.time()
    cache_valid = _cached_positions['timestamp'] and (current_time - _cached_positions['timestamp'] < 30)
    
    if cache_valid and not force_refresh:
        return _cached_positions
    
    real_success = False
    paper_success = False
    
    try:
        tiger = TigerClient()
        result = tiger.get_positions()
        if result.get('success'):
            _cached_positions['real'] = {p['symbol']: p for p in result.get('positions', [])}
            real_success = True
            logger.debug(f"✅ Real account: fetched {len(_cached_positions['real'])} positions from API")
        else:
            _cached_positions['real'], real_success, source = _try_fallback_positions('real')
            if real_success:
                logger.info(f"⚡ Real account: API failed, using {source} fallback")
    except Exception as e:
        logger.warning(f"❌ Failed to get real positions: {e}")
        _cached_positions['real'], real_success, source = _try_fallback_positions('real')
        if real_success:
            logger.info(f"⚡ Real account: API exception, using {source} fallback")
    
    try:
        tiger_paper = TigerPaperClient()
        result = tiger_paper.get_positions()
        if result.get('success'):
            _cached_positions['paper'] = {p['symbol']: p for p in result.get('positions', [])}
            paper_success = True
            logger.debug(f"✅ Paper account: fetched {len(_cached_positions['paper'])} positions from API")
        else:
            _cached_positions['paper'], paper_success, source = _try_fallback_positions('paper')
            if paper_success:
                logger.info(f"⚡ Paper account: API failed, using {source} fallback")
    except Exception as e:
        logger.warning(f"❌ Failed to get paper positions: {e}")
        _cached_positions['paper'], paper_success, source = _try_fallback_positions('paper')
        if paper_success:
            logger.info(f"⚡ Paper account: API exception, using {source} fallback")
    
    _cached_positions['real_success'] = real_success
    _cached_positions['paper_success'] = paper_success
    _cached_positions['timestamp'] = current_time
    return _cached_positions


def sync_trailing_stop_from_holdings() -> int:
    """Sync entry_price and quantity from TigerHolding table to active TrailingStopPositions.
    Uses locally cached TigerHolding data (synced by holdings_sync independently).
    Safe to call anytime - no Tiger API calls, only database reads.
    Returns number of positions updated.
    """
    from models import TigerHolding
    
    updated_count = 0
    active_positions = TrailingStopPosition.query.filter_by(is_active=True).all()
    
    if not active_positions:
        return 0
    
    for pos in active_positions:
        clean_symbol = pos.symbol.replace('[PAPER]', '').strip()
        
        holding = TigerHolding.query.filter_by(
            symbol=clean_symbol,
            account_type=pos.account_type
        ).first()
        
        if not holding:
            continue
        
        changed = False
        
        if holding.average_cost and holding.average_cost > 0 and holding.average_cost != pos.entry_price:
            logger.info(f"📊 Holdings sync: {pos.symbol} entry_price {pos.entry_price:.4f} -> {holding.average_cost:.4f}")
            pos.entry_price = holding.average_cost
            changed = True
        
        tiger_qty = abs(holding.quantity) if holding.quantity else None
        pos_qty = abs(pos.quantity) if pos.quantity else None
        if tiger_qty and tiger_qty != pos_qty:
            logger.info(f"📊 Holdings sync: {pos.symbol} quantity {pos.quantity} -> {holding.quantity}")
            pos.quantity = abs(holding.quantity)
            changed = True
        
        if holding.latest_price and holding.latest_price > 0 and pos.entry_price and pos.entry_price > 0:
            if pos.side == 'long':
                expected_pct = (holding.latest_price - pos.entry_price) / pos.entry_price
            else:
                expected_pct = (pos.entry_price - holding.latest_price) / pos.entry_price
            if pos.current_profit_pct is not None and abs(pos.current_profit_pct - expected_pct) > 0.01:
                changed = True
        
        if changed and holding.latest_price and holding.latest_price > 0 and pos.entry_price and pos.entry_price > 0:
            if pos.side == 'long':
                new_profit_pct = (holding.latest_price - pos.entry_price) / pos.entry_price
            else:
                new_profit_pct = (pos.entry_price - holding.latest_price) / pos.entry_price
            old_pct = pos.current_profit_pct
            pos.current_profit_pct = new_profit_pct
            logger.info(f"📊 Holdings sync: {pos.symbol} current_profit_pct {old_pct:.4f} -> {new_profit_pct:.4f} (latest_price=${holding.latest_price:.2f})")
            
            if pos.highest_price and pos.side == 'long' and holding.latest_price < pos.highest_price:
                pass
            elif holding.latest_price:
                if pos.highest_price is None or (pos.side == 'long' and holding.latest_price > pos.highest_price):
                    pos.highest_price = holding.latest_price
                if pos.lowest_price is None or (pos.side != 'long' and holding.latest_price < pos.lowest_price):
                    pos.lowest_price = holding.latest_price
        
        if changed:
            updated_count += 1
    
    if updated_count > 0:
        db.session.commit()
        logger.info(f"📊 Holdings sync: updated {updated_count} trailing stop positions")
    
    return updated_count


def process_active_positions_fast() -> List[Dict]:
    """Fast price-only trailing stop check using WebSocket cached prices.
    No Tiger API calls - only uses WebSocket push data and database.
    Called every 5 seconds for real-time monitoring.
    """
    results = []
    
    active_positions = TrailingStopPosition.query.filter_by(is_active=True).all()
    if not active_positions:
        return results
    
    for position in active_positions:
        result = process_trailing_stop_check(position, tiger_positions=None)
        results.append(result)
    
    return results


def process_all_active_positions() -> List[Dict]:
    """Full position check with Tiger API verification.
    Syncs positions with Tiger, detects externally closed positions.
    Called every 60 seconds as a fallback/verification.
    Uses cached positions (caller should force_refresh beforehand).
    """
    tiger_positions = get_cached_tiger_positions(force_refresh=False)
    
    deactivated = []
    active_positions = TrailingStopPosition.query.filter_by(is_active=True).all()
    
    # Count active positions per account type
    real_active_count = sum(1 for p in active_positions if p.account_type == 'real')
    paper_active_count = sum(1 for p in active_positions if p.account_type == 'paper')
    
    # Check API success and empty status
    real_api_success = tiger_positions.get('real_success', False)
    paper_api_success = tiger_positions.get('paper_success', False)
    real_positions_empty = real_api_success and len(tiger_positions.get('real', {})) == 0
    paper_positions_empty = paper_api_success and len(tiger_positions.get('paper', {})) == 0
    
    # IMPROVED LOGIC: Only skip if API returned empty AND we have MORE THAN 2 active positions
    # If only 1-2 active positions and API returns empty, it's likely a real close not an anomaly
    ANOMALY_THRESHOLD = 3  # Only consider it API anomaly if we have 3+ active positions
    
    if real_positions_empty and real_active_count >= ANOMALY_THRESHOLD:
        logger.warning(f"⚠️ Tiger API returned empty real positions but we have {real_active_count} active - possible API anomaly, skipping real deactivation")
    if paper_positions_empty and paper_active_count >= ANOMALY_THRESHOLD:
        logger.warning(f"⚠️ Tiger API returned empty paper positions but we have {paper_active_count} active - possible API anomaly, skipping paper deactivation")
    
    GRACE_PERIOD_SECONDS = 60
    
    for pos in active_positions:
        clean_symbol = pos.symbol.replace('[PAPER]', '').strip()
        
        if pos.created_at and (datetime.utcnow() - pos.created_at).total_seconds() < GRACE_PERIOD_SECONDS:
            logger.debug(f"⏳ Skipping deactivation check for {pos.symbol}: created {(datetime.utcnow() - pos.created_at).total_seconds():.0f}s ago (grace period)")
            continue
        
        if pos.account_type == 'real':
            api_success = real_api_success
        else:
            api_success = paper_api_success
        
        if not api_success:
            logger.warning(f"⚠️ Skipping deactivation check for {pos.symbol} ({pos.account_type}): Tiger API call failed")
            continue
        
        if pos.account_type == 'real' and real_positions_empty and real_active_count >= ANOMALY_THRESHOLD:
            continue
        if pos.account_type == 'paper' and paper_positions_empty and paper_active_count >= ANOMALY_THRESHOLD:
            continue
        
        account_positions = tiger_positions.get(pos.account_type, {})
        
        if clean_symbol not in account_positions:
            pos.is_active = False
            pos.trigger_reason = "Position closed externally (not in Tiger positions)"
            pos.triggered_at = datetime.utcnow()
            deactivated.append({
                'symbol': pos.symbol,
                'account_type': pos.account_type,
                'reason': 'Position not found in Tiger API',
                'entry_price': pos.entry_price,
                'side': pos.side
            })
            logger.info(f"🛑 Auto-deactivated {pos.symbol} ({pos.account_type}): position closed externally")
            
            exit_price = None
            realized_pnl_from_tiger = None
            exit_tiger_order_id = None
            try:
                from models import TigerFilledOrder
                recent_sell = TigerFilledOrder.query.filter(
                    TigerFilledOrder.symbol == clean_symbol,
                    TigerFilledOrder.account_type == pos.account_type,
                    TigerFilledOrder.action == ('SELL' if pos.side == 'long' else 'BUY'),
                    TigerFilledOrder.status == 'Filled'
                ).order_by(TigerFilledOrder.trade_time.desc()).first()
                if recent_sell and recent_sell.avg_fill_price:
                    exit_price = recent_sell.avg_fill_price
                    realized_pnl_from_tiger = recent_sell.realized_pnl
                    exit_tiger_order_id = str(recent_sell.order_id) if recent_sell.order_id else None
                    logger.info(f"📊 Found Tiger filled exit for {clean_symbol}: ${exit_price}, PnL=${realized_pnl_from_tiger}, order={exit_tiger_order_id}")
            except Exception as e:
                logger.debug(f"Could not look up Tiger filled order: {e}")
            
            if not exit_price:
                try:
                    quote_client = get_tiger_quote_client()
                    quote_result = get_realtime_price_with_websocket_fallback(clean_symbol, quote_client)
                    if quote_result:
                        exit_price = quote_result.get('price')
                except Exception:
                    pass
            
            if not exit_price:
                exit_price = pos.entry_price
            
            actual_entry_price = getattr(pos, 'first_entry_price', None) or pos.entry_price
            
            try:
                from models import ExitMethod
                from position_service import find_open_position, add_exit_leg
                from models import PositionStatus as PS
                
                pnl = realized_pnl_from_tiger
                if pnl is None and exit_price and actual_entry_price and pos.quantity:
                    if pos.side == 'long':
                        pnl = (exit_price - actual_entry_price) * pos.quantity
                    else:
                        pnl = (actual_entry_price - exit_price) * pos.quantity
                
                open_pos_obj = find_open_position(pos.symbol, pos.account_type, pos.side)
                if open_pos_obj and open_pos_obj.status == PS.OPEN:
                    add_exit_leg(
                        position=open_pos_obj,
                        tiger_order_id=exit_tiger_order_id,
                        price=exit_price,
                        quantity=pos.quantity,
                        filled_at=datetime.utcnow(),
                        exit_method=ExitMethod.EXTERNAL,
                        realized_pnl=pnl,
                    )
                    pnl_str = f"${pnl:.2f}" if pnl is not None else "N/A"
                    logger.info(f"📊 Added exit leg to Position for external close: {pos.symbol} "
                               f"(exit=${exit_price:.2f}, PnL={pnl_str})")
                elif open_pos_obj:
                    logger.info(f"📊 Position already CLOSED for {pos.symbol}, skipping exit leg")
                else:
                    logger.warning(f"📊 No open Position found for external close: {pos.symbol}/{pos.account_type}")
                    
            except Exception as pos_error:
                logger.error(f"📊 Failed to add exit leg to Position on external exit: {str(pos_error)}")
    
    if deactivated:
        db.session.commit()
    
    results = []
    
    active_positions = TrailingStopPosition.query.filter_by(is_active=True).all()
    
    for position in active_positions:
        result = process_trailing_stop_check(position, tiger_positions)
        results.append(result)
    
    for d in deactivated:
        results.append({
            'symbol': d['symbol'],
            'action': 'deactivate',
            'message': d['reason']
        })
    
    return results


def was_manually_deactivated(symbol: str, account_type: str = None) -> bool:
    """Check if a trailing stop for this symbol was manually deactivated.
    If so, it should never be auto-recreated."""
    clean_symbol = symbol.replace('[PAPER]', '').strip()
    filters = [
        TrailingStopPosition.is_active == False,
        db.or_(
            TrailingStopPosition.symbol == clean_symbol,
            TrailingStopPosition.symbol == f'[PAPER]{clean_symbol}'
        ),
        db.or_(
            TrailingStopPosition.trigger_reason.ilike('%手动停用%'),
            TrailingStopPosition.trigger_reason.ilike('%manually deactivated%'),
            TrailingStopPosition.trigger_reason.ilike('%manual_deactivat%'),
        ),
    ]
    if account_type:
        filters.append(TrailingStopPosition.account_type == account_type)
    
    manual_ts = TrailingStopPosition.query.filter(*filters).order_by(
        TrailingStopPosition.updated_at.desc()
    ).first()
    
    if manual_ts:
        new_entry_ts = TrailingStopPosition.query.filter(
            db.or_(
                TrailingStopPosition.symbol == clean_symbol,
                TrailingStopPosition.symbol == f'[PAPER]{clean_symbol}'
            ),
            TrailingStopPosition.created_at > manual_ts.updated_at,
            TrailingStopPosition.is_active == True,
        )
        if account_type:
            new_entry_ts = new_entry_ts.filter(TrailingStopPosition.account_type == account_type)
        if new_entry_ts.first():
            return False
        
        logger.info(f"⛔ [{clean_symbol}] Was manually deactivated (TS #{manual_ts.id}, reason: {manual_ts.trigger_reason}), blocking auto-recreation")
        return True
    
    return False


def _validate_stop_loss_price(stop_price: float, entry_price: float, side: str, symbol: str) -> float:
    if stop_price is None or entry_price is None or entry_price <= 0:
        return stop_price
    is_long = side.lower() == 'long'
    if is_long and stop_price >= entry_price:
        logger.warning(f"⚠️ [{symbol}] Invalid SL for LONG: SL ${stop_price:.2f} >= entry ${entry_price:.2f}, discarding SL")
        return None
    if not is_long and stop_price <= entry_price:
        logger.warning(f"⚠️ [{symbol}] Invalid SL for SHORT: SL ${stop_price:.2f} <= entry ${entry_price:.2f}, discarding SL")
        return None
    return stop_price


def _validate_take_profit_price(tp_price: float, entry_price: float, side: str, symbol: str) -> float:
    if tp_price is None or entry_price is None or entry_price <= 0:
        return tp_price
    is_long = side.lower() == 'long'
    if is_long and tp_price <= entry_price:
        logger.warning(f"⚠️ [{symbol}] Invalid TP for LONG: TP ${tp_price:.2f} <= entry ${entry_price:.2f}, discarding TP")
        return None
    if not is_long and tp_price >= entry_price:
        logger.warning(f"⚠️ [{symbol}] Invalid TP for SHORT: TP ${tp_price:.2f} >= entry ${entry_price:.2f}, discarding TP")
        return None
    return tp_price


def create_trailing_stop_for_trade(
    trade_id: int,
    symbol: str,
    side: str,
    entry_price: float,
    quantity: float,
    account_type: str = 'real',
    fixed_stop_loss: float = None,
    fixed_take_profit: float = None,
    stop_loss_order_id: str = None,
    take_profit_order_id: str = None,
    mode: TrailingStopMode = TrailingStopMode.BALANCED,
    timeframe: str = '15',
    creation_source: str = None,
    from_reconciliation: bool = False
) -> TrailingStopPosition:
    
    clean_symbol = symbol.replace('[PAPER]', '').strip()
    symbol = clean_symbol
    
    logger.info(f"🔍 [TRACE] create_trailing_stop_for_trade called: symbol={clean_symbol}, quantity={quantity}, side={side}, entry_price={entry_price}, trade_id={trade_id}, account_type={account_type}, SL={fixed_stop_loss}, TP={fixed_take_profit}, sl_order_id={stop_loss_order_id}, tp_order_id={take_profit_order_id}, from_reconciliation={from_reconciliation}, source={creation_source}")
    
    if creation_source in ('holdings_cross_check', 'tiger_reconciliation', 'scheduler_orphan'):
        if was_manually_deactivated(clean_symbol, account_type):
            logger.warning(f"⛔ [{clean_symbol}] Skipping auto TS creation (source={creation_source}): manually deactivated")
            return None
    
    original_sl = fixed_stop_loss
    original_tp = fixed_take_profit
    fixed_stop_loss = _validate_stop_loss_price(fixed_stop_loss, entry_price, side, clean_symbol)
    fixed_take_profit = _validate_take_profit_price(fixed_take_profit, entry_price, side, clean_symbol)
    if fixed_stop_loss is None and original_sl is not None:
        stop_loss_order_id = None
        logger.warning(f"⚠️ [{clean_symbol}] Cleared invalid SL order ID due to bad SL price (original SL=${original_sl})")
    if fixed_take_profit is None and original_tp is not None:
        take_profit_order_id = None
        logger.warning(f"⚠️ [{clean_symbol}] Cleared invalid TP order ID due to bad TP price (original TP=${original_tp})")
    
    if not from_reconciliation:
        any_triggered = TrailingStopPosition.query.filter(
            TrailingStopPosition.account_type == account_type,
            TrailingStopPosition.is_active == False,
            TrailingStopPosition.is_triggered == True,
            db.or_(
                TrailingStopPosition.symbol == clean_symbol,
                TrailingStopPosition.symbol == f'[PAPER]{clean_symbol}'
            )
        ).order_by(TrailingStopPosition.triggered_at.desc()).first()
        
        if any_triggered:
            if any_triggered.triggered_at and any_triggered.triggered_at >= (datetime.utcnow() - timedelta(minutes=10)):
                logger.warning(f"⚠️ Skipping trailing stop creation for {clean_symbol}: "
                              f"recently triggered {(datetime.utcnow() - any_triggered.triggered_at).seconds}s ago "
                              f"(reason: {any_triggered.trigger_reason})")
                return None
            
            from models import OrderTracker, OrderRole
            recent_cutoff = datetime.utcnow() - timedelta(minutes=30)
            pending_close = OrderTracker.query.filter(
                OrderTracker.symbol == clean_symbol,
                OrderTracker.account_type == account_type,
                OrderTracker.role.in_([OrderRole.EXIT_TRAILING, OrderRole.EXIT_SIGNAL]),
                OrderTracker.status.in_(['NEW', 'PENDING', 'SUBMITTED', 'HELD']),
                OrderTracker.created_at >= recent_cutoff,
            ).first()
            
            if pending_close:
                logger.warning(f"⚠️ Skipping trailing stop creation for {clean_symbol}: "
                              f"recent pending exit order exists (order {pending_close.tiger_order_id}, "
                              f"created {pending_close.created_at})")
                return None
            
            from models import Position as PositionModel
            open_position = PositionModel.query.filter(
                PositionModel.symbol == clean_symbol,
                PositionModel.account_type == account_type,
                PositionModel.status == 'OPEN',
            ).first()
            
            if not open_position:
                logger.warning(f"⚠️ Skipping trailing stop creation for {clean_symbol}: "
                              f"previously triggered and no OPEN position found "
                              f"(triggered: {any_triggered.trigger_reason})")
                return None
    else:
        logger.info(f"[{clean_symbol}] from_reconciliation=True: bypassing cooldown/ghost checks")
    
    existing = TrailingStopPosition.query.filter(
        TrailingStopPosition.account_type == account_type,
        TrailingStopPosition.is_active == True,
        db.or_(
            TrailingStopPosition.symbol == clean_symbol,
            TrailingStopPosition.symbol == f'[PAPER]{clean_symbol}'
        )
    ).first()
    
    if existing:
        if existing.symbol != clean_symbol:
            logger.info(f"Normalizing trailing stop symbol: {existing.symbol} → {clean_symbol}")
            existing.symbol = clean_symbol
        existing.entry_price = entry_price
        existing.quantity = quantity
        existing.fixed_stop_loss = fixed_stop_loss
        existing.fixed_take_profit = fixed_take_profit
        if stop_loss_order_id:
            existing.stop_loss_order_id = stop_loss_order_id
        if take_profit_order_id:
            existing.take_profit_order_id = take_profit_order_id
        existing.timeframe = timeframe
        db.session.commit()
        logger.info(f"Updated existing trailing stop for {clean_symbol}")
        
        has_oca = existing.stop_loss_order_id or existing.take_profit_order_id
        needs_oca = fixed_stop_loss or fixed_take_profit
        if needs_oca and not has_oca:
            try:
                from oca_service import create_oca_protection
                oca_result, oca_status = create_oca_protection(
                    trailing_stop_id=existing.id,
                    symbol=clean_symbol,
                    side=side,
                    quantity=quantity,
                    stop_price=fixed_stop_loss,
                    take_profit_price=fixed_take_profit,
                    account_type=account_type,
                    trade_id=trade_id,
                    entry_price=entry_price,
                    creation_source=creation_source or 'ts_creation_auto'
                )
                if oca_result:
                    existing.stop_loss_order_id = oca_result.stop_order_id
                    existing.take_profit_order_id = oca_result.take_profit_order_id
                    db.session.commit()
                    logger.info(f"✅ OCA protection auto-created for existing TS {clean_symbol}: {oca_status}")
                else:
                    logger.warning(f"⚠️ OCA auto-creation failed for existing TS {clean_symbol}: {oca_status}")
            except Exception as oca_err:
                logger.error(f"❌ OCA auto-creation error for existing TS {clean_symbol}: {oca_err}")
        
        return existing
    
    position = TrailingStopPosition(
        symbol=symbol,
        account_type=account_type,
        side=side,
        entry_price=entry_price,
        first_entry_price=entry_price,
        quantity=quantity,
        trade_id=trade_id,
        signal_stop_loss=fixed_stop_loss,
        fixed_stop_loss=fixed_stop_loss,
        fixed_take_profit=fixed_take_profit,
        stop_loss_order_id=stop_loss_order_id,
        take_profit_order_id=take_profit_order_id,
        mode=mode,
        timeframe=timeframe,
        highest_price=entry_price if side == 'long' else None,
        lowest_price=entry_price if side == 'short' else None,
        current_trailing_stop=fixed_stop_loss,
        profit_tier=0,
        is_active=True,
        creation_source=creation_source
    )
    
    db.session.add(position)
    db.session.commit()
    
    logger.info(f"Created trailing stop for {symbol}: entry={entry_price}, SL={fixed_stop_loss}, TP={fixed_take_profit}")
    
    try:
        from tiger_push_client import get_push_manager
        pm = get_push_manager()
        if pm.is_connected:
            pm.subscribe_quotes([clean_symbol])
            logger.info(f"📊 Auto-subscribed {clean_symbol} to WebSocket on trailing stop creation")
    except Exception as sub_err:
        logger.debug(f"Could not auto-subscribe {clean_symbol}: {sub_err}")
    
    if (fixed_stop_loss or fixed_take_profit) and not stop_loss_order_id and not take_profit_order_id:
        try:
            from oca_service import create_oca_protection
            oca_result, oca_status = create_oca_protection(
                trailing_stop_id=position.id,
                symbol=clean_symbol,
                side=side,
                quantity=quantity,
                stop_price=fixed_stop_loss,
                take_profit_price=fixed_take_profit,
                account_type=account_type,
                trade_id=trade_id,
                entry_price=entry_price,
                creation_source=creation_source or 'ts_creation_auto'
            )
            if oca_result:
                position.stop_loss_order_id = oca_result.stop_order_id
                position.take_profit_order_id = oca_result.take_profit_order_id
                db.session.commit()
                logger.info(f"✅ OCA protection auto-created for {clean_symbol}: {oca_status}")
            else:
                logger.warning(f"⚠️ OCA auto-creation failed for {clean_symbol}: {oca_status}")
        except Exception as oca_err:
            logger.error(f"❌ OCA auto-creation error for {clean_symbol}: {oca_err}")
    
    # CRITICAL: Convert attached stop order to standalone STP_LMT for extended hours support
    # Tiger's attached orders (order_leg LOSS) may create:
    # - STP type (pure stop market) - doesn't execute outside RTH
    # - STP_LMT type with outsideRth=false - also doesn't execute outside RTH
    # We need to detect both cases and convert to STP_LMT with outsideRth=True
    if stop_loss_order_id and fixed_stop_loss:
        try:
            from tiger_client import TigerClient, TigerPaperClient
            tiger_client = TigerPaperClient() if account_type == 'paper' else TigerClient()
            if tiger_client:
                order_status = tiger_client.get_order_status(stop_loss_order_id)
                if order_status.get('success'):
                    order_type = order_status.get('order_type', '')
                    outside_rth = order_status.get('outside_rth', False)
                    # Use mapped status (pending/filled/cancelled) not Tiger's original status
                    order_active = order_status.get('status', '') in ['pending', 'partially_filled']
                    
                    logger.info(f"🔍 Stop order {stop_loss_order_id} status check: type={order_type}, outside_rth={outside_rth}, status={order_status.get('status')}, order_active={order_active}")
                    
                    # Need to replace if: STP type OR STP_LMT with outsideRth=false
                    needs_replacement = order_active and (
                        order_type == 'STP' or 
                        (order_type == 'STP_LMT' and not outside_rth)
                    )
                    
                    if needs_replacement:
                        logger.info(f"🔄 Detected stop order {stop_loss_order_id} for {symbol}: type={order_type}, outsideRth={outside_rth}. Converting to STP_LMT with extended hours support...")
                        
                        # Cancel the STP order
                        cancel_result = tiger_client.cancel_order(stop_loss_order_id)
                        if cancel_result.get('success'):
                            logger.info(f"✅ Cancelled STP order {stop_loss_order_id}")
                            
                            # Create new STP_LMT order for regular hours only
                            # TIGER API LIMITATION: STP_LMT does NOT support outside_rth=True
                            # Error: "允许盘前盘后成交订单仅支持限价单" (only LMT orders support extended hours)
                            # TrailingStop system provides extended hours soft protection
                            clean_symbol = symbol.replace('[PAPER]', '').strip()
                            action = 'SELL' if side == 'long' else 'BUY'
                            
                            new_order = tiger_client.place_stop_limit_order(
                                symbol=clean_symbol,
                                action=action,
                                quantity=quantity,
                                stop_price=fixed_stop_loss,
                                limit_price=None,  # Will calculate with slippage
                                outside_rth=False  # STP_LMT only supports regular hours
                            )
                            
                            if new_order.get('success'):
                                new_order_id = new_order.get('order_id')
                                logger.info(f"✅ Created STP_LMT order {new_order_id} for {symbol} (RTH only, extended hours via TrailingStop)")
                                
                                # Update position with new order ID
                                position.stop_loss_order_id = new_order_id
                                
                                # Sync to Trade table and OrderTracker
                                sync_stop_loss_order_to_trade(position, new_order_id, fixed_stop_loss, stop_loss_order_id, commit=False, create_tracker=True)
                                db.session.commit()
                                
                                logger.info(f"📋 Synced new STP_LMT order to Trade and OrderTracker")
                            else:
                                logger.error(f"❌ Failed to create STP_LMT order for {symbol}: {new_order.get('message')}")
                        else:
                            logger.error(f"❌ Failed to cancel STP order {stop_loss_order_id}: {cancel_result.get('message')}")
        except Exception as e:
            logger.error(f"Error converting STP to STP_LMT for {symbol}: {e}")
    
    # Subscribe to WebSocket quotes for real-time price updates
    try:
        from tiger_push_client import get_push_manager
        clean_symbol = symbol.replace('[PAPER]', '').strip()
        push_manager = get_push_manager()
        if push_manager.is_connected:
            push_manager.subscribe_quotes([clean_symbol])
            logger.info(f"📊 Subscribed to WebSocket quotes for {clean_symbol}")
    except Exception as e:
        logger.debug(f"Could not subscribe WebSocket for {symbol}: {e}")
    
    return position


def update_trailing_stop_on_position_increase(
    symbol: str,
    account_type: str,
    new_quantity: float,
    new_entry_price: float,
    new_stop_loss_price: float,
    new_take_profit_price: float = None,
    new_stop_loss_order_id: str = None,
    new_take_profit_order_id: str = None
) -> Dict:
    """
    Update TrailingStopPosition when position is increased (加仓).
    
    Key behaviors:
    - Updates quantity and entry_price (from Tiger's average cost)
    - Updates stop_loss_order_id (new order created after canceling old one)
    - If already switched to dynamic trailing: only update stop loss, no take profit
    - If still in progressive phase: update both stop loss and take profit
    - Preserves: highest_price, profit_tier, has_switched_to_trailing, current_trailing_stop
    
    Args:
        symbol: Stock symbol
        account_type: 'real' or 'paper'
        new_quantity: Total position quantity after increase
        new_entry_price: New average entry price (from Tiger API)
        new_stop_loss_price: New stop loss price from latest signal
        new_take_profit_price: New take profit price from latest signal
        new_stop_loss_order_id: New stop loss order ID from Tiger
        new_take_profit_order_id: New take profit order ID from Tiger
        
    Returns:
        dict with success status and message
    """
    result = {'success': False, 'message': '', 'updated': False}
    
    try:
        position = TrailingStopPosition.query.filter_by(
            symbol=symbol,
            account_type=account_type,
            is_active=True
        ).first()
        
        if not position:
            result['message'] = f"No active trailing stop found for {symbol}"
            logger.warning(f"📊 加仓更新: {result['message']}")
            return result
        
        old_quantity = position.quantity
        old_entry = position.entry_price
        old_stop = position.fixed_stop_loss
        has_switched = position.has_switched_to_trailing
        
        # Ensure quantity is always positive
        position.quantity = abs(new_quantity) if new_quantity else position.quantity
        position.entry_price = new_entry_price
        
        # Update stop loss order ID (always update since old order was cancelled)
        old_sl_order_id = position.stop_loss_order_id
        if new_stop_loss_order_id:
            position.stop_loss_order_id = new_stop_loss_order_id
            # Sync to Trade table and OrderTracker
            sync_stop_loss_order_to_trade(position, new_stop_loss_order_id, new_stop_loss_price or position.fixed_stop_loss or position.entry_price, old_sl_order_id, commit=False, create_tracker=True)
        
        # Stop loss price: only tighten (for long: only move up, for short: only move down)
        # Exception: new signal may have higher stop for long if entry is higher
        # Format helper for safe logging
        def fmt_price(p):
            return f"${p:.2f}" if p else "N/A"
        
        if position.side == 'long':
            # For long positions, higher stop is tighter (better)
            if new_stop_loss_price and (not old_stop or new_stop_loss_price > old_stop):
                position.fixed_stop_loss = new_stop_loss_price
                logger.info(f"📊 加仓更新 {symbol}: 止损收紧 {fmt_price(old_stop)}->{fmt_price(new_stop_loss_price)}")
            elif new_stop_loss_price and old_stop:
                # Keep old stop if new is lower (looser) - but still update order ID
                logger.info(f"📊 加仓更新 {symbol}: 保持原止损 {fmt_price(old_stop)} (新信号止损{fmt_price(new_stop_loss_price)}更低)")
        else:
            # For short positions, lower stop is tighter (better)
            if new_stop_loss_price and (not old_stop or new_stop_loss_price < old_stop):
                position.fixed_stop_loss = new_stop_loss_price
                logger.info(f"📊 加仓更新 {symbol}: 止损收紧 {fmt_price(old_stop)}->{fmt_price(new_stop_loss_price)}")
            elif new_stop_loss_price and old_stop:
                logger.info(f"📊 加仓更新 {symbol}: 保持原止损 {fmt_price(old_stop)} (新信号止损{fmt_price(new_stop_loss_price)}更高)")
        
        # Update take profit only if NOT switched to dynamic trailing
        if not has_switched:
            if new_take_profit_price:
                position.fixed_take_profit = new_take_profit_price
            if new_take_profit_order_id:
                position.take_profit_order_id = new_take_profit_order_id
            if new_take_profit_price:
                logger.info(f"📊 加仓更新 {symbol}: 更新止盈 TP=${new_take_profit_price:.2f} (阶梯止损阶段)")
        else:
            # Already switched - don't update take profit
            logger.info(f"📊 加仓更新 {symbol}: 已切换到动态trailing，不更新止盈，仅更新止损")
        
        # PRESERVE: profit_tier, current_trailing_stop, highest_price, lowest_price
        # Do NOT reset these values - preserve current tracking state
        
        # Also update CompletedTrade's entry_price to reflect new avg cost
        # This ensures P&L calculation is accurate when position is closed
        try:
            completed_trade = CompletedTrade.query.filter_by(
                trailing_stop_id=position.id,
                is_open=True
            ).first()
            
            if completed_trade and new_entry_price:
                old_ct_entry = completed_trade.entry_price
                completed_trade.entry_price = new_entry_price
                completed_trade.entry_quantity = abs(new_quantity) if new_quantity else completed_trade.entry_quantity
                logger.info(f"📊 加仓更新 CompletedTrade #{completed_trade.id}: entry_price ${old_ct_entry:.2f} -> ${new_entry_price:.2f}")
        except Exception as ct_err:
            logger.warning(f"📊 加仓更新 CompletedTrade失败: {str(ct_err)}")
        
        db.session.commit()
        
        result['success'] = True
        result['updated'] = True
        entry_str = f"${old_entry:.2f}" if old_entry else "N/A"
        new_entry_str = f"${new_entry_price:.2f}" if new_entry_price else "N/A"
        result['message'] = f"加仓更新成功: qty {old_quantity}->{new_quantity}, entry {entry_str}->{new_entry_str}"
        
        logger.info(f"✅ 加仓更新 {symbol} ({account_type}): {result['message']}")
        logger.info(f"   新止损: {fmt_price(new_stop_loss_price)}, 止损订单ID: {new_stop_loss_order_id}")
        if not has_switched and new_take_profit_price:
            logger.info(f"   新止盈: {fmt_price(new_take_profit_price)}, 止盈订单ID: {new_take_profit_order_id}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ 加仓更新trailing stop异常 {symbol}: {str(e)}")
        result['message'] = str(e)
        return result


def calculate_optimal_stop_after_scaling(
    symbol: str,
    account_type: str,
    current_price: float,
    tiger_client
) -> Dict:
    """
    加仓后立即计算最优止损价格并更新止损订单
    
    双入场价逻辑:
    1. 计算基于平均入场价的盈利% (profit_pct_avg)
    2. 计算基于首笔入场价的盈利% (profit_pct_first)
    3. 用两个盈利%确定各自的tier，取更高的tier
    4. 如果基于首笔入场价的盈利≥5%，自动切换到动态trailing
    5. 止损价格始终基于平均入场价计算（这是真正的成本）
    
    Returns:
        dict with 'success', 'message', 'stop_updated', 'new_stop_price', 'switched_to_trailing'
    """
    result = {
        'success': False,
        'message': '',
        'stop_updated': False,
        'new_stop_price': None,
        'old_stop_price': None,
        'profit_pct': 0,
        'profit_pct_first': 0,
        'tier': 0,
        'switched_to_trailing': False
    }
    
    try:
        position = TrailingStopPosition.query.filter_by(
            symbol=symbol,
            account_type=account_type,
            is_active=True
        ).first()
        
        if not position:
            result['message'] = f"No active trailing stop found for {symbol}"
            return result
        
        if not position.entry_price or position.entry_price <= 0:
            result['message'] = "Invalid entry price"
            return result
        
        if not current_price or current_price <= 0:
            result['message'] = "Invalid current price"
            return result
        
        # Get config (no parameter needed)
        config = get_trailing_stop_config()
        
        is_long = position.side == 'long'
        
        # Calculate profit based on AVERAGE entry price (current cost)
        if is_long:
            profit_pct_avg = (current_price - position.entry_price) / position.entry_price
        else:
            profit_pct_avg = (position.entry_price - current_price) / position.entry_price
        
        result['profit_pct'] = profit_pct_avg * 100
        
        # Calculate profit based on FIRST entry price (original position)
        first_entry = position.first_entry_price or position.entry_price
        if is_long:
            profit_pct_first = (current_price - first_entry) / first_entry
        else:
            profit_pct_first = (first_entry - current_price) / first_entry
        
        result['profit_pct_first'] = profit_pct_first * 100
        
        # Determine tier based on both entry prices, take the HIGHER tier
        tier_from_avg = get_progressive_stop_tier(profit_pct_avg, config)
        tier_from_first = get_progressive_stop_tier(profit_pct_first, config)
        target_tier = max(tier_from_avg, tier_from_first)
        result['tier'] = target_tier
        
        logger.info(f"📊 加仓止损优化 {symbol}: 平均入场${position.entry_price:.2f}盈利{profit_pct_avg*100:.2f}%(tier{tier_from_avg}), "
                   f"首笔入场${first_entry:.2f}盈利{profit_pct_first*100:.2f}%(tier{tier_from_first}) -> 取tier{target_tier}")
        
        # Check if we should switch to dynamic trailing based on first entry profit
        # Condition: first entry profit >= 5% and not already switched
        EARLY_SWITCH_THRESHOLD = 0.05  # 5%
        if profit_pct_first >= EARLY_SWITCH_THRESHOLD and not position.has_switched_to_trailing:
            logger.info(f"🚀 {symbol} 首笔入场盈利{profit_pct_first*100:.2f}% >= 5%, 提前切换到动态trailing!")
            position.has_switched_to_trailing = True
            position.switch_triggered_at = datetime.utcnow()
            position.switch_reason = f"Early switch: first entry profit {profit_pct_first*100:.1f}% >= 5%"
            result['switched_to_trailing'] = True
            
            # Cancel take profit order since we're switching to trailing
            if position.take_profit_order_id and tiger_client:
                try:
                    cancel_result = tiger_client.cancel_order(position.take_profit_order_id)
                    if cancel_result.get('success'):
                        logger.info(f"📝 取消止盈订单 {position.take_profit_order_id}")
                        position.take_profit_order_id = None
                except Exception as e:
                    logger.warning(f"取消止盈订单失败: {e}")
        
        if target_tier <= 0:
            result['success'] = True
            result['message'] = f"盈利{profit_pct_avg*100:.2f}%/首笔{profit_pct_first*100:.2f}%未达tier1阈值，无需调整止损"
            db.session.commit()  # Commit any switch changes
            return result
        
        # Calculate the stop price for this tier (ALWAYS based on average entry price - this is true cost)
        tier_stop_price = calculate_progressive_stop_price(position, target_tier, config)
        
        if not tier_stop_price:
            result['message'] = "Failed to calculate tier stop price"
            return result
        
        # Get current stop price
        current_stop = position.fixed_stop_loss or position.current_trailing_stop
        result['old_stop_price'] = current_stop
        
        # Compare: for long, higher stop is tighter; for short, lower stop is tighter
        should_update = False
        if is_long:
            if not current_stop or tier_stop_price > current_stop:
                should_update = True
        else:
            if not current_stop or tier_stop_price < current_stop:
                should_update = True
        
        if not should_update:
            result['success'] = True
            current_stop_str = f"${current_stop:.2f}" if current_stop else "N/A"
            result['message'] = f"当前止损{current_stop_str}已经优于tier{target_tier}止损${tier_stop_price:.2f}，无需更新"
            db.session.commit()  # Commit any switch changes
            return result
        
        # Need to update the stop loss order immediately
        result['new_stop_price'] = tier_stop_price
        
        # First, query current open orders for this symbol to find the actual stop order
        # (加仓后会产生新的止损订单，order_id已经变了)
        if tiger_client:
            logger.info(f"🔄 加仓后立即优化止损 {symbol}: 当前价${current_price:.2f}, tier{target_tier} (基于更高盈利)")
            old_stop_str = f"${current_stop:.2f}" if current_stop else "$0"
            logger.info(f"   止损调整: {old_stop_str} -> ${tier_stop_price:.2f}")
            
            # Query current open orders to find the actual stop order
            open_orders_result = tiger_client.get_open_orders_for_symbol(symbol)
            actual_stop_order_id = None
            
            if open_orders_result.get('success'):
                for order in open_orders_result.get('orders', []):
                    order_type = getattr(order, 'order_type', '')
                    order_type_str = str(order_type).upper()
                    # Stop orders have type 'STP' or 'STOP' or similar
                    if 'STP' in order_type_str or 'STOP' in order_type_str:
                        actual_stop_order_id = order.id
                        logger.info(f"📋 找到当前止损订单: {actual_stop_order_id} (type={order_type})")
                        break
            
            if not actual_stop_order_id:
                # Fallback to the stored order_id
                actual_stop_order_id = position.stop_loss_order_id
                logger.info(f"📋 使用存储的止损订单ID: {actual_stop_order_id}")
            
            if not actual_stop_order_id:
                # No stop order found, create new one
                logger.info(f"📋 未找到止损订单，创建新订单")
                stop_result = tiger_client.place_stop_limit_order(
                    symbol=symbol,
                    action='SELL' if is_long else 'BUY',
                    quantity=abs(position.quantity),
                    stop_price=tier_stop_price
                )
                
                if stop_result.get('success'):
                    new_order_id = stop_result.get('order_id')
                    position.stop_loss_order_id = new_order_id
                    position.fixed_stop_loss = tier_stop_price
                    position.current_trailing_stop = tier_stop_price
                    position.profit_tier = target_tier
                    
                    # Sync to Trade table and OrderTracker (before final commit)
                    sync_stop_loss_order_to_trade(position, new_order_id, tier_stop_price, None, commit=False)
                    db.session.commit()
                    
                    result['success'] = True
                    result['stop_updated'] = True
                    result['message'] = f"✅ 创建新止损订单成功: tier{target_tier}, 止损${tier_stop_price:.2f}"
                    logger.info(result['message'])
                else:
                    result['message'] = f"Failed to create stop order: {stop_result.get('error')}"
                    logger.error(result['message'])
                return result
            
            # Update stored order_id if different
            if actual_stop_order_id != position.stop_loss_order_id:
                logger.info(f"📝 更新止损订单ID: {position.stop_loss_order_id} -> {actual_stop_order_id}")
                old_discovered_id = position.stop_loss_order_id
                position.stop_loss_order_id = actual_stop_order_id
                # Sync discovered order ID to Trade table (no new tracker needed)
                sync_stop_loss_order_to_trade(position, actual_stop_order_id, position.fixed_stop_loss or position.entry_price, old_discovered_id, commit=False, create_tracker=False)
            
            # Try modify_stop_loss_price first
            modify_result = tiger_client.modify_stop_loss_price(
                old_order_id=str(actual_stop_order_id),
                symbol=symbol,
                quantity=abs(position.quantity),
                new_stop_price=tier_stop_price,
                side='sell' if is_long else 'buy'
            )
            
            if modify_result.get('success'):
                # Update position record
                new_order_id = modify_result.get('new_order_id', actual_stop_order_id)
                if str(new_order_id) != str(actual_stop_order_id):
                    position.stop_loss_order_id = str(new_order_id)
                position.fixed_stop_loss = tier_stop_price
                position.current_trailing_stop = tier_stop_price
                position.profit_tier = target_tier
                
                # Sync price update to Trade table
                sync_stop_loss_order_to_trade(position, new_order_id, tier_stop_price, None, commit=False, create_tracker=False)
                db.session.commit()
                
                result['success'] = True
                result['stop_updated'] = True
                result['message'] = f"✅ 加仓后立即优化止损成功: tier{target_tier}, 止损${tier_stop_price:.2f} (盈利{profit_pct_avg*100:.2f}%)"
                logger.info(result['message'])
                
                # Log event
                log_trailing_stop_event(
                    position,
                    'scaling_stop_optimization',
                    current_price,
                    f"加仓止损优化: profit={profit_pct_avg*100:.2f}%, tier={target_tier}, old_stop={current_stop}, new_stop={tier_stop_price}"
                )
            else:
                # modify_stop_loss_price failed, try cancel and recreate
                logger.warning(f"modify_stop_loss_price failed: {modify_result.get('error')}, trying cancel+recreate")
                
                # Cancel the actual stop order (use actual_stop_order_id, not stored order_id)
                cancel_result = tiger_client.cancel_order(actual_stop_order_id)
                if cancel_result.get('success'):
                    import time
                    time.sleep(0.3)
                    
                    # Create new stop order
                    stop_result = tiger_client.place_stop_limit_order(
                        symbol=symbol,
                        action='SELL' if is_long else 'BUY',
                        quantity=abs(position.quantity),
                        stop_price=tier_stop_price
                    )
                    
                    if stop_result.get('success'):
                        new_order_id = stop_result.get('order_id')
                        position.stop_loss_order_id = new_order_id
                        position.fixed_stop_loss = tier_stop_price
                        position.current_trailing_stop = tier_stop_price
                        position.profit_tier = target_tier
                        
                        # Sync to Trade table and OrderTracker (before final commit)
                        sync_stop_loss_order_to_trade(position, new_order_id, tier_stop_price, actual_stop_order_id, commit=False)
                        db.session.commit()
                        
                        result['success'] = True
                        result['stop_updated'] = True
                        result['message'] = f"✅ 加仓后止损优化成功(重建订单): tier{target_tier}, 止损${tier_stop_price:.2f}"
                        logger.info(result['message'])
                    else:
                        result['message'] = f"Failed to create new stop order: {stop_result.get('error')}"
                        logger.error(result['message'])
                else:
                    result['message'] = f"Failed to cancel old order: {cancel_result.get('error')}"
                    logger.error(result['message'])
        else:
            # Just update the database record, no order to modify
            position.fixed_stop_loss = tier_stop_price
            position.current_trailing_stop = tier_stop_price
            position.profit_tier = target_tier
            db.session.commit()
            
            result['success'] = True
            result['stop_updated'] = True
            result['message'] = f"Updated stop in database (no order ID to modify): ${tier_stop_price:.2f}"
        
        return result
        
    except Exception as e:
        logger.error(f"❌ calculate_optimal_stop_after_scaling异常 {symbol}: {str(e)}")
        result['message'] = str(e)
        return result


def _update_completed_trade_on_exit(position: TrailingStopPosition, exit_method: ExitMethod, exit_price: float):
    """
    Helper function to update CompletedTrade when a position is closed via stop loss or take profit.
    """
    completed_trade = CompletedTrade.query.filter_by(
        trailing_stop_id=position.id,
        is_open=True
    ).first()
    
    if not completed_trade:
        # Try to find by symbol and account_type
        completed_trade = CompletedTrade.query.filter_by(
            symbol=position.symbol,
            account_type=position.account_type,
            is_open=True
        ).order_by(CompletedTrade.created_at.desc()).first()
    
    if completed_trade:
        completed_trade.exit_method = exit_method
        completed_trade.exit_time = datetime.utcnow()
        completed_trade.exit_price = exit_price
        completed_trade.exit_quantity = position.quantity
        completed_trade.is_open = False
        completed_trade.final_stop_loss = position.current_trailing_stop
        completed_trade.highest_price = position.highest_price
        completed_trade.lowest_price = position.lowest_price
        completed_trade.stop_adjustment_count = position.stop_adjustment_count or 0
        
        # Calculate P&L
        if completed_trade.entry_price and exit_price:
            if position.side == 'long':
                completed_trade.pnl_amount = (exit_price - completed_trade.entry_price) * completed_trade.entry_quantity
                completed_trade.pnl_percent = ((exit_price - completed_trade.entry_price) / completed_trade.entry_price) * 100
                if position.highest_price:
                    completed_trade.max_profit_pct = ((position.highest_price - completed_trade.entry_price) / completed_trade.entry_price) * 100
            else:  # short
                completed_trade.pnl_amount = (completed_trade.entry_price - exit_price) * completed_trade.entry_quantity
                completed_trade.pnl_percent = ((completed_trade.entry_price - exit_price) / completed_trade.entry_price) * 100
                if position.lowest_price:
                    completed_trade.max_profit_pct = ((completed_trade.entry_price - position.lowest_price) / completed_trade.entry_price) * 100
        
        # Calculate hold duration
        if completed_trade.entry_time:
            hold_duration = datetime.utcnow() - completed_trade.entry_time
            completed_trade.hold_duration_seconds = int(hold_duration.total_seconds())
        
        logger.info(f"📊 Updated CompletedTrade #{completed_trade.id} with {exit_method.value}, P&L: {completed_trade.pnl_percent:.2f}%")
    else:
        logger.warning(f"📊 No open CompletedTrade found for {position.symbol}")


def deactivate_trailing_stop(symbol: str, account_type: str = 'real', reason: str = None):
    position = TrailingStopPosition.query.filter_by(
        symbol=symbol,
        account_type=account_type,
        is_active=True
    ).first()
    
    if position:
        position.is_active = False
        position.trigger_reason = reason or "Manually deactivated"
        db.session.commit()
        logger.info(f"Deactivated trailing stop for {symbol}: {reason}")
        return True
    
    return False


def sync_position_with_tiger(position: TrailingStopPosition, tiger_client) -> Dict:
    """
    Synchronize a single TrailingStopPosition with Tiger API order status.
    
    Checks if local order IDs are still valid in Tiger and updates status accordingly.
    
    Returns:
        Dict with sync results and any fixes applied
    """
    from datetime import datetime
    
    result = {
        'symbol': position.symbol,
        'account_type': position.account_type,
        'checks_performed': [],
        'issues_found': [],
        'fixes_applied': []
    }
    
    if not tiger_client:
        result['error'] = 'Tiger client not available'
        return result
    
    # Check stop loss order status - 即使order_id为空也尝试查询获取
    result['checks_performed'].append('stop_loss_order')
    try:
        # 查询实际的止损订单ID
        actual_stop_order_id = get_actual_stop_order_id(
            tiger_client, position.symbol, position.stop_loss_order_id or ''
        )
        
        if actual_stop_order_id:
            if actual_stop_order_id != position.stop_loss_order_id:
                result['issues_found'].append(f'Stop loss order ID updated: {position.stop_loss_order_id} -> {actual_stop_order_id}')
                old_discovered_id = position.stop_loss_order_id
                position.stop_loss_order_id = actual_stop_order_id
                # Sync discovered order ID to Trade table (no new tracker needed)
                sync_stop_loss_order_to_trade(position, actual_stop_order_id, position.fixed_stop_loss or position.entry_price, old_discovered_id, commit=False, create_tracker=False)
                result['fixes_applied'].append('Updated stop_loss_order_id from Tiger API')
            
            order_status = tiger_client.get_order_status(actual_stop_order_id)
            if order_status.get('success'):
                status = order_status.get('status', '')
                
                if status == 'filled':
                    # Stop loss was triggered - position should be closed
                    result['issues_found'].append(f'Stop loss order {actual_stop_order_id} was filled but position still active')
                    position.is_active = False
                    position.is_triggered = True
                    position.triggered_at = datetime.utcnow()
                    position.trigger_reason = "止损订单已成交 (同步检测)"
                    result['fixes_applied'].append('Deactivated position - stop loss filled')
                    
                    # Update CompletedTrade
                    try:
                        _update_completed_trade_on_exit(
                            position, 
                            ExitMethod.STOP_LOSS, 
                            order_status.get('filled_price') or position.fixed_stop_loss
                        )
                    except Exception as ct_e:
                        logger.error(f"Failed to update CompletedTrade on stop loss: {ct_e}")
                    
                elif status == 'cancelled':
                    # Stop loss was cancelled - might be an issue
                    result['issues_found'].append(f'Stop loss order {actual_stop_order_id} was cancelled')
                    old_order_id = position.stop_loss_order_id
                    position.stop_loss_order_id = None
                    # Sync Trade table: clear stop_loss_order_id
                    _sync_trade_stop_loss_cleared(position, old_order_id, 'cancelled')
                    result['fixes_applied'].append('Cleared invalid stop_loss_order_id')
                    
                elif status == 'rejected':
                    result['issues_found'].append(f'Stop loss order {actual_stop_order_id} was rejected')
                    old_order_id = position.stop_loss_order_id
                    position.stop_loss_order_id = None
                    # Sync Trade table: clear stop_loss_order_id
                    _sync_trade_stop_loss_cleared(position, old_order_id, 'rejected')
                    result['fixes_applied'].append('Cleared rejected stop_loss_order_id')
                    
            else:
                # Order not found - might be expired or invalid
                error = order_status.get('error', 'Unknown error')
                if 'not found' in error.lower() or 'invalid' in error.lower():
                    result['issues_found'].append(f'Stop loss order {actual_stop_order_id} not found in Tiger')
                    old_order_id = position.stop_loss_order_id
                    position.stop_loss_order_id = None
                    # Sync Trade table: clear stop_loss_order_id
                    _sync_trade_stop_loss_cleared(position, old_order_id, 'not_found')
                    result['fixes_applied'].append('Cleared missing stop_loss_order_id')
        else:
            if position.stop_loss_order_id:
                result['issues_found'].append(f'Stored stop_loss_order_id {position.stop_loss_order_id} but no order found in Tiger')
                old_order_id = position.stop_loss_order_id
                position.stop_loss_order_id = None
                # Sync Trade table: clear stop_loss_order_id
                _sync_trade_stop_loss_cleared(position, old_order_id, 'orphaned')
                result['fixes_applied'].append('Cleared orphaned stop_loss_order_id')
                    
    except Exception as e:
        result['checks_performed'].append(f'stop_loss_check_error: {str(e)}')
    
    # Check take profit order status - 即使order_id为空也尝试查询获取
    result['checks_performed'].append('take_profit_order')
    try:
        # 查询实际的止盈订单ID
        actual_tp_order_id = get_actual_take_profit_order_id(
            tiger_client, position.symbol, position.take_profit_order_id or ''
        )
        
        if actual_tp_order_id:
            if actual_tp_order_id != position.take_profit_order_id:
                result['issues_found'].append(f'Take profit order ID updated: {position.take_profit_order_id} -> {actual_tp_order_id}')
                position.take_profit_order_id = actual_tp_order_id
                result['fixes_applied'].append('Updated take_profit_order_id from Tiger API')
            
            order_status = tiger_client.get_order_status(actual_tp_order_id)
            if order_status.get('success'):
                status = order_status.get('status', '')
                
                if status == 'filled':
                    # Take profit was triggered - position should be closed
                    result['issues_found'].append(f'Take profit order {actual_tp_order_id} was filled but position still active')
                    position.is_active = False
                    position.triggered_at = datetime.utcnow()
                    position.trigger_reason = "止盈订单已成交 (同步检测)"
                    result['fixes_applied'].append('Deactivated position - take profit filled')
                    
                    # Update CompletedTrade
                    try:
                        _update_completed_trade_on_exit(
                            position, 
                            ExitMethod.TAKE_PROFIT, 
                            order_status.get('filled_price') or position.fixed_take_profit
                        )
                    except Exception as ct_e:
                        logger.error(f"Failed to update CompletedTrade on take profit: {ct_e}")
                    
                elif status == 'cancelled':
                    result['issues_found'].append(f'Take profit order {actual_tp_order_id} was cancelled')
                    position.take_profit_order_id = None
                    result['fixes_applied'].append('Cleared cancelled take_profit_order_id')
                    
                elif status == 'rejected':
                    result['issues_found'].append(f'Take profit order {actual_tp_order_id} was rejected')
                    position.take_profit_order_id = None
                    result['fixes_applied'].append('Cleared rejected take_profit_order_id')
                    
            else:
                error = order_status.get('error', 'Unknown error')
                if 'not found' in error.lower() or 'invalid' in error.lower():
                    result['issues_found'].append(f'Take profit order {actual_tp_order_id} not found in Tiger')
                    position.take_profit_order_id = None
                    result['fixes_applied'].append('Cleared missing take_profit_order_id')
        else:
            if position.take_profit_order_id:
                result['issues_found'].append(f'Stored take_profit_order_id {position.take_profit_order_id} but no order found in Tiger')
                position.take_profit_order_id = None
                result['fixes_applied'].append('Cleared orphaned take_profit_order_id')
                    
    except Exception as e:
        result['checks_performed'].append(f'take_profit_check_error: {str(e)}')
    
    # Commit changes if any fixes were applied
    if result['fixes_applied']:
        db.session.commit()
        logger.info(f"🔄 Sync fixes for {position.symbol}: {result['fixes_applied']}")
    
    return result


def sync_all_active_positions(tiger_client) -> List[Dict]:
    """
    Synchronize all active TrailingStopPositions with Tiger API.
    
    This should be called periodically to detect and fix data inconsistencies.
    
    Returns:
        List of sync results for each position
    """
    results = []
    
    active_positions = TrailingStopPosition.query.filter_by(is_active=True).all()
    
    logger.info(f"🔄 Starting sync check for {len(active_positions)} active positions")
    
    for position in active_positions:
        try:
            result = sync_position_with_tiger(position, tiger_client)
            results.append(result)
            
            if result.get('issues_found'):
                logger.warning(f"⚠️ Issues found for {position.symbol}: {result['issues_found']}")
                
        except Exception as e:
            logger.error(f"Error syncing position {position.symbol}: {str(e)}")
            results.append({
                'symbol': position.symbol,
                'account_type': position.account_type,
                'error': str(e)
            })
    
    # Summary
    total_issues = sum(len(r.get('issues_found', [])) for r in results)
    total_fixes = sum(len(r.get('fixes_applied', [])) for r in results)
    
    logger.info(f"🔄 Sync complete: {len(results)} positions checked, {total_issues} issues found, {total_fixes} fixes applied")
    
    return results


def close_completed_trades_fifo(symbol: str, account_type: str, exit_quantity: float, exit_price: float, 
                                 exit_method: ExitMethod, exit_time: datetime = None) -> List[dict]:
    """
    Close CompletedTrade records using FIFO (First In First Out) matching.
    
    Args:
        symbol: Stock symbol
        account_type: 'real' or 'paper'
        exit_quantity: Total quantity being exited
        exit_price: Exit price
        exit_method: How the position was closed
        exit_time: Exit timestamp (default: now)
        
    Returns:
        List of dicts with matched trade info and calculated P&L
    """
    if exit_time is None:
        exit_time = datetime.utcnow()
    
    remaining_exit_qty = exit_quantity
    matched_trades = []
    
    # Find open CompletedTrade records for this symbol, ordered by entry_time (FIFO)
    open_trades = CompletedTrade.query.filter(
        CompletedTrade.symbol == symbol,
        CompletedTrade.account_type == account_type,
        CompletedTrade.is_open == True
    ).order_by(CompletedTrade.entry_time.asc()).all()
    
    if not open_trades:
        logger.warning(f"⚠️ FIFO: No open CompletedTrade records found for {symbol} ({account_type})")
        return []
    
    logger.info(f"📊 FIFO matching: {exit_quantity} shares @ ${exit_price:.2f} against {len(open_trades)} open entries")
    
    for trade in open_trades:
        if remaining_exit_qty <= 0:
            break
        
        # Get available quantity for this trade
        available_qty = trade.remaining_quantity if trade.remaining_quantity is not None else trade.entry_quantity
        if available_qty is None or available_qty <= 0:
            continue
        
        # How much to exit from this trade
        qty_to_exit = min(available_qty, remaining_exit_qty)
        remaining_exit_qty -= qty_to_exit
        
        # Calculate P&L for this portion
        if trade.side == 'long':
            pnl_amount = (exit_price - trade.entry_price) * qty_to_exit
            pnl_percent = ((exit_price - trade.entry_price) / trade.entry_price) * 100 if trade.entry_price else 0
        else:
            pnl_amount = (trade.entry_price - exit_price) * qty_to_exit
            pnl_percent = ((trade.entry_price - exit_price) / trade.entry_price) * 100 if trade.entry_price else 0
        
        # Update trade record
        new_remaining = available_qty - qty_to_exit
        trade.remaining_quantity = new_remaining
        
        # Update exited quantity and avg exit price (weighted average)
        prev_exited = trade.exited_quantity or 0
        prev_avg_exit = trade.avg_exit_price or 0
        total_exited = prev_exited + qty_to_exit
        
        if total_exited > 0:
            trade.avg_exit_price = ((prev_avg_exit * prev_exited) + (exit_price * qty_to_exit)) / total_exited
        trade.exited_quantity = total_exited
        
        # If fully closed, update status
        if new_remaining <= 0:
            trade.is_open = False
            trade.exit_time = exit_time
            trade.exit_price = trade.avg_exit_price
            trade.exit_method = exit_method
            
            # Calculate total P&L based on weighted average exit
            if trade.side == 'long':
                trade.pnl_amount = (trade.avg_exit_price - trade.entry_price) * trade.entry_quantity
                trade.pnl_percent = ((trade.avg_exit_price - trade.entry_price) / trade.entry_price) * 100
            else:
                trade.pnl_amount = (trade.entry_price - trade.avg_exit_price) * trade.entry_quantity
                trade.pnl_percent = ((trade.entry_price - trade.avg_exit_price) / trade.entry_price) * 100
            
            # Calculate hold duration
            if trade.entry_time:
                trade.hold_duration_seconds = int((exit_time - trade.entry_time).total_seconds())
        
        matched_trades.append({
            'trade_id': trade.id,
            'symbol': symbol,
            'entry_price': trade.entry_price,
            'entry_quantity': trade.entry_quantity,
            'qty_exited': qty_to_exit,
            'exit_price': exit_price,
            'pnl_amount': pnl_amount,
            'pnl_percent': pnl_percent,
            'fully_closed': new_remaining <= 0
        })
        
        logger.info(f"📊 FIFO matched: Trade #{trade.id} - {qty_to_exit} shares @ ${exit_price:.2f}, P&L: ${pnl_amount:.2f} ({pnl_percent:.2f}%)")
    
    db.session.commit()
    
    if remaining_exit_qty > 0:
        logger.warning(f"⚠️ FIFO: {remaining_exit_qty} shares could not be matched to any open entry")
    
    total_pnl = sum(t['pnl_amount'] for t in matched_trades)
    logger.info(f"📊 FIFO complete: {len(matched_trades)} entries matched, total P&L: ${total_pnl:.2f}")
    
    return matched_trades


def init_completed_trade_remaining_quantity(trade: CompletedTrade):
    """Initialize remaining_quantity field when creating a new CompletedTrade"""
    if trade.remaining_quantity is None:
        trade.remaining_quantity = trade.entry_quantity
    if trade.exited_quantity is None:
        trade.exited_quantity = 0


def cleanup_duplicate_trailing_stops(dry_run=False):
    active_positions = TrailingStopPosition.query.filter_by(is_active=True).all()
    
    symbol_groups = {}
    for pos in active_positions:
        clean_sym = pos.symbol.replace('[PAPER]', '').strip()
        key = (clean_sym, pos.account_type)
        if key not in symbol_groups:
            symbol_groups[key] = []
        symbol_groups[key].append(pos)
    
    deactivated = []
    normalized = []
    
    for (sym, acct), positions in symbol_groups.items():
        has_paper_prefix = any('[PAPER]' in p.symbol for p in positions)
        has_clean = any('[PAPER]' not in p.symbol for p in positions)
        
        if len(positions) > 1 and has_paper_prefix and has_clean:
            paper_prefixed = [p for p in positions if '[PAPER]' in p.symbol]
            clean_ones = [p for p in positions if '[PAPER]' not in p.symbol]
            
            keeper = clean_ones[-1]
            
            for dup in paper_prefixed:
                if not dry_run:
                    dup.is_active = False
                    dup.trigger_reason = f"Duplicate cleanup ([PAPER] prefix) - kept #{keeper.id}"
                    dup.triggered_at = datetime.utcnow()
                deactivated.append({
                    'id': dup.id,
                    'symbol': dup.symbol,
                    'account_type': dup.account_type,
                    'created_at': str(dup.created_at),
                    'kept_id': keeper.id
                })
                logger.info(f"{'[DRY RUN] ' if dry_run else ''}🗑️ Deactivated duplicate #{dup.id} ({dup.symbol}) - kept #{keeper.id} ({keeper.symbol})")
        
        deactivated_ids = {d['id'] for d in deactivated}
        for pos in positions:
            if '[PAPER]' in pos.symbol and pos.is_active and pos.id not in deactivated_ids:
                old_sym = pos.symbol
                if not dry_run:
                    pos.symbol = sym
                normalized.append(f"{old_sym} → {sym} (id={pos.id})")
                logger.info(f"{'[DRY RUN] ' if dry_run else ''}🔄 Normalized: {old_sym} → {sym}")
    
    if not dry_run:
        db.session.commit()
    
    result = {
        'dry_run': dry_run,
        'deactivated_count': len(deactivated),
        'normalized_count': len(normalized),
        'deactivated': deactivated,
        'normalized': normalized,
        'total_active_after': TrailingStopPosition.query.filter_by(is_active=True).count()
    }
    
    logger.info(f"🧹 Cleanup {'(DRY RUN) ' if dry_run else ''}complete: {len(deactivated)} duplicates removed, {len(normalized)} symbols normalized, {result['total_active_after']} active remaining")
    return result


def clear_bracket_sub_order_ids_eod(app) -> Dict:
    """Clear bracket sub-order IDs from TrailingStopPosition at EOD.
    
    Called after 20:00 ET when DAY bracket sub-orders have expired.
    Clears stop_loss_order_id and take_profit_order_id so the soft stop
    (software monitoring) takes over for cross-day protection.
    
    This replaces the old OCA rebuild approach: instead of creating new
    orders each morning, the soft stop provides 24/7 protection.
    """
    cleared = []
    try:
        with app.app_context():
            active_positions = TrailingStopPosition.query.filter_by(
                is_active=True, account_type='paper'
            ).all()
            
            for pos in active_positions:
                changed = False
                if pos.stop_loss_order_id:
                    old_sl = pos.stop_loss_order_id
                    pos.stop_loss_order_id = None
                    _sync_trade_stop_loss_cleared(pos, old_sl, 'eod_bracket_expiry')
                    changed = True
                if pos.take_profit_order_id:
                    pos.take_profit_order_id = None
                    changed = True
                if changed:
                    cleared.append(pos.symbol)
                    logger.info(f"🌙 EOD bracket cleanup: cleared order IDs for {pos.symbol} "
                               f"(#{pos.id}), soft stop active")
            
            if cleared:
                db.session.commit()
                logger.info(f"🌙 EOD bracket cleanup complete: {len(cleared)} positions → soft stop mode")
            
            return {
                'cleared_count': len(cleared),
                'symbols': cleared
            }
    except Exception as e:
        logger.error(f"❌ EOD bracket cleanup error: {e}")
        return {'cleared_count': 0, 'symbols': [], 'error': str(e)}
