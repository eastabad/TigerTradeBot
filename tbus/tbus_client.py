"""
TBUS Tiger Trading Client - Order operations for TBUS (US Standard) accounts.

TBUS accounts have these API limitations:
- No OCA orders
- No attached orders (bracket orders)
- No STP_LMT order type
- GTC orders: outside_rth forced to False by server for both STP and LMT
- Supported: GTC STP, GTC LMT, DAY MKT/LMT/STP with outside_rth=True

Protection strategy:
- Stop Loss: GTC STP (active during regular hours only)
- Take Profit: GTC LMT with outside_rth=False
- Extended hours: Soft Stop via software monitoring (tbus_protection_service)

This module is called from routing logic in tiger_client.py and oca_service.py
when TBUS account is detected.
"""

import logging
import time as time_module
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)


def create_tbus_protection_orders(
    tiger_client,
    symbol: str,
    quantity: int,
    stop_loss_price: float,
    take_profit_price: float,
    entry_price: float = None,
    trade_id: int = None,
    trailing_stop_id: int = None,
    cancel_existing: bool = True,
    skip_position_check: bool = False,
    position_side: str = 'long'
) -> Dict:
    """Create TBUS protection orders: separate GTC STP + GTC LMT.
    
    This replaces create_oca_orders_for_position() for TBUS accounts.
    Uses the existing TigerClient instance for API calls.
    
    TBUS-specific behavior:
    - STP (not STP_LMT) for stop loss — TBUS doesn't support STP_LMT
    - GTC for both orders (don't expire daily)
    - outside_rth=False for both (server forces it for TBUS GTC anyway)
    - No OCA grouping — orders are independent
    - Software handles sibling cancellation via OCAGroup DB tracking
    
    Args:
        tiger_client: Initialized TigerClient instance (with .client and .client_config)
        symbol: Stock symbol
        quantity: Number of shares
        stop_loss_price: Stop loss trigger price
        take_profit_price: Take profit limit price
        entry_price: Entry price for reference
        trade_id: Related Trade record ID
        trailing_stop_id: Related TrailingStopPosition ID
        cancel_existing: Whether to cancel existing orders first
        skip_position_check: Skip position verification
        position_side: 'long' or 'short'
        
    Returns:
        dict with success, order IDs, and OCAGroup record ID
    """
    from tigeropen.common.util.contract_utils import stock_contract
    from tigeropen.common.util.order_utils import stop_order, limit_order
    from config import get_config

    if not tiger_client.client or not tiger_client.client_config:
        return {'success': False, 'error': 'Tiger client not initialized'}

    try:
        if get_config('TRADING_ENABLED', 'true').lower() != 'true':
            return {'success': False, 'error': 'Trading is currently disabled'}

        quantity = abs(int(quantity)) if quantity else 0
        if quantity <= 0:
            return {'success': False, 'error': f'Invalid quantity={quantity}, must be > 0'}

        if skip_position_check:
            if position_side == 'long':
                action = 'SELL'
                side = 'long'
            else:
                action = 'BUY'
                side = 'short'
        else:
            positions_result = tiger_client.get_positions(symbol)
            if not positions_result['success'] or not positions_result['positions']:
                return {'success': False, 'error': f'No position found for {symbol}'}

            position = positions_result['positions'][0]
            current_qty = position['quantity']

            if current_qty > 0:
                action = 'SELL'
                side = 'long'
            else:
                action = 'BUY'
                side = 'short'
                current_qty = abs(current_qty)

            if quantity > current_qty:
                return {
                    'success': False,
                    'error': f'Cannot set protection for {quantity} shares, only {current_qty} available'
                }

        logger.info(f"[TBUS] Creating protection orders for {side.upper()} position: "
                   f"{quantity} shares of {symbol}, SL=${stop_loss_price}, TP=${take_profit_price}")

        if stop_loss_price:
            stop_loss_price = round(stop_loss_price, 2)
        if take_profit_price:
            take_profit_price = round(take_profit_price, 2)

        existing_tp_order_id = None
        existing_tp_price = None
        existing_tp_qty = None
        try:
            open_orders_result = tiger_client.get_open_orders_for_symbol(symbol)
            if open_orders_result.get('success'):
                for order in open_orders_result.get('orders', []):
                    order_action = getattr(order, 'action', '')
                    if (side == 'long' and order_action == 'SELL') or (side == 'short' and order_action == 'BUY'):
                        existing_tp_order_id = str(order.id)
                        existing_tp_price = getattr(order, 'limit_price', None)
                        existing_tp_qty = getattr(order, 'quantity', None)
                        logger.info(f"[TBUS] Found existing TP order {existing_tp_order_id}: "
                                   f"{order_action} {existing_tp_qty} @ ${existing_tp_price}")
                        break
        except Exception as e:
            logger.warning(f"[TBUS] Could not check existing orders: {e}")

        contract = stock_contract(symbol=symbol, currency='USD')
        oca_group_id_str = f"TBUS-{symbol}-{int(time_module.time())}"
        orders_created = []
        stop_loss_id = None
        take_profit_id = None

        tif = 'GTC'
        outside_rth = False

        if stop_loss_price:
            logger.info(f"[TBUS] Skipping stop loss order (SL=${stop_loss_price}) — "
                       f"TBUS uses software trailing stop for SL to avoid position hold conflicts. "
                       f"SL managed by trailing stop engine.")

        if take_profit_price:
            if existing_tp_order_id and existing_tp_price == take_profit_price and existing_tp_qty == quantity:
                take_profit_id = existing_tp_order_id
                orders_created.append(f"Take Profit: {take_profit_id}")
                logger.info(f"[TBUS] TP order already exists at ${take_profit_price} qty={quantity}, "
                           f"reusing order {take_profit_id}, skipping duplicate placement")
            else:
                if existing_tp_order_id:
                    try:
                        tiger_client.cancel_order(existing_tp_order_id)
                        logger.info(f"[TBUS] Cancelled old TP {existing_tp_order_id} "
                                   f"(price ${existing_tp_price}->${take_profit_price}, qty {existing_tp_qty}->{quantity})")
                        time_module.sleep(2)
                        logger.info(f"[TBUS] Waited 2s for position hold release after cancelling old TP")
                    except Exception as e:
                        logger.warning(f"[TBUS] Could not cancel old TP {existing_tp_order_id}: {e}")

                try:
                    tp_order = limit_order(
                        account=tiger_client.client_config.account,
                        contract=contract,
                        action=action,
                        quantity=quantity,
                        limit_price=take_profit_price,
                        time_in_force=tif
                    )
                    tp_order.outside_rth = outside_rth

                    tp_result = tiger_client.client.place_order(tp_order)
                    if tp_result and tp_order.id:
                        take_profit_id = str(tp_order.id)
                        orders_created.append(f"Take Profit: {take_profit_id}")
                        logger.info(f"[TBUS] GTC LMT take profit placed: {take_profit_id} at ${take_profit_price} "
                                   f"(outside_rth={outside_rth})")
                    else:
                        logger.error(f"[TBUS] Take profit order placement returned no ID")
                except Exception as e:
                    error_str = str(e).lower()
                    if 'code=1200' in error_str or 'code=4' in error_str or 'forbidden' in error_str:
                        logger.warning(f"[TBUS] Take profit rate-limited: {e}")
                        return {
                            'success': False,
                            'error': f'Rate limited: {e}',
                            'rate_limited': True,
                            'orders_created': []
                        }
                    logger.error(f"[TBUS] Take profit order failed: {e}")

        oca_group_record = None
        if orders_created:
            try:
                from app import db
                from models import OCAGroup, OCAStatus

                oca_group_record = OCAGroup(
                    oca_group_id=oca_group_id_str,
                    symbol=symbol,
                    account=tiger_client.client_config.account,
                    account_type='real',
                    side=side,
                    quantity=quantity,
                    entry_price=entry_price,
                    stop_order_id=stop_loss_id,
                    take_profit_order_id=take_profit_id,
                    stop_price=stop_loss_price,
                    stop_limit_price=None,
                    take_profit_price=take_profit_price,
                    time_in_force=tif,
                    outside_rth_stop=outside_rth,
                    outside_rth_tp=outside_rth,
                    status=OCAStatus.ACTIVE,
                    trade_id=trade_id,
                    trailing_stop_id=trailing_stop_id
                )
                db.session.add(oca_group_record)
                db.session.commit()
                logger.info(f"[TBUS] OCAGroup record created: ID={oca_group_record.id}, group={oca_group_id_str}")

                from order_tracker_service import register_order
                if stop_loss_id:
                    register_order(
                        tiger_order_id=stop_loss_id,
                        symbol=symbol,
                        account_type='real',
                        role='stop_loss',
                        side=action,
                        quantity=quantity,
                        order_type='STP',
                        limit_price=None,
                        stop_price=stop_loss_price,
                        trade_id=trade_id,
                        trailing_stop_id=trailing_stop_id
                    )
                if take_profit_id:
                    register_order(
                        tiger_order_id=take_profit_id,
                        symbol=symbol,
                        account_type='real',
                        role='take_profit',
                        side=action,
                        quantity=quantity,
                        order_type='LMT',
                        limit_price=take_profit_price,
                        trade_id=trade_id,
                        trailing_stop_id=trailing_stop_id
                    )
            except Exception as db_e:
                db.session.rollback()
                logger.error(f"[TBUS] Failed to save OCAGroup record: {db_e}")

        if orders_created:
            warnings = []
            warnings.append("TBUS: SL managed by software trailing stop (no broker SL order).")
            if take_profit_price and not take_profit_id:
                warnings.append("Take profit order could not be created")
            warnings.append("TBUS: GTC LMT TP only active during regular hours. Soft Stop active for extended hours.")

            result = {
                'success': True,
                'order_id': stop_loss_id or take_profit_id,
                'stop_loss_order_id': stop_loss_id,
                'take_profit_order_id': take_profit_id,
                'oca_group': oca_group_id_str,
                'oca_group_record_id': oca_group_record.id if oca_group_record else None,
                'time_in_force': tif,
                'outside_rth_stop': outside_rth,
                'outside_rth_tp': outside_rth,
                'message': f'[TBUS] Protection created for {quantity} shares of {symbol}: {", ".join(orders_created)}',
                'warnings': warnings
            }
            return result
        else:
            return {'success': False, 'error': 'Failed to create any protection orders'}

    except Exception as e:
        logger.error(f"[TBUS] Error creating protection orders: {type(e).__name__}: {e}")
        import traceback
        logger.debug(f"[TBUS] Full traceback: {traceback.format_exc()}")
        return {'success': False, 'error': str(e)}


def modify_tbus_stop_price(
    tiger_client,
    order_id: str,
    symbol: str,
    quantity: int,
    new_stop_price: float,
    side: str = 'sell'
) -> Dict:
    """Modify TBUS stop loss price.
    
    TBUS uses STP orders (not STP_LMT), so we only modify aux_price.
    Tries modify_order first, falls back to cancel+create.
    
    Args:
        tiger_client: Initialized TigerClient instance
        order_id: Existing stop order ID
        symbol: Stock symbol
        quantity: Number of shares
        new_stop_price: New stop trigger price
        side: 'sell' for long positions, 'buy' for short positions
        
    Returns:
        dict with success status and order details
    """
    from tigeropen.common.util.contract_utils import stock_contract
    from tigeropen.common.util.order_utils import stop_order

    try:
        if not tiger_client.client or not tiger_client.client_config:
            return {'success': False, 'error': 'Tiger client not initialized'}

        clean_symbol = symbol.replace('[PAPER]', '').strip()
        new_stop_price = round(new_stop_price, 2)

        logger.info(f"[TBUS] Modifying stop: {clean_symbol} order {order_id} to ${new_stop_price}")

        try:
            order_obj = tiger_client.client.get_order(id=int(order_id))
            if order_obj:
                order_obj.aux_price = new_stop_price
                modify_result = tiger_client.client.modify_order(order_obj)
                if modify_result:
                    logger.info(f"[TBUS] Stop order {order_id} modified to ${new_stop_price}")
                    return {
                        'success': True,
                        'order_id': order_id,
                        'new_stop_price': new_stop_price,
                        'method': 'modify'
                    }
                else:
                    logger.warning(f"[TBUS] Modify returned no result, falling back to cancel+create")
        except Exception as modify_err:
            logger.warning(f"[TBUS] Modify failed: {modify_err}, falling back to cancel+create")

        cancel_result = tiger_client.cancel_order(order_id)
        if not cancel_result['success']:
            logger.error(f"[TBUS] Failed to cancel stop order {order_id}: {cancel_result.get('error')}")
            return {'success': False, 'error': f"Cancel failed: {cancel_result.get('error')}"}

        logger.info(f"[TBUS] Cancelled old stop order {order_id}")
        time_module.sleep(0.5)

        contract = stock_contract(symbol=clean_symbol, currency='USD')
        action = 'SELL' if side == 'sell' else 'BUY'

        sl_order = stop_order(
            account=tiger_client.client_config.account,
            contract=contract,
            action=action,
            quantity=abs(int(quantity)),
            aux_price=new_stop_price,
            time_in_force='GTC'
        )
        sl_order.outside_rth = False

        max_retries = 2
        last_error = None

        for attempt in range(max_retries):
            try:
                result = tiger_client.client.place_order(sl_order)
                if result and sl_order.id:
                    new_order_id = str(sl_order.id)
                    logger.info(f"[TBUS] New GTC STP stop order {new_order_id} at ${new_stop_price}")
                    return {
                        'success': True,
                        'old_order_id': order_id,
                        'new_order_id': new_order_id,
                        'new_stop_price': new_stop_price,
                        'method': 'cancel_create'
                    }
                else:
                    last_error = 'Place order returned no result'
            except Exception as retry_err:
                last_error = str(retry_err)
                logger.warning(f"[TBUS] Attempt {attempt + 1}/{max_retries} error: {last_error}")

            if attempt < max_retries - 1:
                time_module.sleep(1.0)

        logger.error(f"[TBUS] CRITICAL: Failed to create new stop after {max_retries} attempts for {clean_symbol}")
        return {
            'success': False,
            'error': f'Failed after {max_retries} retries: {last_error}',
            'critical': True,
            'old_order_cancelled': True
        }

    except Exception as e:
        logger.error(f"[TBUS] Error modifying stop price: {e}")
        return {'success': False, 'error': str(e)}
