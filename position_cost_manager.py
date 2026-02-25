"""
Position Cost Manager - Tracks average cost basis for positions across trading cycles.

This module manages the PositionCost table to:
1. Track real-time average cost as positions are built up
2. Capture the average cost when positions are closed
3. Handle new trading cycles with fresh cost basis
"""

import logging
from app import db
from models import PositionCost, Trade

logger = logging.getLogger(__name__)


def get_or_create_position_cost(symbol: str, account_type: str = 'real') -> PositionCost:
    """Get existing position cost record or create new one."""
    position_cost = PositionCost.query.filter_by(
        symbol=symbol,
        account_type=account_type
    ).first()
    
    if not position_cost:
        position_cost = PositionCost(
            symbol=symbol,
            account_type=account_type,
            quantity=0,
            total_cost_basis=0,
            average_cost=None
        )
        db.session.add(position_cost)
        db.session.flush()
        logger.info(f"Created new PositionCost record for {symbol} ({account_type})")
    
    return position_cost


def update_position_cost_on_fill(symbol: str, side: str, quantity: float, 
                                  fill_price: float, account_type: str = 'real') -> float:
    """
    Update position cost when an order is filled.
    
    Args:
        symbol: Stock symbol
        side: 'buy' or 'sell'
        quantity: Number of shares filled
        fill_price: Average fill price
        account_type: 'real' or 'paper'
    
    Returns:
        Current average cost (useful for recording in Trade on sells)
    """
    try:
        position_cost = get_or_create_position_cost(symbol, account_type)
        
        if side.lower() == 'buy':
            position_cost.update_on_buy(quantity, fill_price)
            avg_cost_val = position_cost.average_cost or 0
            logger.info(f"Updated {symbol} ({account_type}) on BUY: qty={position_cost.quantity}, "
                       f"avg_cost=${avg_cost_val:.2f}")
            return position_cost.average_cost
        
        elif side.lower() == 'sell':
            avg_cost_at_sale = position_cost.update_on_sell(quantity)
            if position_cost.quantity <= 0:
                final_cost = avg_cost_at_sale or 0
                logger.info(f"Position {symbol} ({account_type}) fully closed. Final avg_cost was ${final_cost:.2f}")
            else:
                avg_cost_val = position_cost.average_cost or 0
                logger.info(f"Partial close {symbol} ({account_type}): remaining qty={position_cost.quantity}, "
                           f"avg_cost=${avg_cost_val:.2f}")
            return avg_cost_at_sale
        
        db.session.commit()
        return position_cost.average_cost
        
    except Exception as e:
        logger.error(f"Error updating position cost for {symbol}: {str(e)}")
        db.session.rollback()
        return None


def sync_position_costs_from_api(tiger_client, account_type: str = 'real'):
    """
    Sync position costs from Tiger API to ensure our records match reality.
    Call this on startup or periodically to handle any missed fills.
    
    Args:
        tiger_client: TigerClient or TigerPaperClient instance
        account_type: 'real' or 'paper'
    """
    try:
        result = tiger_client.get_positions()
        if not result.get('success'):
            logger.error(f"Failed to get positions for sync: {result.get('error')}")
            return
        
        positions = result.get('positions', [])
        synced_symbols = set()
        
        for pos in positions:
            symbol = pos['symbol']
            api_quantity = pos['quantity']
            api_avg_cost = pos.get('average_cost', 0)
            
            if api_quantity == 0:
                continue
            
            position_cost = get_or_create_position_cost(symbol, account_type)
            
            if position_cost.quantity != api_quantity or position_cost.average_cost != api_avg_cost:
                logger.info(f"Syncing {symbol} ({account_type}): "
                           f"DB({position_cost.quantity}@${position_cost.average_cost or 0:.2f}) -> "
                           f"API({api_quantity}@${api_avg_cost:.2f})")
                
                position_cost.quantity = api_quantity
                position_cost.total_cost_basis = api_quantity * api_avg_cost
                position_cost.average_cost = api_avg_cost
            
            synced_symbols.add(symbol)
        
        existing_records = PositionCost.query.filter_by(account_type=account_type).all()
        for record in existing_records:
            if record.symbol not in synced_symbols and record.quantity > 0:
                logger.info(f"Position {record.symbol} ({account_type}) no longer in API, marking as closed")
                record.quantity = 0
                record.total_cost_basis = 0
        
        db.session.commit()
        logger.info(f"Position cost sync complete for {account_type} account: {len(synced_symbols)} positions")
        
    except Exception as e:
        logger.error(f"Error syncing position costs: {str(e)}")
        db.session.rollback()


def get_avg_cost_for_symbol(symbol: str, account_type: str = 'real') -> float:
    """Get current average cost for a symbol, or None if no position."""
    position_cost = PositionCost.query.filter_by(
        symbol=symbol,
        account_type=account_type
    ).first()
    
    if position_cost and position_cost.average_cost:
        return position_cost.average_cost
    return None


def record_entry_cost_on_trade(trade: Trade, avg_cost: float):
    """Record the average cost on a trade record (typically for sells/closes)."""
    if trade and avg_cost:
        trade.entry_avg_cost = avg_cost
        logger.info(f"Recorded entry_avg_cost=${avg_cost:.2f} on trade {trade.id} ({trade.symbol})")
