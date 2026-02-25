import logging
from datetime import datetime
from app import db

logger = logging.getLogger(__name__)

_last_sync_time = {}
_last_sync_error = {}


def sync_holdings(account_type='real'):
    """Fetch current positions from Tiger API and upsert into TigerHolding table."""
    from models import TigerHolding
    global _last_sync_time, _last_sync_error
    
    try:
        if account_type == 'paper':
            from tiger_client import TigerPaperClient
            client = TigerPaperClient()
        else:
            from tiger_client import TigerClient
            client = TigerClient()
        
        result = client.get_positions()
        
        if not result.get('success'):
            error_msg = result.get('error', 'Unknown error')
            _last_sync_error[account_type] = error_msg
            logger.warning(f"Failed to sync {account_type} holdings: {error_msg}")
            return {'success': False, 'error': error_msg}
        
        positions = result.get('positions', [])
        now = datetime.utcnow()
        
        synced_symbols = set()
        updated = 0
        created = 0
        
        for pos in positions:
            symbol = pos.get('symbol', '')
            if not symbol:
                continue
            
            synced_symbols.add(symbol)
            
            holding = TigerHolding.query.filter_by(
                account_type=account_type, symbol=symbol
            ).first()
            
            if holding:
                holding.quantity = pos.get('quantity', 0)
                holding.average_cost = pos.get('average_cost')
                holding.market_value = pos.get('market_value')
                holding.unrealized_pnl = pos.get('unrealized_pnl')
                upnl_pct = pos.get('unrealized_pnl_pct')
                if upnl_pct is not None:
                    holding.unrealized_pnl_pct = upnl_pct
                elif holding.average_cost and holding.average_cost > 0 and holding.unrealized_pnl is not None:
                    holding.unrealized_pnl_pct = (holding.unrealized_pnl / (holding.average_cost * abs(holding.quantity))) * 100
                holding.latest_price = pos.get('market_price')
                holding.sec_type = pos.get('sec_type')
                holding.currency = pos.get('currency')
                holding.multiplier = pos.get('multiplier', 1)
                holding.salable_qty = pos.get('salable_qty')
                holding.synced_at = now
                updated += 1
            else:
                avg_cost = pos.get('average_cost', 0)
                qty = pos.get('quantity', 0)
                upnl = pos.get('unrealized_pnl')
                pnl_pct = pos.get('unrealized_pnl_pct')
                if pnl_pct is None and avg_cost and avg_cost > 0 and upnl is not None:
                    pnl_pct = (upnl / (avg_cost * abs(qty))) * 100
                
                holding = TigerHolding(
                    account_type=account_type,
                    symbol=symbol,
                    quantity=qty,
                    average_cost=avg_cost,
                    market_value=pos.get('market_value'),
                    unrealized_pnl=upnl,
                    unrealized_pnl_pct=pnl_pct,
                    latest_price=pos.get('market_price'),
                    sec_type=pos.get('sec_type'),
                    currency=pos.get('currency'),
                    multiplier=pos.get('multiplier', 1),
                    salable_qty=pos.get('salable_qty'),
                    synced_at=now,
                )
                db.session.add(holding)
                created += 1
        
        stale = TigerHolding.query.filter(
            TigerHolding.account_type == account_type,
            ~TigerHolding.symbol.in_(synced_symbols) if synced_symbols else True
        ).all()
        removed = 0
        for h in stale:
            if h.symbol not in synced_symbols:
                db.session.delete(h)
                removed += 1
        
        db.session.commit()
        
        _last_sync_time[account_type] = now
        _last_sync_error[account_type] = None
        
        logger.info(f"Holdings sync [{account_type}]: {created} new, {updated} updated, {removed} removed, {len(positions)} total")
        
        return {
            'success': True,
            'created': created,
            'updated': updated,
            'removed': removed,
            'total': len(positions),
            'synced_at': now.isoformat(),
        }
    
    except Exception as e:
        db.session.rollback()
        _last_sync_error[account_type] = str(e)
        logger.error(f"Holdings sync error [{account_type}]: {str(e)}")
        return {'success': False, 'error': str(e)}


def sync_all_holdings():
    """Sync holdings for both real and paper accounts."""
    results = {}
    for acct in ['real', 'paper']:
        results[acct] = sync_holdings(acct)
    return results


def get_sync_status():
    """Get last sync times and errors."""
    return {
        'real': {
            'last_sync': _last_sync_time.get('real'),
            'error': _last_sync_error.get('real'),
        },
        'paper': {
            'last_sync': _last_sync_time.get('paper'),
            'error': _last_sync_error.get('paper'),
        },
    }
