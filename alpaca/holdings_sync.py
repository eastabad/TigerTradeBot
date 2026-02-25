import logging
from datetime import datetime
from app import db

logger = logging.getLogger(__name__)

_last_sync_time = None
_last_sync_error = None


def sync_holdings():
    from alpaca.models import AlpacaHolding
    from alpaca.client import AlpacaClient
    global _last_sync_time, _last_sync_error

    try:
        client = AlpacaClient()
        positions = client.get_positions()

        if not isinstance(positions, list):
            error_msg = f"Unexpected response type: {type(positions)}"
            _last_sync_error = error_msg
            logger.warning(f"Failed to sync Alpaca holdings: {error_msg}")
            return {'success': False, 'error': error_msg}

        now = datetime.utcnow()
        synced_symbols = set()
        updated = 0
        created = 0

        for pos in positions:
            symbol = pos.get('symbol', '')
            if not symbol:
                continue

            synced_symbols.add(symbol)

            qty = float(pos.get('qty', 0) or 0)
            avg_cost = float(pos.get('avg_entry_price', 0) or 0)
            market_value = float(pos.get('market_value', 0) or 0)
            cost_basis = float(pos.get('cost_basis', 0) or 0)
            unrealized_pnl = float(pos.get('unrealized_pl', 0) or 0)
            unrealized_pnl_pct = float(pos.get('unrealized_plpc', 0) or 0) * 100
            current_price = float(pos.get('current_price', 0) or 0)
            lastday_price = float(pos.get('lastday_price', 0) or 0)
            change_today = float(pos.get('change_today', 0) or 0) * 100

            holding = AlpacaHolding.query.filter_by(symbol=symbol).first()

            if holding:
                holding.quantity = qty
                holding.average_cost = avg_cost
                holding.market_value = market_value
                holding.cost_basis = cost_basis
                holding.unrealized_pnl = unrealized_pnl
                holding.unrealized_pnl_pct = unrealized_pnl_pct
                holding.current_price = current_price
                holding.lastday_price = lastday_price
                holding.change_today = change_today
                holding.asset_class = pos.get('asset_class')
                holding.exchange = pos.get('exchange')
                holding.side = pos.get('side')
                holding.synced_at = now
                updated += 1
            else:
                holding = AlpacaHolding(
                    symbol=symbol,
                    quantity=qty,
                    average_cost=avg_cost,
                    market_value=market_value,
                    cost_basis=cost_basis,
                    unrealized_pnl=unrealized_pnl,
                    unrealized_pnl_pct=unrealized_pnl_pct,
                    current_price=current_price,
                    lastday_price=lastday_price,
                    change_today=change_today,
                    asset_class=pos.get('asset_class'),
                    exchange=pos.get('exchange'),
                    side=pos.get('side'),
                    synced_at=now,
                )
                db.session.add(holding)
                created += 1

        stale = AlpacaHolding.query.filter(
            ~AlpacaHolding.symbol.in_(synced_symbols) if synced_symbols else AlpacaHolding.id > 0
        ).all()
        removed = 0
        for h in stale:
            if h.symbol not in synced_symbols:
                db.session.delete(h)
                removed += 1

        db.session.commit()

        _last_sync_time = now
        _last_sync_error = None

        logger.info(f"Alpaca holdings sync: {created} new, {updated} updated, {removed} removed, {len(positions)} total")

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
        _last_sync_error = str(e)
        logger.error(f"Alpaca holdings sync error: {str(e)}")
        return {'success': False, 'error': str(e)}


def get_sync_status():
    return {
        'last_sync': _last_sync_time,
        'error': _last_sync_error,
    }
