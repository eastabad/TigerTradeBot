#!/usr/bin/env python3
"""
Production Data Cleanup Script
清理生产环境历史数据脚本

This script clears historical trading data from the production database.
Run this after deployment to start fresh.

Usage:
    python cleanup_production_data.py

Tables cleaned:
- closed_position (已平仓记录)
- entry_signal_record (入场信号记录 - 交易分析)
- order_tracker (订单跟踪)
- trailing_stop_position (跟踪止损仓位)
- trailing_stop_log (跟踪止损日志)
- completed_trade (已完成交易)
- position_cost (仓位成本)
- signal_log (信号日志)
- trade (交易记录)
"""

import os
import sys

def main():
    print("=" * 60)
    print("🗑️  Production Data Cleanup Script")
    print("=" * 60)
    print()
    print("This will DELETE the following data:")
    print("  - ClosedPosition (已平仓记录)")
    print("  - EntrySignalRecord (入场信号记录)")
    print("  - OrderTracker (订单跟踪)")
    print("  - TrailingStopPosition (跟踪止损仓位)")
    print("  - TrailingStopLog (跟踪止损日志)")
    print("  - CompletedTrade (已完成交易)")
    print("  - PositionCost (仓位成本)")
    print("  - SignalLog (信号日志)")
    print("  - Trade (交易记录)")
    print()
    
    confirm = input("⚠️  Are you sure? Type 'YES' to confirm: ")
    if confirm != "YES":
        print("❌ Cancelled.")
        sys.exit(0)
    
    print()
    print("🔄 Starting cleanup...")
    
    from app import app, db
    from models import (
        ClosedPosition, 
        EntrySignalRecord, 
        OrderTracker,
        TrailingStopPosition, 
        TrailingStopLog,
        CompletedTrade,
        PositionCost,
        SignalLog,
        Trade
    )
    
    with app.app_context():
        try:
            count_closed = db.session.query(ClosedPosition).count()
            db.session.query(ClosedPosition).delete()
            print(f"  ✅ ClosedPosition: deleted {count_closed} records")
            
            count_entry = db.session.query(EntrySignalRecord).count()
            db.session.query(EntrySignalRecord).delete()
            print(f"  ✅ EntrySignalRecord: deleted {count_entry} records")
            
            count_tracker = db.session.query(OrderTracker).count()
            db.session.query(OrderTracker).delete()
            print(f"  ✅ OrderTracker: deleted {count_tracker} records")
            
            count_trailing = db.session.query(TrailingStopPosition).count()
            db.session.query(TrailingStopPosition).delete()
            print(f"  ✅ TrailingStopPosition: deleted {count_trailing} records")
            
            count_logs = db.session.query(TrailingStopLog).count()
            db.session.query(TrailingStopLog).delete()
            print(f"  ✅ TrailingStopLog: deleted {count_logs} records")
            
            count_completed = db.session.query(CompletedTrade).count()
            db.session.query(CompletedTrade).delete()
            print(f"  ✅ CompletedTrade: deleted {count_completed} records")
            
            count_cost = db.session.query(PositionCost).count()
            db.session.query(PositionCost).delete()
            print(f"  ✅ PositionCost: deleted {count_cost} records")
            
            count_signal = db.session.query(SignalLog).count()
            db.session.query(SignalLog).delete()
            print(f"  ✅ SignalLog: deleted {count_signal} records")
            
            count_trade = db.session.query(Trade).count()
            db.session.query(Trade).delete()
            print(f"  ✅ Trade: deleted {count_trade} records")
            
            db.session.commit()
            
            total = (count_closed + count_entry + count_tracker + count_trailing + 
                     count_logs + count_completed + count_cost + count_signal + count_trade)
            
            print()
            print("=" * 60)
            print(f"🎉 Cleanup complete! Total {total} records deleted.")
            print("=" * 60)
            
        except Exception as e:
            db.session.rollback()
            print(f"❌ Error: {e}")
            sys.exit(1)

if __name__ == "__main__":
    main()
