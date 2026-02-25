"""
Fetch production data via Flask app and insert into dev database.
This script uses the app context to execute raw SQL.
Run: python scripts/fetch_and_insert.py
"""
import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor

DB_URL = os.environ.get('DATABASE_URL')
TARGET_DATE = '2026-02-20'

def get_conn():
    return psycopg2.connect(DB_URL)

def clear_feb20_order_tracker(conn):
    """Remove existing Feb 20 order_tracker rows to avoid conflicts."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM order_tracker WHERE created_at::date = %s", (TARGET_DATE,))
        deleted = cur.rowcount
        conn.commit()
        if deleted:
            print(f"  Cleared {deleted} existing Feb 20 order_tracker rows")

def insert_from_json_file(conn, table_name, json_file, columns, conflict_action='DO NOTHING'):
    """Insert data from a JSON file into a table."""
    if not os.path.exists(json_file):
        print(f"  {table_name}: File {json_file} not found")
        return 0
    
    with open(json_file, 'r') as f:
        rows = json.load(f)
    
    if not rows:
        return 0
    
    col_list = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))
    sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders}) ON CONFLICT (id) {conflict_action}"
    
    inserted = 0
    with conn.cursor() as cur:
        for r in rows:
            vals = tuple(r.get(c) for c in columns)
            try:
                cur.execute(sql, vals)
                inserted += 1
            except Exception as e:
                conn.rollback()
                print(f"  Error inserting {table_name} id={r.get('id')}: {e}")
    
    conn.commit()
    
    with conn.cursor() as cur:
        cur.execute(f"SELECT setval(pg_get_serial_sequence('{table_name}', 'id'), GREATEST((SELECT MAX(id) FROM {table_name}), 1))")
        conn.commit()
    
    print(f"  {table_name}: Inserted {inserted}/{len(rows)} rows")
    return inserted

def main():
    import glob
    
    print(f"=== Importing Feb 20 production data ===\n")
    conn = get_conn()
    
    tables = {
        'closed_position': [
            'id', 'symbol', 'account_type', 'exit_order_id', 'exit_time',
            'exit_price', 'exit_quantity', 'side', 'exit_method',
            'total_pnl', 'total_pnl_pct', 'avg_entry_price', 'trailing_stop_id',
            'created_at', 'commission', 'exit_indicator'
        ],
        'signal_log': [
            'id', 'raw_signal', 'parsed_successfully', 'error_message',
            'ip_address', 'created_at', 'trade_id', 'endpoint',
            'account_type', 'tiger_status', 'tiger_order_id', 'tiger_response'
        ],
        'trade': [
            'id', 'symbol', 'side', 'quantity', 'price', 'order_type',
            'status', 'tiger_order_id', 'signal_data', 'error_message',
            'created_at', 'updated_at', 'filled_price', 'filled_quantity',
            'stop_loss_price', 'take_profit_price', 'stop_loss_order_id',
            'take_profit_order_id', 'trading_session', 'outside_rth',
            'is_close_position', 'reference_price', 'tiger_response',
            'needs_auto_protection', 'protection_info', 'account_type',
            'entry_avg_cost', 'parent_entry_order_id'
        ],
        'order_tracker': [
            'id', 'tiger_order_id', 'parent_order_id', 'symbol',
            'account_type', 'role', 'side', 'quantity', 'order_type',
            'limit_price', 'stop_price', 'status', 'filled_quantity',
            'avg_fill_price', 'realized_pnl', 'commission', 'fill_time',
            'trade_id', 'trailing_stop_id', 'closed_position_id',
            'created_at', 'updated_at', 'oca_group_id', 'leg_role',
            'fill_source'
        ],
    }
    
    total = 0
    for table_name, columns in tables.items():
        files = sorted(glob.glob(f'/tmp/prod_data/{table_name}_*.json'))
        if not files:
            print(f"{table_name}: No data files found, skipping")
            continue
        
        print(f"Processing {table_name} ({len(files)} files)...")
        for f in files:
            count = insert_from_json_file(conn, table_name, f, columns)
            total += count
    
    conn.close()
    print(f"\n=== Done! Total: {total} rows ===")

if __name__ == '__main__':
    main()
