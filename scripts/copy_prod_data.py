"""
Copy Feb 20, 2026 Tiger trading data from production to development database.
Run this script within the Flask app context.
"""
import os
import sys
import json
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

PROD_DB_URL = os.environ.get('PRODUCTION_DATABASE_URL') or os.environ.get('DATABASE_URL')
DEV_DB_URL = os.environ.get('DATABASE_URL')

TARGET_DATE = '2026-02-20'

TABLES_CONFIG = [
    {
        'table': 'closed_position',
        'date_filter': "exit_time::date = %s",
        'columns': [
            'id', 'symbol', 'account_type', 'exit_order_id', 'exit_time',
            'exit_price', 'exit_quantity', 'side', 'exit_method',
            'exit_signal_content', 'total_pnl', 'total_pnl_pct',
            'avg_entry_price', 'trailing_stop_id', 'created_at',
            'commission', 'exit_indicator'
        ],
    },
    {
        'table': 'signal_log',
        'date_filter': "created_at::date = %s",
        'columns': [
            'id', 'raw_signal', 'parsed_successfully', 'error_message',
            'ip_address', 'created_at', 'trade_id', 'endpoint',
            'account_type', 'tiger_status', 'tiger_order_id', 'tiger_response'
        ],
    },
    {
        'table': 'trade',
        'date_filter': "created_at::date = %s",
        'columns': [
            'id', 'symbol', 'side', 'quantity', 'price', 'order_type',
            'status', 'tiger_order_id', 'signal_data', 'error_message',
            'created_at', 'updated_at', 'filled_price', 'filled_quantity',
            'stop_loss_price', 'take_profit_price', 'stop_loss_order_id',
            'take_profit_order_id', 'trading_session', 'outside_rth',
            'is_close_position', 'reference_price', 'tiger_response',
            'needs_auto_protection', 'protection_info', 'account_type',
            'entry_avg_cost', 'parent_entry_order_id'
        ],
    },
    {
        'table': 'order_tracker',
        'date_filter': "created_at::date = %s",
        'columns': [
            'id', 'tiger_order_id', 'parent_order_id', 'symbol',
            'account_type', 'role', 'side', 'quantity', 'order_type',
            'limit_price', 'stop_price', 'status', 'filled_quantity',
            'avg_fill_price', 'realized_pnl', 'commission', 'fill_time',
            'trade_id', 'trailing_stop_id', 'closed_position_id',
            'created_at', 'updated_at', 'oca_group_id', 'leg_role',
            'fill_source'
        ],
    },
]


def copy_table(prod_conn, dev_conn, config):
    table = config['table']
    columns = config['columns']
    date_filter = config['date_filter']
    
    col_list = ', '.join(columns)
    select_sql = f"SELECT {col_list} FROM {table} WHERE {date_filter} ORDER BY id"
    
    with prod_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(select_sql, (TARGET_DATE,))
        rows = cur.fetchall()
    
    if not rows:
        print(f"  {table}: No data found for {TARGET_DATE}")
        return 0
    
    print(f"  {table}: Fetched {len(rows)} rows from production")
    
    with dev_conn.cursor() as cur:
        existing_ids = set()
        cur.execute(f"SELECT id FROM {table} WHERE id = ANY(%s)", 
                    ([r['id'] for r in rows],))
        for row in cur.fetchall():
            existing_ids.add(row[0])
        
        new_rows = [r for r in rows if r['id'] not in existing_ids]
        if not new_rows:
            print(f"  {table}: All {len(rows)} rows already exist in dev, skipping")
            return 0
        
        if existing_ids:
            print(f"  {table}: Skipping {len(existing_ids)} existing rows, inserting {len(new_rows)} new")
        
        placeholders = ', '.join(['%s'] * len(columns))
        insert_sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING"
        
        batch_size = 100
        inserted = 0
        for i in range(0, len(new_rows), batch_size):
            batch = new_rows[i:i+batch_size]
            for row in batch:
                values = tuple(row[col] for col in columns)
                try:
                    cur.execute(insert_sql, values)
                    inserted += 1
                except Exception as e:
                    print(f"  {table}: Error inserting row id={row['id']}: {e}")
                    dev_conn.rollback()
                    continue
        
        dev_conn.commit()
        
        cur.execute(f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), GREATEST((SELECT MAX(id) FROM {table}), 1))")
        dev_conn.commit()
        
        print(f"  {table}: Inserted {inserted} rows into dev")
        return inserted


def main():
    print(f"=== Copying Tiger data for {TARGET_DATE} from production to development ===\n")
    
    prod_conn = psycopg2.connect(PROD_DB_URL, options="-c default_transaction_read_only=on")
    dev_conn = psycopg2.connect(DEV_DB_URL)
    
    try:
        total = 0
        for config in TABLES_CONFIG:
            count = copy_table(prod_conn, dev_conn, config)
            total += count
        
        print(f"\n=== Done! Total rows inserted: {total} ===")
    finally:
        prod_conn.close()
        dev_conn.close()


if __name__ == '__main__':
    main()
