"""
Migrate Feb 20 production data to development database.
Reads JSON data from /tmp/prod_data/ files and inserts into dev DB.
"""
import os
import json
import glob
import psycopg2

DB_URL = os.environ.get('DATABASE_URL')

def load_files(table_name):
    pattern = f'/tmp/prod_data/{table_name}_*.json'
    files = sorted(glob.glob(pattern))
    all_rows = []
    for f in files:
        with open(f, 'r') as fp:
            data = json.load(fp)
            if isinstance(data, list):
                all_rows.extend(data)
    return all_rows

def insert_closed_position(conn, rows):
    if not rows:
        return
    cols = ['id', 'symbol', 'account_type', 'exit_order_id', 'exit_time',
            'exit_price', 'exit_quantity', 'side', 'exit_method',
            'total_pnl', 'total_pnl_pct', 'avg_entry_price', 'trailing_stop_id',
            'created_at', 'commission', 'exit_indicator']
    col_list = ', '.join(cols)
    placeholders = ', '.join(['%s'] * len(cols))
    sql = f"""INSERT INTO closed_position ({col_list}) 
              VALUES ({placeholders}) 
              ON CONFLICT (id) DO NOTHING"""
    with conn.cursor() as cur:
        for r in rows:
            vals = []
            for c in cols:
                v = r.get(c)
                vals.append(v)
            cur.execute(sql, tuple(vals))
        conn.commit()
        cur.execute("SELECT setval(pg_get_serial_sequence('closed_position', 'id'), GREATEST((SELECT MAX(id) FROM closed_position), 1))")
        conn.commit()
    print(f"  closed_position: processed {len(rows)} rows")

def insert_signal_log(conn, rows):
    if not rows:
        return
    cols = ['id', 'raw_signal', 'parsed_successfully', 'error_message',
            'ip_address', 'created_at', 'trade_id', 'endpoint',
            'account_type', 'tiger_status', 'tiger_order_id', 'tiger_response']
    col_list = ', '.join(cols)
    placeholders = ', '.join(['%s'] * len(cols))
    sql = f"""INSERT INTO signal_log ({col_list}) 
              VALUES ({placeholders}) 
              ON CONFLICT (id) DO NOTHING"""
    with conn.cursor() as cur:
        for r in rows:
            vals = [r.get(c) for c in cols]
            cur.execute(sql, tuple(vals))
        conn.commit()
        cur.execute("SELECT setval(pg_get_serial_sequence('signal_log', 'id'), GREATEST((SELECT MAX(id) FROM signal_log), 1))")
        conn.commit()
    print(f"  signal_log: processed {len(rows)} rows")

def insert_trade(conn, rows):
    if not rows:
        return
    cols = ['id', 'symbol', 'side', 'quantity', 'price', 'order_type',
            'status', 'tiger_order_id', 'signal_data', 'error_message',
            'created_at', 'updated_at', 'filled_price', 'filled_quantity',
            'stop_loss_price', 'take_profit_price', 'stop_loss_order_id',
            'take_profit_order_id', 'trading_session', 'outside_rth',
            'is_close_position', 'reference_price', 'tiger_response',
            'needs_auto_protection', 'protection_info', 'account_type',
            'entry_avg_cost', 'parent_entry_order_id']
    col_list = ', '.join(cols)
    placeholders = ', '.join(['%s'] * len(cols))
    sql = f"""INSERT INTO trade ({col_list}) 
              VALUES ({placeholders}) 
              ON CONFLICT (id) DO NOTHING"""
    with conn.cursor() as cur:
        for r in rows:
            vals = [r.get(c) for c in cols]
            cur.execute(sql, tuple(vals))
        conn.commit()
        cur.execute("SELECT setval(pg_get_serial_sequence('trade', 'id'), GREATEST((SELECT MAX(id) FROM trade), 1))")
        conn.commit()
    print(f"  trade: processed {len(rows)} rows")

def insert_order_tracker(conn, rows):
    if not rows:
        return
    cols = ['id', 'tiger_order_id', 'parent_order_id', 'symbol',
            'account_type', 'role', 'side', 'quantity', 'order_type',
            'limit_price', 'stop_price', 'status', 'filled_quantity',
            'avg_fill_price', 'realized_pnl', 'commission', 'fill_time',
            'trade_id', 'trailing_stop_id', 'closed_position_id',
            'created_at', 'updated_at', 'oca_group_id', 'leg_role',
            'fill_source']
    col_list = ', '.join(cols)
    placeholders = ', '.join(['%s'] * len(cols))
    sql = f"""INSERT INTO order_tracker ({col_list}) 
              VALUES ({placeholders}) 
              ON CONFLICT (id) DO NOTHING"""
    with conn.cursor() as cur:
        for r in rows:
            vals = [r.get(c) for c in cols]
            cur.execute(sql, tuple(vals))
        conn.commit()
        cur.execute("SELECT setval(pg_get_serial_sequence('order_tracker', 'id'), GREATEST((SELECT MAX(id) FROM order_tracker), 1))")
        conn.commit()
    print(f"  order_tracker: processed {len(rows)} rows")

def main():
    print("=== Migrating Feb 20 production data ===\n")
    conn = psycopg2.connect(DB_URL)
    
    tables = [
        ('closed_position', insert_closed_position),
        ('signal_log', insert_signal_log),
        ('trade', insert_trade),
        ('order_tracker', insert_order_tracker),
    ]
    
    for table_name, insert_fn in tables:
        rows = load_files(table_name)
        if rows:
            print(f"Loaded {len(rows)} {table_name} rows")
            insert_fn(conn, rows)
        else:
            print(f"No {table_name} data files found")
    
    conn.close()
    print("\n=== Migration complete ===")

if __name__ == '__main__':
    main()
