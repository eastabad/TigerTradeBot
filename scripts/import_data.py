"""
Import production data from JSON files into development database.
Usage: python scripts/import_data.py
"""
import os
import sys
import json
import glob
import psycopg2
from datetime import datetime

DB_URL = os.environ.get('DATABASE_URL')

TABLE_COLUMNS = {
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

INSERT_ORDER = ['closed_position', 'trade', 'signal_log', 'order_tracker']


def load_json_files(table_name):
    pattern = f'/tmp/prod_data/{table_name}_*.json'
    files = sorted(glob.glob(pattern))
    all_rows = []
    for f in files:
        with open(f, 'r') as fp:
            data = json.load(fp)
            if isinstance(data, list):
                all_rows.extend(data)
    return all_rows


def insert_rows(conn, table_name, rows, columns):
    if not rows:
        print(f"  {table_name}: No data to insert")
        return 0

    with conn.cursor() as cur:
        existing_ids = set()
        cur.execute(f"SELECT id FROM {table_name} WHERE id = ANY(%s)",
                    ([r['id'] for r in rows],))
        for row in cur.fetchall():
            existing_ids.add(row[0])

        new_rows = [r for r in rows if r['id'] not in existing_ids]
        if not new_rows:
            print(f"  {table_name}: All {len(rows)} rows already exist, skipping")
            return 0

        col_list = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING"

        inserted = 0
        for row in new_rows:
            values = []
            for col in columns:
                v = row.get(col)
                values.append(v)
            try:
                cur.execute(sql, tuple(values))
                inserted += 1
            except Exception as e:
                conn.rollback()
                print(f"  {table_name}: Error inserting id={row.get('id')}: {e}")
                continue

        conn.commit()

        cur.execute(f"SELECT setval(pg_get_serial_sequence('{table_name}', 'id'), GREATEST((SELECT MAX(id) FROM {table_name}), 1))")
        conn.commit()

        print(f"  {table_name}: Inserted {inserted}/{len(new_rows)} new rows (skipped {len(existing_ids)} existing)")
        return inserted


def main():
    print("=== Importing production data into development ===\n")

    conn = psycopg2.connect(DB_URL)

    total = 0
    for table_name in INSERT_ORDER:
        columns = TABLE_COLUMNS[table_name]
        print(f"Processing {table_name}...")
        rows = load_json_files(table_name)
        print(f"  Loaded {len(rows)} rows from JSON files")
        count = insert_rows(conn, table_name, rows, columns)
        total += count

    conn.close()
    print(f"\n=== Done! Total rows inserted: {total} ===")


if __name__ == '__main__':
    main()
