"""
Bulk insert trade data into development database.
Reads formatted tuple lines from stdin or file and inserts into trade table.
"""
import os
import sys
import re
import psycopg2

DB_URL = os.environ.get('DATABASE_URL')

INSERT_COLS = """id, symbol, side, quantity, price, order_type, status, tiger_order_id,
created_at, updated_at, error_message, filled_price, filled_quantity,
stop_loss_price, take_profit_price, stop_loss_order_id, take_profit_order_id,
trading_session, outside_rth, is_close_position, reference_price, entry_avg_cost,
account_type, needs_auto_protection, parent_entry_order_id"""

def insert_batch(conn, values_lines, batch_size=25):
    total = 0
    for i in range(0, len(values_lines), batch_size):
        batch = values_lines[i:i+batch_size]
        values_str = ',\n'.join(batch)
        sql = f"INSERT INTO trade ({INSERT_COLS}) VALUES\n{values_str}\nON CONFLICT (id) DO NOTHING;"
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            total += len(batch)
        except Exception as e:
            conn.rollback()
            print(f"Error in batch starting at line {i}: {e}")
            for line in batch:
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"INSERT INTO trade ({INSERT_COLS}) VALUES\n{line}\nON CONFLICT (id) DO NOTHING;")
                    conn.commit()
                    total += 1
                except Exception as e2:
                    conn.rollback()
                    id_match = re.match(r'\((\d+),', line)
                    print(f"  Skip id={id_match.group(1) if id_match else '?'}: {e2}")
    return total

def main():
    if len(sys.argv) < 2:
        print("Usage: python bulk_insert_trades.py <data_file>")
        sys.exit(1)
    
    data_file = sys.argv[1]
    with open(data_file, 'r') as f:
        lines = f.readlines()
    
    values_lines = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith('v') or line.startswith('"v"'):
            continue
        line = line.strip('"')
        if line.startswith('('):
            values_lines.append(line)
    
    print(f"Parsed {len(values_lines)} value rows from {data_file}")
    
    conn = psycopg2.connect(DB_URL)
    total = insert_batch(conn, values_lines)
    
    with conn.cursor() as cur:
        cur.execute("SELECT setval(pg_get_serial_sequence('trade', 'id'), GREATEST((SELECT MAX(id) FROM trade), 1))")
    conn.commit()
    conn.close()
    
    print(f"Inserted {total} rows")

if __name__ == '__main__':
    main()
