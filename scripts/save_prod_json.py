"""
Helper to save production query results as JSON files.
Run queries via execute_sql_tool, then use this to process and save.
"""
import json
import os
import sys
import re

def fix_sql_json(raw):
    """Fix JSON from SQL tool output (CSV-escaped double quotes)."""
    s = raw.strip()
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    s = s.replace('""', '"')
    return json.loads(s)

def main():
    if len(sys.argv) < 3:
        print("Usage: python save_prod_json.py <table_name> <batch_num> < input_file")
        sys.exit(1)
    table = sys.argv[1]
    batch = sys.argv[2]
    raw = sys.stdin.read()
    
    lines = raw.strip().split('\n')
    json_line = '\n'.join(lines[1:]) if lines[0].strip() == 'json_agg' else '\n'.join(lines)
    
    data = fix_sql_json(json_line)
    
    os.makedirs('/tmp/prod_data', exist_ok=True)
    outfile = f'/tmp/prod_data/{table}_{batch}.json'
    with open(outfile, 'w') as f:
        json.dump(data, f)
    
    print(f"Saved {len(data)} rows to {outfile}")

if __name__ == '__main__':
    main()
