#!/usr/bin/env python3

import argparse
import os
import json
import sqlite3
from tabulate import tabulate

def parse_log_line(line):
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None

def extract_query_data(data):
    if data["msg"] == "Slow query" and "attr" in data and "queryHash" in data["attr"] and data["attr"]["queryHash"]:
        return {
            "hash": data["attr"]["queryHash"],
            "durationMillis": data["attr"]["durationMillis"],
            "ns": data["attr"]["ns"],
            "planSummary": data["attr"]["planSummary"],
            "command": data["attr"]["command"] if "command" in data["attr"] else None
        }
    return None

def create_or_update_result(result, query_data):
    hash_key = query_data["hash"]
    result["durationMillis_" + hash_key] = result.get("durationMillis_" + hash_key, 0) + query_data["durationMillis"]
    result["count_" + hash_key] = result.get("count_" + hash_key, 0) + 1
    result.setdefault("ns_" + hash_key, query_data["ns"])
    result.setdefault("planSummary_" + hash_key, []).append(query_data["planSummary"])
    result["avgDurationMillis_" + hash_key] = result["durationMillis_" + hash_key] / result["count_" + hash_key] if result["durationMillis_" + hash_key] and result["count_" + hash_key] > 0 else 0
    if query_data["command"] and "command_" + hash_key not in result:
        result["command_" + hash_key] = query_data["command"]

def process_slow_log(data, db, limit, char_limit, count, query_condition):
    hashes = set()
    result = {}

    for line in data:
        parsed_data = parse_log_line(line)
        if parsed_data:
            query_data = extract_query_data(parsed_data)
            if query_data:
                hash_key = query_data["hash"]
                if hash_key not in hashes:
                    hashes.add(hash_key)
                create_or_update_result(result, query_data)

    if os.path.exists(db):
        os.remove(db)
        print(f"Old database file {db} has been dropped")

    connection = sqlite3.connect(db)
    cursor = connection.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS results (
            hash TEXT PRIMARY KEY,
            durationMillis INTEGER,
            count INTEGER,
            avgDurationMillis REAL,
            ns STRING,
            planSummary String,
            command STRING
        )
    ''')

    for hash_key in hashes:
        cursor.execute('''
            INSERT OR REPLACE INTO results (hash, durationMillis, count, avgDurationMillis, ns, planSummary, command)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (hash_key, result.get("durationMillis_" + hash_key, 0), result.get("count_" + hash_key, 0), result.get("avgDurationMillis_" + hash_key, 0), result.get("ns_" + hash_key, ''),
              str(result.get("planSummary_" + hash_key, '')), str(result.get("command_" + hash_key, ''))))
        connection.commit()

    cursor.execute(f"PRAGMA table_info(results);")
    columns = cursor.fetchall()
    column_names = [column_info[1] for column_info in columns]

    cursor.execute(f'''
        SELECT hash, durationMillis, count, avgDurationMillis, ns, SUBSTR(planSummary, 1, {char_limit}),
               SUBSTR(command, 1, {char_limit})
        FROM results WHERE count >= {count}{query_condition}
        ORDER BY avgDurationMillis DESC LIMIT {limit};
    ''')

    rows = cursor.fetchall()
    table_data = [column_names] + list(rows)

    print(tabulate(table_data, headers="firstrow", tablefmt="fancy_grid"))
    connection.close()

def print_sql_info(db, limit, char_limit, count, query_condition):
    print(f'sqlite3 {db}')
    print('.mode column')
    print(f"SELECT hash, durationMillis, count, avgDurationMillis, ns, SUBSTR(planSummary, 1, {char_limit}), "
          f"SUBSTR(command, 1, {char_limit}) FROM results WHERE count >= {count}{query_condition} ORDER BY avgDurationMillis DESC LIMIT {limit};")
    print("SELECT hash, durationMillis, count, avgDurationMillis, ns, planSummary, command FROM results ORDER BY avgDurationMillis DESC;")
    exit()

def main():
    parser = argparse.ArgumentParser(description="Process MongoDB slow log file")

    parser.add_argument("log", nargs="?", default="/var/log/mongod.log", help="Path to the mongodb log file (default: /var/log/mongod.log)")
    parser.add_argument("--db", default="./mongo_slow_logs.sql", help="Path to the SQLite database file (default: ./mongodb-slow-log.sql)")
    parser.add_argument("--limit", default=10, type=int, help="Limit the number of rows in SQL output (default: 10)")
    parser.add_argument("--char-limit", default=100, type=int, help="Limit the number of characters in SQL strings output (default: 100)")
    parser.add_argument("--count", default=1, type=int, help="Filter queries that appear less than this count in the log (default: 1)")
    parser.add_argument("--collscan", action="store_true", help="Filter queries with COLLSCAN in the results (default: no filters)")
    parser.add_argument("--sql", action="store_true", help="Print useful SQL information and exit")

    args = parser.parse_args()

    query_condition = ' AND planSummary LIKE \'%COLLSCAN%\'' if args.collscan else ''

    if args.sql:
        print_sql_info(args.db, args.limit, args.char_limit, args.count, query_condition)

    try:
        with open(args.log, "r") as log_file:
            process_slow_log(log_file, args.db, args.limit, args.char_limit, args.count, query_condition)

    except FileNotFoundError:
        print(f"The file '{args.log}' does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()