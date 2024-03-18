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
    msg = data.get("msg")
    attr = data.get("attr", {})
    if data["msg"] == "Slow query" and "queryHash" in attr:
        return {
            "hash": attr["queryHash"],
            "durationMillis": attr.get("durationMillis"),
            "ns": attr.get("ns"),
            "planSummary": attr.get("planSummary"),
            "command": attr.get("command")
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

def process_slow_log(data, db, limit, char_limit, count, sort, query_condition):
    hashes = set()
    result = {}

    for line in data:
        parsed_data = parse_log_line(line)
        if parsed_data:
            try:
                query_data = extract_query_data(parsed_data)
            except Exception as e:
                print(f'failed to extract query data: {e}')
            if query_data:
                hash_key = query_data["hash"]
                if hash_key not in hashes:
                    hashes.add(hash_key)
                try:
                    create_or_update_result(result, query_data)
                except Exception as e:
                    print(f'failed to create or update result: {e}')

    if os.path.exists(db):
        os.remove(db)

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
        FROM results WHERE count >= {count}
        {query_condition}
        ORDER BY {sort} DESC LIMIT {limit};
    ''')

    rows = cursor.fetchall()
    table_data = [column_names] + list(rows)

    print(tabulate(table_data, headers="firstrow", tablefmt="fancy_grid"))
    connection.close()

def print_sql_info(db, limit, char_limit, count, sort, query_condition):
    print(f'sqlite3 {db}')
    print('.mode column')
    print(f"SELECT hash, durationMillis, count, avgDurationMillis, ns, SUBSTR(planSummary, 1, {char_limit}), "
          f"SUBSTR(command, 1, {char_limit}) FROM results WHERE count >= {count}{query_condition} ORDER BY {sort} DESC LIMIT {limit};")
    print(f"SELECT hash, durationMillis, count, avgDurationMillis, ns, planSummary, command FROM results ORDER BY {sort} DESC;")
    exit()

def main():
    parser = argparse.ArgumentParser(description="Process MongoDB slow log file",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("log", nargs="?", default="/var/log/mongodb/mongod.log", help="Path to the mongodb log file")
    parser.add_argument("--db", default="./mongo_slow_logs.sql", help="Path to the SQLite database file")
    parser.add_argument("--limit", default=10, type=int, help="Limit the number of rows in SQL output")
    parser.add_argument("--char-limit", default=100, type=int, help="Limit the number of characters in SQL strings output")
    parser.add_argument("--count", default=1, type=int, help="Filter queries that appear less than this count in the log")
    parser.add_argument("--collscan", action="store_true", help="Filter queries with COLLSCAN in the results")
    parser.add_argument("--sort", default="avgDurationMillis", choices=["avgDurationMillis", "durationMillis", "count"], help="Sort field")
    parser.add_argument("--sql", action="store_true", help="Print useful SQL information and exit")

    args = parser.parse_args()

    query_condition = ' AND planSummary LIKE \'%COLLSCAN%\'' if args.collscan else ''

    if args.sql:
        print_sql_info(args.db, args.limit, args.char_limit, args.count, args.sort, query_condition)

    try:
        with open(args.log, "r") as log_file:
            process_slow_log(log_file, args.db, args.limit, args.char_limit, args.count, args.sort, query_condition)

    except FileNotFoundError:
        print(f"The file '{args.log}' does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
