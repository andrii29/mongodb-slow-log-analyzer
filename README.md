# MongoDB Slow Log Analyzer

MongoDB Slow Log Analyzer is a Python script that processes MongoDB slow log files, extracts relevant information, and print it. For more deep analysis data is stores in an SQLite database.

##  Requirements
- MongoDB version 4.4 or higher
- Python 3

## Installation

To install the required dependencies, run the following command:

```bash
pip3 install -r requirements.txt
```

## Usage

```bash
python3 mongodb-slow-log-analyzer.py [log] [--db DB_PATH] [--limit ROW_LIMIT] [--char-limit CHAR_LIMIT] [--count COUNT] [--sql]

python3 mongodb-slow-log-analyzer.py --db ./slow-log.sql --limit 10 --char-limit 100 --count 5 /var/log/mongod.log
```

## Check sql command
```bash
python3 mongodb-slow-log-analyzer.py --sql
python3 mongodb-slow-log-analyzer.py --sql --limit 30  --char-limit 200 --count 100
```

## Enable slow log
```bash
db.setProfilingLevel(0, { slowms: 100 })
```

## Why Use This Script?
MongoDB's built-in profiler offers a range of options for slow log analysis, but it comes with its challenges. Setting a profiling level affects server performance, and the default profiler collection, system.profile, has a 1MB size limit. To increase the size, the collection needs to be manually deleted. Handling this on replica sets adds complexity, as commands on the primary node for system.profile do not automatically propagate to replicas.

## Benefits of This Script:
This script provides a straightforward solution for estimating slow queries without affecting server performance. It eliminates the need for manual tuning of the system.profile collection and simplifies the analysis process on replica sets.