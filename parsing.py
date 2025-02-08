#!/usr/bin/env python3
import re
import argparse
from elasticsearch import Elasticsearch
from datetime import datetime

# Example log-line format:
# 2024-06-13 12:34:56,789 - INFO - User logged in successfully.
LOG_PATTERN = re.compile(
    r'^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - (?P<level>[A-Z]+) - (?P<message>.*)$'
)

def parse_log_line(line):
    """
    Parses a single log line using the defined regex.
    Returns a dictionary with parsed data if matched; otherwise, None.
    """
    match = LOG_PATTERN.match(line.strip())
    if match:
        data = match.groupdict()
        # Convert the timestamp string to a datetime object, then to ISO format
        try:
            dt = datetime.strptime(data["timestamp"], "%Y-%m-%d %H:%M:%S,%f")
            data["timestamp"] = dt.isoformat()
        except ValueError:
            # If conversion fails, leave the timestamp as is
            pass
        return data
    else:
        return None

def index_log_entry(es_client, index_name, doc):
    """
    Indexes a single document into Elasticsearch.
    """
    response = es_client.index(index=index_name, document=doc)
    return response

def process_log_file(log_file_path, es_client, index_name):
    """
    Reads the log file line by line, parses each line, and indexes it to Elasticsearch.
    """
    with open(log_file_path, 'r') as file:
        for line in file:
            parsed_doc = parse_log_line(line)
            if parsed_doc:
                response = index_log_entry(es_client, index_name, parsed_doc)
                print(f"Indexed document ID: {response['_id']}")
            else:
                print(f"Could not parse line: {line.strip()}")

def main():
    parser = argparse.ArgumentParser(description="Custom Log Parser and Elasticsearch Indexer")
    parser.add_argument("log_file", help="Path to the log file to parse")
    parser.add_argument("--es_host", default="localhost", help="Elasticsearch host address")
    parser.add_argument("--es_port", type=int, default=9200, help="Elasticsearch port")
    parser.add_argument("--index", default="application_logs", help="Elasticsearch index name")
    args = parser.parse_args()

    # Connect to elasticsearch
    es_client = Elasticsearch([{"host": args.es_host, "port": args.es_port}])
    if not es_client.ping():
        print("Error: Could not connect to Elasticsearch.")
        return

    print(f"Processing log file: {args.log_file}")
    process_log_file(args.log_file, es_client, args.index)
    print("Log processing completed.")

if __name__ == "__main__":
    main()
