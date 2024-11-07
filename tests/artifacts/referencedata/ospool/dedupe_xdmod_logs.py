import json
import argparse
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--today", required=True, type=Path, help="today's log file")
    parser.add_argument("--yest", required=True, type=Path, help="yesterday's log file")
    return parser.parse_args()


def main():
    args = parse_args()

    today_set = {json.dumps(log) for log in json.load(args.today.open())}
    print(f"Found {len(today_set)} logs today")

    yest_set = {json.dumps(log) for log in json.load(args.yest.open())}
    print(f"Found {len(yest_set)} logs yesterday")

    new_logs = [json.loads(log) for log in today_set - yest_set]
    del today_set, yest_set
    print(f"Found {len(new_logs)} unique logs today")

    deduped_today = args.today.with_name(f"deduped.{args.today.name}")
    json.dump(new_logs, deduped_today.open("w"))
    print(f"Wrote deduped logs to {str(deduped_today)}")


if __name__ == "__main__":
    main()
