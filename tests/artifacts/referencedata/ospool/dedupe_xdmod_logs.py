import json
import shutil
import argparse
from os import fsync
from pathlib import Path
from tempfile import NamedTemporaryFile


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--today", required=True, type=Path, help="today's log file")
    parser.add_argument("--yest", required=True, type=Path, help="yesterday's log file")
    return parser.parse_args()


def write_json_file(obj, path: Path, indent=None):
    with NamedTemporaryFile(mode="w", delete=False) as f:
        tmp_path = Path(f.name)
        json.dump(obj, f, indent=indent)
        f.flush()
        fsync(f.fileno())
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        try:  # try atomic move first
            tmp_path.rename(path)
        except OSError:
            shutil.move(tmp_path.as_posix(), path.as_posix())
    except OSError:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        raise


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
    write_json_file(new_logs, deduped_today, indent=2)
    print(f"Wrote deduped logs to {str(deduped_today)}")


if __name__ == "__main__":
    main()
