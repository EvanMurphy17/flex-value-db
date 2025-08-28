# derdata/dsire/update_dsire.py
import argparse
from datetime import datetime
from pathlib import Path
from time import sleep

from tqdm import tqdm

from derdata.dsire.client import DsireClient
from derdata.utils.io import ensure_dir, write_json_gz, read_json
from derdata.utils.dates import yyyymmdd, month_chunks
from derdata.utils.logging import get_logger

logger = get_logger("dsire_update")

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw" / "dsire"
STATE_FILE = DATA_DIR / "meta" / "state" / "dsire_last_pull.json"


def save_state(last_end: str) -> None:
    ensure_dir(STATE_FILE.parent)
    STATE_FILE.write_text(f'{{"last_end":"{last_end}"}}', encoding="utf-8")


def load_state() -> str | None:
    st = read_json(STATE_FILE)
    return (st or {}).get("last_end")


def cli() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch DSIRE programs JSON by date range")
    p.add_argument("--start", help="YYYYMMDD inclusive. If omitted uses last saved end or defaults to 20100101")
    p.add_argument("--end", help="YYYYMMDD inclusive. Defaults to today")
    p.add_argument("--version_tag", default="VSDB_2025_08_w1", help="Folder tag under data/raw/dsire")
    p.add_argument("--sleep_sec", type=float, default=0.0, help="Sleep between chunks")
    return p.parse_args()


def main() -> None:
    args = cli()
    client = DsireClient()

    today = datetime.utcnow()
    end = datetime.strptime(args.end, "%Y%m%d") if args.end else today
    start_str = args.start or load_state() or "20100101"
    start = datetime.strptime(start_str, "%Y%m%d")

    out_dir = RAW_DIR / args.version_tag
    ensure_dir(out_dir)

    logger.info(f"Fetching DSIRE programs from {yyyymmdd(start)} to {yyyymmdd(end)} into {out_dir}")

    for s, e in tqdm(list(month_chunks(start, end))):
        s_str, e_str = yyyymmdd(s), yyyymmdd(e)
        fname = out_dir / f"dsire_programs_{s_str}_{e_str}.json.gz"
        if fname.exists():
            logger.info(f"Skip existing {fname.name}")
            continue

        data = client.get_programs_by_date(s_str, e_str)
        write_json_gz(data, fname)
        logger.info(f"Wrote {fname.name}")
        if args.sleep_sec > 0:
            sleep(args.sleep_sec)

    save_state(yyyymmdd(end))
    logger.info("Done")


if __name__ == "__main__":
    main()
