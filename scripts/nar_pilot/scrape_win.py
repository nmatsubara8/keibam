"""地方競馬(NAR)効率性パイロット: 単勝オッズ＋着順のスクレイパ（少数場・礼儀正しく）。

対象場（既定 大井44/高知54/佐賀55）の開催日を nar.netkeiba.com で走査し race_id を場コードで
絞り、db.netkeiba.com/race/{race_id} の結果表から (着順/馬番/単勝/人気) を取得する。
出力: {data-dir}/nar_pilot.csv。礼儀: curl(プロキシ/CA実績あり)経由・1req/1秒・指数バックオフ。

使い方:
    python scripts/nar_pilot/scrape_win.py --data-dir data/nar_pilot \
        --tracks 44,54,55 --start 2026-05-01 --end 2026-06-30 --cap 160
"""
from __future__ import annotations

import argparse
import io
import os
import re
import subprocess
import time
from datetime import date, datetime, timedelta

import pandas as pd

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120 Safari/537.36")
TRACK_NAMES = {"44": "大井", "54": "高知", "55": "佐賀", "43": "船橋", "45": "川崎",
               "42": "浦和", "46": "金沢", "48": "名古屋", "50": "園田", "30": "門別"}


def fetch(url: str, tries: int = 4) -> bytes | None:
    for i in range(tries):
        try:
            p = subprocess.run(["curl", "-sS", "-A", UA, "--max-time", "30", url],
                               capture_output=True, timeout=40)
            if p.returncode == 0 and p.stdout:
                return p.stdout
        except Exception:
            pass
        time.sleep(2 ** i)
    return None


def daterange(a: date, b: date):
    d = a
    while d <= b:
        yield d
        d += timedelta(days=1)


def discover(tracks, start, end, cap, sleep) -> dict:
    by = {t: [] for t in tracks}
    for d in daterange(start, end):
        if all(len(v) >= cap for v in by.values()):
            break
        ymd = d.strftime("%Y%m%d")
        b = fetch(f"https://nar.netkeiba.com/top/race_list_sub.html?kaisai_date={ymd}")
        time.sleep(sleep)
        if not b:
            continue
        ids = sorted(set(re.findall(r"race_id=(\d{12})", b.decode("euc-jp", "replace"))))
        for rid in ids:
            pc = rid[4:6]
            if pc in by and len(by[pc]) < cap:
                by[pc].append((ymd, rid))
        print(f"discover {ymd}: " + " ".join(f"{TRACK_NAMES.get(t, t)}={len(by[t])}" for t in tracks), flush=True)
    return by


def parse_race(rid: str, ymd: str) -> list[dict]:
    b = fetch(f"https://db.netkeiba.com/race/{rid}/")
    if not b:
        return []
    try:
        tabs = pd.read_html(io.StringIO(b.decode("euc-jp", "replace")))
    except Exception:
        return []
    res = None
    for t in tabs:
        cols = ["".join(str(c).split()) for c in t.columns]
        if "着順" in cols and "単勝" in cols:
            t.columns = cols
            res = t
            break
    if res is None:
        return []
    df = pd.DataFrame({
        "rank": pd.to_numeric(res["着順"], errors="coerce"),
        "umaban": pd.to_numeric(res["馬番"], errors="coerce"),
        "tansho": pd.to_numeric(res["単勝"], errors="coerce"),
        "ninki": pd.to_numeric(res.get("人気", pd.Series([None] * len(res))), errors="coerce"),
    })
    df = df[df["rank"].notna() & df["tansho"].notna() & df["umaban"].notna()]
    if df.empty:
        return []
    n = len(df)
    pc = rid[4:6]
    return [{"race_id": rid, "place": pc, "place_name": TRACK_NAMES.get(pc, pc),
             "date": ymd, "umaban": int(r.umaban), "rank": int(r.rank),
             "tansho_odds": float(r.tansho),
             "ninki": (int(r.ninki) if pd.notna(r.ninki) else ""), "n_horses": n}
            for r in df.itertuples()]


def main() -> int:
    ap = argparse.ArgumentParser(description="NAR 単勝/着順パイロット・スクレイパ")
    ap.add_argument("--data-dir", default="data/nar_pilot")
    ap.add_argument("--tracks", default="44,54,55", help="場コードのカンマ区切り")
    ap.add_argument("--start", default="2026-05-01")
    ap.add_argument("--end", default="2026-06-30")
    ap.add_argument("--cap", type=int, default=160, help="場ごとの取得上限レース数")
    ap.add_argument("--sleep", type=float, default=1.0)
    args = ap.parse_args()

    os.makedirs(args.data_dir, exist_ok=True)
    out = os.path.join(args.data_dir, "nar_pilot.csv")
    tracks = args.tracks.split(",")
    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()

    by = discover(tracks, start, end, args.cap, args.sleep)
    cols = ["race_id", "place", "place_name", "date", "umaban", "rank",
            "tansho_odds", "ninki", "n_horses"]
    with open(out, "w") as f:
        f.write(",".join(cols) + "\n")
    total = 0
    for t, items in by.items():
        for ymd, rid in items:
            rows = parse_race(rid, ymd)
            time.sleep(args.sleep)
            if rows:
                with open(out, "a") as f:
                    for r in rows:
                        f.write(",".join(str(r[c]) for c in cols) + "\n")
                total += 1
        print(f"DONE {TRACK_NAMES.get(t, t)}: {len([1 for _ in items])} candidates", flush=True)
    print(f"ALL DONE: {total} races -> {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
