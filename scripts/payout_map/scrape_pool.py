"""荒れ度×配当・bold-play 地図用のプール別スクレイパ（1フェッチで単勝/着順＋連系払戻）。

--pool jra: race.netkeiba.com 発見・場コード01-10。 --pool nar: nar.netkeiba.com・場30-65。
各レースを db.netkeiba.com/race/{id} で**1回だけ**取得し、結果表(単勝/着順)と払戻表(三連単)を同時に
パース → {out}_runners.csv, {out}_payoffs.csv（analyze_payout_map.py がそのまま読める）。礼儀: ~0.8s間隔。

使い方:
    python scripts/payout_map/scrape_pool.py --pool jra --n-races 250 --out data/payout_map/jra
    python scripts/payout_map/scrape_pool.py --pool nar --n-races 250 --out data/payout_map/nar
"""
from __future__ import annotations

import argparse
import io
import os
import re
import subprocess
import time
from datetime import datetime, timedelta

import pandas as pd

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120 Safari/537.36")
WANT = {"馬連": ("umaren", 2), "馬単": ("umatan", 2), "三連複": ("sanrenpuku", 3), "三連単": ("sanrentan", 3)}


def fetch(url, tries=4):
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


def parse_payoffs(tabs):
    out = {}
    for t in tabs:
        if t.shape[1] < 3:
            continue
        for row in [list(map(str, t.columns))] + t.astype(str).values.tolist():
            bt = str(row[0]).strip()
            if bt not in WANT or WANT[bt][0] in out:
                continue
            key, ln = WANT[bt]
            nums = re.findall(r"\d+", str(row[1]))[:ln]
            pays = re.findall(r"\d[\d,]*", str(row[2]))
            if len(nums) < ln or not pays:
                continue
            out[key] = ("-".join(nums), int(pays[0].replace(",", "")))
    return out


def parse_result(h):
    tabs = pd.read_html(io.StringIO(h))
    for t in tabs:
        cols = ["".join(str(c).split()) for c in t.columns]
        if "着順" in cols and "単勝" in cols:
            t.columns = cols
            rank = pd.to_numeric(t["着順"], errors="coerce")
            keep = rank.notna() & pd.to_numeric(t["単勝"], errors="coerce").notna()
            return t[keep], tabs
    return None, tabs


def discover(host, places, start, end, n_races, sleep):
    got, d = [], end
    while d >= start and len(got) < n_races * 2:
        ymd = d.strftime("%Y%m%d")
        b = fetch(f"https://{host}/top/race_list_sub.html?kaisai_date={ymd}")
        time.sleep(sleep)
        if b:
            for rid in sorted(set(re.findall(r"race_id=(\d{12})", b.decode("euc-jp", "replace")))):
                if rid[4:6] in places:
                    got.append((ymd, rid))
        print(f"discover {ymd}: cum={len(got)}", flush=True)
        d -= timedelta(days=1)
    return got


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", choices=["jra", "nar"], required=True)
    ap.add_argument("--n-races", type=int, default=250)
    ap.add_argument("--start", default="2026-01-01")
    ap.add_argument("--end", default="2026-06-30")
    ap.add_argument("--out", required=True, help="出力プレフィックス（_runners.csv / _payoffs.csv を付す）")
    ap.add_argument("--sleep", type=float, default=0.8)
    args = ap.parse_args()

    host = "race.netkeiba.com" if args.pool == "jra" else "nar.netkeiba.com"
    places = {f"{i:02d}" for i in range(1, 11)} if args.pool == "jra" else {f"{i:02d}" for i in range(30, 66)}
    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    cand = discover(host, places, start, end, args.n_races, args.sleep)
    rcols = ["race_id", "place", "date", "umaban", "rank", "tansho_odds", "ninki", "n_horses"]
    pcols = ["race_id", "place", "umaren_c", "umaren_pay", "umatan_c", "umatan_pay",
             "sanrenpuku_c", "sanrenpuku_pay", "sanrentan_c", "sanrentan_pay"]
    fr = open(f"{args.out}_runners.csv", "w")
    fp = open(f"{args.out}_payoffs.csv", "w")
    fr.write(",".join(rcols) + "\n")
    fp.write(",".join(pcols) + "\n")
    nr = 0
    for ymd, rid in cand:
        if nr >= args.n_races:
            break
        b = fetch(f"https://db.netkeiba.com/race/{rid}/")
        time.sleep(args.sleep)
        if not b:
            continue
        try:
            res, tabs = parse_result(b.decode("euc-jp", "replace"))
        except Exception:
            continue
        if res is None or len(res) < 4:
            continue
        pay = parse_payoffs(tabs)
        if "sanrentan" not in pay:
            continue
        pc, n = rid[4:6], len(res)
        for r in res.itertuples():
            d = r._asdict()
            fr.write(",".join(str(x) for x in [
                rid, pc, ymd, int(pd.to_numeric(d["馬番"], errors="coerce")),
                int(pd.to_numeric(d["着順"], errors="coerce")),
                float(pd.to_numeric(d["単勝"], errors="coerce")),
                pd.to_numeric(d.get("人気"), errors="coerce"), n]) + "\n")
        fp.write(",".join(str(x) for x in [
            rid, pc,
            pay.get("umaren", ("", 0))[0], pay.get("umaren", ("", 0))[1],
            pay.get("umatan", ("", 0))[0], pay.get("umatan", ("", 0))[1],
            pay.get("sanrenpuku", ("", 0))[0], pay.get("sanrenpuku", ("", 0))[1],
            pay["sanrentan"][0], pay["sanrentan"][1]]) + "\n")
        fr.flush()
        fp.flush()
        nr += 1
        if nr % 25 == 0:
            print(f"{args.pool}: {nr} races", flush=True)
    fr.close()
    fp.close()
    print(f"ALL DONE {args.pool}: {nr} races -> {args.out}_{{runners,payoffs}}.csv", flush=True)


if __name__ == "__main__":
    main()
