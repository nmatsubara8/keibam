"""ファンダ検証用レーススクレイパ: 出走馬ごとに horse_id/単勝/着順/斤量/馬体重/枠 を取得。

--pool jra: race.netkeiba.com 発見・場コード01-10。 --pool nar: nar.netkeiba.com・場30-65。
出力(append): {out} に per-runner 行。date は発見時の kaisai_date（リークセーフな履歴フィルタ用）。
"""
from __future__ import annotations

import argparse
import io
import re
import subprocess
import time
from datetime import datetime, timedelta

import pandas as pd

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120 Safari/537.36")


def fetch(url, ref=None, tries=4):
    cmd = ["curl", "-sS", "-A", UA, "--max-time", "30"]
    if ref:
        cmd += ["-e", ref]
    cmd.append(url)
    for i in range(tries):
        try:
            p = subprocess.run(cmd, capture_output=True, timeout=40)
            if p.returncode == 0 and p.stdout:
                return p.stdout
        except Exception:
            pass
        time.sleep(2 ** i)
    return None


def wnum(s):
    m = re.match(r"\s*(\d+)", str(s))
    return float(m.group(1)) if m else float("nan")


def parse_race(rid, ymd):
    b = fetch(f"https://db.netkeiba.com/race/{rid}/")
    if not b:
        return []
    h = b.decode("euc-jp", "replace")
    m = re.search(r"<table[^>]*race_table_01.*?</table>", h, re.S)
    if not m:
        return []
    hids = re.findall(r"/horse/(\d+)/", m.group(0))
    try:
        tabs = pd.read_html(io.StringIO(h))
    except Exception:
        return []
    res = None
    for t in tabs:
        cols = ["".join(str(c).split()) for c in t.columns]
        if "着順" in cols and "単勝" in cols:
            t.columns = cols
            res = t
            break
    if res is None or len(res) != len(hids):
        return []
    res = res.assign(horse_id=hids)
    rank = pd.to_numeric(res["着順"], errors="coerce")
    keep = rank.notna() & pd.to_numeric(res["単勝"], errors="coerce").notna()
    res = res[keep]
    n = len(res)
    if n < 4:
        return []
    rows = []
    for r in res.itertuples():
        d = r._asdict()
        rows.append({
            "race_id": rid, "place": rid[4:6], "date": ymd,
            "horse_id": d["horse_id"], "umaban": int(pd.to_numeric(d["馬番"], errors="coerce")),
            "rank": int(pd.to_numeric(d["着順"], errors="coerce")),
            "tansho": float(pd.to_numeric(d["単勝"], errors="coerce")),
            "ninki": pd.to_numeric(d.get("人気"), errors="coerce"),
            "kinryo": pd.to_numeric(d.get("斤量"), errors="coerce"),
            "horse_weight": wnum(d.get("馬体重", "")),
            "sexage": str(d.get("性齢", "")).strip(),
            "field_size": n,
        })
    return rows


def discover(host, places, start, end, n_races, sleep):
    got = []
    d = end
    while d >= start and len(got) < n_races * 2:
        ymd = d.strftime("%Y%m%d")
        b = fetch(f"https://{host}/top/race_list_sub.html?kaisai_date={ymd}")
        time.sleep(sleep)
        if b:
            ids = sorted(set(re.findall(r"race_id=(\d{12})", b.decode("euc-jp", "replace"))))
            for rid in ids:
                if rid[4:6] in places:
                    got.append((ymd, rid))
        print(f"discover {ymd}: cum={len(got)}", flush=True)
        d -= timedelta(days=1)
    return got[:n_races * 2]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", choices=["jra", "nar"], required=True)
    ap.add_argument("--n-races", type=int, default=250)
    ap.add_argument("--start", default="2026-01-01")
    ap.add_argument("--end", default="2026-06-30")
    ap.add_argument("--out", required=True)
    ap.add_argument("--sleep", type=float, default=0.8)
    args = ap.parse_args()

    host = "race.netkeiba.com" if args.pool == "jra" else "nar.netkeiba.com"
    places = {f"{i:02d}" for i in range(1, 11)} if args.pool == "jra" else {f"{i:02d}" for i in range(30, 66)}
    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()

    cand = discover(host, places, start, end, args.n_races, args.sleep)
    cols = ["race_id", "place", "date", "horse_id", "umaban", "rank", "tansho",
            "ninki", "kinryo", "horse_weight", "sexage", "field_size"]
    with open(args.out, "w") as f:
        f.write(",".join(cols) + "\n")
    nr = 0
    for ymd, rid in cand:
        if nr >= args.n_races:
            break
        rows = parse_race(rid, ymd)
        time.sleep(args.sleep)
        if rows:
            with open(args.out, "a") as f:
                for r in rows:
                    f.write(",".join(str(r[c]) for c in cols) + "\n")
            nr += 1
            if nr % 25 == 0:
                print(f"{args.pool}: {nr} races", flush=True)
    print(f"ALL DONE {args.pool}: {nr} races -> {args.out}", flush=True)


if __name__ == "__main__":
    main()
