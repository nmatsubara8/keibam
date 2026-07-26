"""馬別過去走スクレイパ（リークセーフなファンダ特徴の素材）。resumable。

runner CSV 群から unique horse_id を集め、db.netkeiba.com/horse/result/{id} (AJAX, Referer必須) を
取得して career table をパース。出力(append): horse_id,date,field_size,rank,ninki,distance,prize。
既に出力済みの horse_id はスキップ（途中再開可）。
"""
from __future__ import annotations

import argparse
import io
import os
import re
import subprocess
import time

import pandas as pd

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120 Safari/537.36")


def fetch(url, ref, tries=4):
    for i in range(tries):
        try:
            p = subprocess.run(["curl", "-sS", "-A", UA, "-e", ref, "--max-time", "30", url],
                               capture_output=True, timeout=40)
            if p.returncode == 0 and p.stdout:
                return p.stdout
        except Exception:
            pass
        time.sleep(2 ** i)
    return None


def parse_hist(hid):
    b = fetch(f"https://db.netkeiba.com/horse/result/{hid}/", f"https://db.netkeiba.com/horse/{hid}/")
    if not b:
        return None
    try:
        tabs = pd.read_html(io.StringIO(b.decode("euc-jp", "replace")))
    except Exception:
        return None
    if not tabs:
        return None
    t = max(tabs, key=lambda x: x.shape[0])
    t.columns = ["".join(str(c).split()) for c in t.columns]
    if "日付" not in t.columns or "着順" not in t.columns:
        return []
    rows = []
    for r in t.itertuples():
        d = r._asdict()
        dt = str(d.get("日付", "")).replace("/", "")
        if not re.match(r"^\d{8}$", dt):
            continue
        dist = re.sub(r"\D", "", str(d.get("距離", "")))
        rows.append({
            "horse_id": hid, "date": dt,
            "field_size": pd.to_numeric(d.get("頭数"), errors="coerce"),
            "rank": pd.to_numeric(d.get("着順"), errors="coerce"),
            "ninki": pd.to_numeric(d.get("人気"), errors="coerce"),
            "distance": dist,
            "prize": pd.to_numeric(str(d.get("賞金", "")).replace(",", ""), errors="coerce"),
        })
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runners", nargs="+", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--sleep", type=float, default=0.8)
    args = ap.parse_args()

    ids = set()
    for rf in args.runners:
        ids |= set(pd.read_csv(rf)["horse_id"].astype(str))
    done = set()
    cols = ["horse_id", "date", "field_size", "rank", "ninki", "distance", "prize"]
    if os.path.exists(args.out):
        done = set(pd.read_csv(args.out)["horse_id"].astype(str))
    else:
        with open(args.out, "w") as f:
            f.write(",".join(cols) + "\n")
    todo = sorted(ids - done)
    print(f"unique horses={len(ids)} done={len(done)} todo={len(todo)}", flush=True)

    for k, hid in enumerate(todo):
        rows = parse_hist(hid)
        time.sleep(args.sleep)
        if rows:
            with open(args.out, "a") as f:
                for r in rows:
                    f.write(",".join(str(r[c]) for c in cols) + "\n")
        elif rows == []:
            with open(args.out, "a") as f:  # 履歴なし(新馬)でも1行の空マーカーは書かない
                pass
        if (k + 1) % 100 == 0:
            print(f"{k + 1}/{len(todo)} horses", flush=True)
    print(f"ALL DONE horses -> {args.out}", flush=True)


if __name__ == "__main__":
    main()
