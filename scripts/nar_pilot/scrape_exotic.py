"""地方競馬(NAR)効率性パイロット: 連系(馬連/馬単/三連複/三連単)の払戻スクレイパ。

race_id は {data-dir}/nar_pilot.csv から読む（再発見しない＝礼儀正しく最小リクエスト）。
db.netkeiba.com/race/{race_id} の払戻表を解析し、券種別の的中組合せ＋払戻(¥100あたり)を得る。
出力: {data-dir}/nar_payoffs.csv。順序券(馬単/三連単)は「→」、非順序券(馬連/三連複)は「-」区切り。

使い方:
    python scripts/nar_pilot/scrape_exotic.py --data-dir data/nar_pilot
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
# 券種名 → (キー, 頭数)
WANT = {"馬連": ("umaren", 2), "馬単": ("umatan", 2),
        "三連複": ("sanrenpuku", 3), "三連単": ("sanrentan", 3)}


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


def parse_payoffs(tabs) -> dict:
    """払戻表群から {キー: ("組合せ", 払戻)} を返す。header 有無に頑健(位置アクセス)。"""
    out = {}
    for t in tabs:
        if t.shape[1] < 3:
            continue
        rows = [list(map(str, t.columns))] + t.astype(str).values.tolist()
        for row in rows:
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


def main() -> int:
    ap = argparse.ArgumentParser(description="NAR 連系払戻スクレイパ")
    ap.add_argument("--data-dir", default="data/nar_pilot")
    ap.add_argument("--sleep", type=float, default=1.0)
    args = ap.parse_args()

    horses = pd.read_csv(os.path.join(args.data_dir, "nar_pilot.csv"))
    races = horses[["race_id", "place_name"]].drop_duplicates().reset_index(drop=True)
    out = os.path.join(args.data_dir, "nar_payoffs.csv")
    cols = ["race_id", "place_name", "umaren_c", "umaren_pay", "umatan_c", "umatan_pay",
            "sanrenpuku_c", "sanrenpuku_pay", "sanrentan_c", "sanrentan_pay"]
    with open(out, "w") as f:
        f.write(",".join(cols) + "\n")

    ok = 0
    for r in races.itertuples():
        rid = str(r.race_id)
        b = fetch(f"https://db.netkeiba.com/race/{rid}/")
        time.sleep(args.sleep)
        if not b:
            continue
        try:
            tabs = pd.read_html(io.StringIO(b.decode("euc-jp", "replace")))
        except Exception:
            continue
        p = parse_payoffs(tabs)
        if "sanrentan" not in p:
            continue
        rec = {"race_id": rid, "place_name": r.place_name,
               "umaren_c": p.get("umaren", ("", 0))[0], "umaren_pay": p.get("umaren", ("", 0))[1],
               "umatan_c": p.get("umatan", ("", 0))[0], "umatan_pay": p.get("umatan", ("", 0))[1],
               "sanrenpuku_c": p.get("sanrenpuku", ("", 0))[0], "sanrenpuku_pay": p.get("sanrenpuku", ("", 0))[1],
               "sanrentan_c": p["sanrentan"][0], "sanrentan_pay": p["sanrentan"][1]}
        with open(out, "a") as f:
            f.write(",".join(str(rec[c]) for c in cols) + "\n")
        ok += 1
        if ok % 50 == 0:
            print(f"{ok}/{len(races)} payoffs", flush=True)
    print(f"ALL DONE: {ok}/{len(races)} races -> {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
