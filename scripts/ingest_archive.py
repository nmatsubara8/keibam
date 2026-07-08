"""アップロードした JRA-VAN アーカイブ CSV を取り込んで featured_data まで一気通貫で作る。

このコンテナはローカルディスク（C:\\Users\\... 等）を見られない。CSV を渡す唯一の方法は
**Claude の画面からのアップロード**で、`/root/.claude/uploads/<session>/` に届く。本スクリプトは
そこ（または --src 指定）から race_result CSV を見つけ、次の連鎖を実行する:

  1. seed_from_csv.py <csv>            → data/raw/seed_{results,race_info,horse_results}.pkl
  2. build_seed_featured.py           → data/raw/seed_featured_data.pkl
  3. seed_featured_data.pkl をコピー   → data/raw/featured_data.pkl（walk_forward が読む既定パス）

完了後:
  python walk_forward.py --quality --stacking --with-tuning   # 対市場 ΔR²（Optuna済み最強構成）
  python walk_forward.py --stacking --with-tuning --by-odds   # OOS 回収率（オッズ帯別）

使い方:
  python scripts/ingest_archive.py                       # uploads から最新CSVを自動検出
  python scripts/ingest_archive.py --src /path/to.csv    # パス明示
  python scripts/ingest_archive.py --limit 200000        # 先頭N行だけ（試行・軽量確認用）
"""
from __future__ import annotations

import argparse
import glob
import os
import shutil
import subprocess
import sys

UPLOAD_ROOT = "/root/.claude/uploads"
RAW = "data/raw"
SEED_FEATURED = os.path.join(RAW, "seed_featured_data.pkl")
FEATURED = os.path.join(RAW, "featured_data.pkl")


def find_csv(src: str | None) -> str | None:
    """--src 指定を優先。無ければ uploads 配下から最大の .csv を選ぶ（race_result を優先）。"""
    if src:
        return src if os.path.exists(src) else None
    cands = glob.glob(os.path.join(UPLOAD_ROOT, "**", "*.csv"), recursive=True)
    cands += glob.glob(os.path.join(UPLOAD_ROOT, "**", "*.CSV"), recursive=True)
    if not cands:
        return None
    # race_result を名前で優先、その中で最大サイズ（=本コーパス）
    pref = [c for c in cands if "race_result" in os.path.basename(c).lower()] or cands
    return max(pref, key=lambda p: os.path.getsize(p))


def run(cmd: list[str]) -> None:
    print("\n$ " + " ".join(cmd), flush=True)
    r = subprocess.run(cmd)
    if r.returncode != 0:
        print(f"[中断] コマンド失敗 (exit {r.returncode}): {' '.join(cmd)}", file=sys.stderr)
        sys.exit(r.returncode)


def main() -> int:
    ap = argparse.ArgumentParser(description="アップロード済みアーカイブCSV→featured_data 一気通貫取り込み")
    ap.add_argument("--src", default=None, help="CSV パス（省略時は uploads から自動検出）")
    ap.add_argument("--limit", type=int, default=None, help="先頭N行だけ変換（試行用）")
    ap.add_argument("--keep-existing-featured", action="store_true",
                    help="既存 featured_data.pkl を上書きしない（seed_featured のコピーをスキップ）")
    args = ap.parse_args()

    csv = find_csv(args.src)
    if not csv:
        print("CSV が見つかりません。Claude の画面から race_result CSV をアップロードしてから再実行するか、"
              "--src でパスを指定してください。", file=sys.stderr)
        print(f"（探索先: {UPLOAD_ROOT}/**/*.csv）", file=sys.stderr)
        return 1
    size_mb = os.path.getsize(csv) / 1e6
    print(f"取り込み対象: {csv}  ({size_mb:,.1f} MB)")
    os.makedirs(RAW, exist_ok=True)

    # 1) CSV → seed_*.pkl
    seed_cmd = [sys.executable, "seed_from_csv.py", csv, "--out-dir", RAW]
    if args.limit:
        seed_cmd += ["--limit", str(args.limit)]
    run(seed_cmd)

    # 2) seed_*.pkl → seed_featured_data.pkl
    run([sys.executable, "build_seed_featured.py",
         "--results", os.path.join(RAW, "seed_results.pkl"),
         "--race-info", os.path.join(RAW, "seed_race_info.pkl"),
         "--horse-results", os.path.join(RAW, "seed_horse_results.pkl"),
         "--out", SEED_FEATURED])

    # 3) walk_forward が読む featured_data.pkl にコピー
    if not os.path.exists(SEED_FEATURED):
        print(f"[中断] {SEED_FEATURED} が生成されていません。", file=sys.stderr)
        return 1
    if args.keep_existing_featured and os.path.exists(FEATURED):
        print(f"既存 {FEATURED} を保持（コピーをスキップ）。walk_forward には seed_featured を別途指定してください。")
    else:
        shutil.copy2(SEED_FEATURED, FEATURED)
        print(f"コピー: {SEED_FEATURED} → {FEATURED}")

    print("\n" + "=" * 70)
    print("取り込み完了。次を実行してください（Optuna済み最強構成の OOS 実測）:")
    print("  python walk_forward.py --quality --stacking --with-tuning")
    print("    → 市場 vs モデル vs companion(合成α·logf+β·logπ) の OOS logloss/Brier/ECE。")
    print("      companion が市場を安定して下回れば edge、そうでなければ NO-GO をチューニング込みで確定。")
    print("  python walk_forward.py --stacking --with-tuning --by-odds")
    print("    → OOS 単勝回収率（オッズ帯別）。")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
