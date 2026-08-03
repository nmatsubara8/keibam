"""JRDBファイル群(KYI/SED/SKB)を読み、featured にJRDB特徴量を付与して保存する。

フォルダ内のファイルを接頭辞(KYI/SED/SKB)で自動分類。連続期間ぶんを入れるほど
前走特記のカバレッジが上がる。出力は元featured＋JRDB列の pickle。

使い方:
  python scripts/jrdb_build_features.py --jrdb-dir /mnt/c/Users/.../jrdb \
      --out data/featured_jrdb.pkl
JRDB列だけのサイドカーが欲しい場合は --sidecar も付ける。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.jrdb._augment import (  # noqa: E402
    MYSPEED_COLS,
    attach,
    build_history,
    build_history_from_dfs,
    build_kyi,
    build_kyi_from_df,
    build_soten_from_df,
    build_soten_history,
    ensure_ketto,
)


def main() -> int:
    ap = argparse.ArgumentParser(description="featured に JRDB 特徴量を付与（--from-store 推奨・raw txt 不要）")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--from-store", action="store_true",
                     help="既取込 JrdbStore(SQLite) から構築（raw txt 不要・推奨）")
    src.add_argument("--jrdb-dir", help="KYI/SED/SKB の .txt/.zip/.lzh を置いたフォルダ")
    ap.add_argument("--extract-to", default="data/jrdb_txt",
                    help="アーカイブ展開先（.lzh/.zip → .txt・--jrdb-dir 時）")
    ap.add_argument("--out", default="data/featured_jrdb.pkl", help="出力 pickle")
    ap.add_argument("--sidecar", action="store_true", help="JRDB列だけのサイドカーも保存")
    # raw MySpeed(jrdb_ms_*) は本番 featured には既定 off（Issue #22 No-Go）だが、JRDB42 研究 artifact
    # には必要。研究 artifact を作るときは --with-myspeed を付ける。
    ap.add_argument("--with-myspeed", action="store_true",
                    help="raw MySpeed(jrdb_ms_*)を付与（JRDB42 研究 artifact に必要・本番は既定 off）")
    args = ap.parse_args()

    from app._model_eval import load_featured_data
    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません")
        return 1

    kyi_for_ketto = None    # ketto ブリッジ用（race_id,umaban,ketto を持つ DataFrame）
    if args.from_store:
        from src.jrdb._store import JrdbStore
        store = JrdbStore()
        print("JrdbStore から KYI/SED/SKB を読込（--from-store・raw txt 不要）...")
        kyi_raw = store.read("KYI")
        kyi = build_kyi_from_df(kyi_raw)
        kyi_for_ketto = kyi_raw
        history = build_history_from_dfs(store.read("SED"), store.read("SKB"))
        soten = build_soten_from_df(store.read("SED")) if args.with_myspeed else None
    else:
        from src.jrdb._extract import extract_dir
        by_type = extract_dir(args.jrdb_dir, args.extract_to)  # .lzh/.zip 透過展開
        files = {k: by_type.get(k, []) for k in ("KYI", "SED", "SKB")}
        print(f"検出: KYI {len(files['KYI'])} / SED {len(files['SED'])} / SKB {len(files['SKB'])} "
              f"ファイル（展開先 {args.extract_to}）")
        if not any(files.values()):
            print("JRDBファイルが見つかりません（KYI/SED/SKB の .txt/.zip/.lzh）。")
            return 1
        kyi = build_kyi(files["KYI"])
        kyi_for_ketto = kyi        # build_kyi 出力は race_id,umaban,ketto を持つ
        history = build_history(files["SED"], files["SKB"])
        soten = build_soten_history(files["SED"]) if args.with_myspeed else None

    myspeed_note = f" / MySpeed {len(soten):,}行" if soten is not None else "（MySpeed off）"
    print(f"  KYI {len(kyi):,}行 / 履歴 {len(history):,}行{myspeed_note}")

    # ketto ブリッジ（netkeiba featured は血統登録番号を持たない）。これが無いと history/soten の
    # asof が空＝prev_*/jrdb_ms_* が全 NaN になる（続36 DEAD の一因）。両経路で必須。
    featured = ensure_ketto(featured, kyi_for_ketto)
    kcov = float(featured["ketto"].notna().mean()) if "ketto" in featured.columns else 0.0
    print(f"  ketto ブリッジ: 有効率={kcov:.3f}（history/soten asof の前提）")

    print("featured へ付与（(race_id,馬番)結合＋前走チェーン）...")
    out = attach(featured, kyi, history, soten=soten)

    # strictly-prior 全件 manifest（history/soten の as-of がリーク安全かを全 target 行で認定）。
    if {"ketto", "date"} <= set(featured.columns):
        from src.jrdb._leak_audit import assert_strictly_prior, strictly_prior_join_report
        tgt = featured[["ketto", "date"]]
        for label, src_df in (("history", history), ("soten", soten)):
            if src_df is None or getattr(src_df, "empty", True):
                continue
            rep = strictly_prior_join_report(tgt, src_df)
            print(f"  [leak manifest {label}] target={rep['target_rows']:,} "
                  f"feature_rows={rep['feature_rows']:,} future={rep['future_reference_count']} "
                  f"same_day={rep['same_day_reference_count']} dup_keys={rep['target_key_duplicates']:,} "
                  f"max_src={rep['max_source_date']} leak_safe={rep['leak_safe']}")
            assert_strictly_prior(rep, label=label)   # 未来/同日参照は fail-closed

    jr_cols = ["jrdb_idm", "jrdb_kijun_odds", "jrdb_kijun_gap", "prev_deokure", "prev_trouble"]
    if args.with_myspeed:
        jr_cols += list(MYSPEED_COLS)
    else:
        # soten=None のとき attach は jrdb_ms_* を全 NaN で作る。既定 featured の schema を
        # 従来通りに保つため落とす（本番学習に No-Go の空列を混ぜない）。
        out = out.drop(columns=[c for c in MYSPEED_COLS if c in out.columns])
    print("\n[JRDB列カバレッジ]")
    for c in jr_cols:
        if c in out.columns:
            cov = out[c].notna().mean()
            print(f"  {c:<18}: {cov:.2%} 非欠損")

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_pickle(args.out)
    print(f"\n保存: {args.out}（{len(out):,}行）")
    if args.sidecar:
        side = out[[c for c in jr_cols if c in out.columns]]
        sp = str(Path(args.out).with_suffix(".sidecar.pkl"))
        side.to_pickle(sp)
        print(f"サイドカー: {sp}")
    print("\n次: この featured_jrdb.pkl を load して manji_walk_forward で "
          "kijun_gap/prev_trouble/prev_deokure を含む因子で前進検証。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
