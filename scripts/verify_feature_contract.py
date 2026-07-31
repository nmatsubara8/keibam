"""② 本番モデルで strict 列契約が意図しない停止を起こさないかを実地確認する。

保存済みモデル（feature_contract_ 採録済み）を読み、実 featured に対して学習=推論の列契約を
検証する。strict で不足列ゼロなら本番配線 OK（silent 0 埋め誤予測は起こり得ない）。不足列が
出たら、その一覧から「学習=推論の真の乖離（要パイプライン修正）」か「良性の欠損（要退避判断）」を
判断できる。calc_score の end-to-end も試し、実際に FeatureContractError で止まるかを確認する。

使い方（DB・featured・モデルがある環境で）:
  python scripts/verify_feature_contract.py                          # 最新モデル×既定 featured
  python scripts/verify_feature_contract.py --version 20260728_keibam
  python scripts/verify_feature_contract.py --model-path models/20260728/xxx.pickle \
      --featured data/featured_jrdb.pkl
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _latest_model_path(models_dir: str) -> str | None:
    """models/<日付>/<version>.pickle のうち、__win/__<category> でない最新の本命(Place)ヘッド。"""
    cands = [
        p for p in glob.glob(os.path.join(models_dir, "*", "*.pickle"))
        if "__" not in os.path.basename(p)
    ]
    return max(cands, key=os.path.getmtime) if cands else None


def main() -> int:
    ap = argparse.ArgumentParser(description="② 本番モデルの strict 列契約 実地確認")
    ap.add_argument("--version", default=None, help="models_dir 内の version 名で読み込む")
    ap.add_argument("--model-path", default=None, help="モデル pickle への直接パス")
    ap.add_argument("--models-dir", default="models")
    ap.add_argument("--featured", default=None, help="featured pickle（既定=本番 featured）")
    args = ap.parse_args()

    from app._data_loader import load_model_from_path
    from app._model_eval import load_featured_data
    from src.pipeline._eval_stamp import feature_schema_hash
    from src.policies._score_policy import META_COLS, StdScorePolicy
    from src.training._feature_contract import FeatureContractError

    # ── モデル ──
    if args.model_path:
        ai = load_model_from_path(args.model_path)
        model_ref = args.model_path
    elif args.version:
        # find_model_paths は本番命名(_keibam)のみ返すため、実験バージョン名でも拾えるよう
        # models/*/*.pickle を version 部分一致で直接検索する（__win/__category は除外）。
        cands = sorted(
            (p for p in glob.glob(os.path.join(args.models_dir, "*", "*.pickle"))
             if args.version in os.path.basename(p) and "__" not in os.path.basename(p)),
            key=os.path.getmtime,
        )
        if not cands:
            print(f"バージョン '{args.version}' のモデルが見つかりません: {args.models_dir}",
                  file=sys.stderr)
            return 1
        ai = load_model_from_path(cands[-1])
        model_ref = cands[-1]
    else:
        p = _latest_model_path(args.models_dir)
        if not p:
            print(f"モデルが見つかりません: {args.models_dir}/*/*.pickle", file=sys.stderr)
            return 1
        ai = load_model_from_path(p)
        model_ref = p
    print(f"モデル: {model_ref}")

    # ── featured ──
    featured = load_featured_data(args.featured) if args.featured else load_featured_data()
    if featured is None or featured.empty:
        print("featured がありません", file=sys.stderr)
        return 1
    print(f"featured: {len(featured):,} 行 / {featured.shape[1]} 列")

    # ── 契約 vs featured ──
    contract = getattr(ai, "feature_contract_", None)
    names = list(contract.names) if contract is not None else getattr(ai, "feature_names_", None)
    if names is None:
        print("このモデルは feature_contract_/feature_names_ を持ちません（旧モデル）。"
              "再学習してから再確認してください。", file=sys.stderr)
        return 1
    schema = feature_schema_hash(names)
    print(f"学習スキーマ: hash={schema} / {len(names)} 列"
          f"{'（FeatureContract）' if contract is not None else '（feature_names_ 互換）'}")

    meta_cols = [c for c in META_COLS if c in featured.columns]
    feat_cols = [c for c in names if c not in meta_cols]
    cols = set(map(str, featured.columns))
    missing = [c for c in feat_cols if str(c) not in cols]
    extra = [c for c in featured.columns if str(c) not in set(map(str, names)) and c not in meta_cols]

    print(f"\n[列契約チェック] 学習特徴 {len(feat_cols)} / 推論 featured に不足 {len(missing)} / "
          f"契約外(無視される) {len(extra)}")
    if missing:
        print(f"  不足列(先頭30): {missing[:30]}")

    # ── end-to-end: strict で calc_score が止まるか ──
    print("\n[end-to-end] calc_score（既定 strict）を実行:")
    try:
        score = ai.calc_score(featured, StdScorePolicy)
        n = len(score) if score is not None else 0
        print(f"  ✅ PASS（例外なし・score {n:,} 行）→ strict 列契約は本番 featured と整合。②を本番配線して安全。")
        verdict = 0
    except FeatureContractError as e:
        print(f"  ❌ FeatureContractError で停止: {e}")
        print("  → 不足列が『学習=推論の真の乖離』なら featured 生成側を修正（本来のバグ検出＝設計どおり）。")
        print("     『良性の欠損』と判断できる場合のみ KEIBA_LENIENT_FEATURES=1 で従来の0埋めに退避可。")
        verdict = 2

    # ── 参考: lenient なら何列 0 埋めされていたか（silent 誤予測の面積） ──
    if missing:
        print(f"\n[参考] 旧挙動(lenient)なら {len(missing)} 列が 0 埋めされ静かに誤予測していた面積です。")
    return verdict


if __name__ == "__main__":
    sys.exit(main())
