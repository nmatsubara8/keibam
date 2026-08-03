"""取得項目の妥当性・網羅性 認定監査（3層: 仕様レイアウト → 保存 store → featured 実体化）。

「取得項目」を3層で確認する:
  L1 仕様妥当性: JRDB 15 形式のオフセット境界（record 長内か）・取得 byte 率（各 format のどれだけを
     parse しているか）。純粋・データ不要。
  L2 store 保存: JrdbStore 各テーブルの行数・キー・年別 coverage（実際に取り込めているか）。要ローカル。
  L3 featured 実体化: KYI_FEATURE_MAP 等で **定義された** jrdb_*/prev_*/MySpeed が featured に**実在**するか・
     非欠測率・年別・sentinel(-99.9/負) 率。**定義≠実体化**（DEAD 特徴の再発）を検出。要ローカル。
  L4 未ブリッジ: 保存済だが featured へ橋渡しされていない rich テーブル（CYB/CHA/TYB/KKA/KAB/SRB…）を
     latent 信号として列挙（網羅性の伸びしろ）。

L1 は本 env でも動く。L2-L4 はローカル（JRDB store・featured）で。単位: coverage は非欠測率。
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# 取得項目の状態分類（ユーザ査読・続31）。「ブリッジ済」は誤解を招くため 7 状態＋時点クラスへ。
#   MATERIALIZED          : featured に実在
#   MATERIALIZED_RACE_CONTEXT: 実在するが race 内定数（馬間分散≈0・展開/予想の場コンテキスト）
#   IMPLEMENTED_NOT_APPLIED: attach 実装はあるが本番 build で未適用（=現 artifact に無い）
#   INGESTED_NOT_BRIDGED  : store 済だが featured へ橋渡し無し
#   WRONG_SOURCE          : その source では観測できない（時点/取得元の契約違反）＝別 source へ移譲
#   SOURCE_EMPTY / INGESTION_MISSING / HISTORICAL_ONLY / OUTCOME_ONLY
# 時点クラス（feature contract 用）: direct_current / bet_time_contract / historical_only / outcome_only
SOURCE_STATE = {
    # KYI: 固定5列のみ本線 MATERIALIZED、残り指数群は standalone augment で実体化検証済（本線未配線）。
    # pace_hms は race 内定数(MATERIALIZED_RACE_CONTEXT)、確定馬体重は WRONG_SOURCE(→TYB) で KYI から除外。
    "KYI": ("MATERIALIZED(5)+IMPLEMENTED_NOT_APPLIED(残指数)+RACE_CONTEXT(pace_hms)", "direct_current",
            "前日予想・指数。5列(idm/kishu_idx/joho_idx/kyakushitsu/kijun_odds)のみ本線注入。"
            "pace_hms は場の展開予想=race内定数。確定馬体重は WRONG_SOURCE で除外(→TYB)"),
    "SED": ("IMPLEMENTED_NOT_APPLIED(prev_*/MySpeed)+OUTCOME_ONLY(当該走)", "historical_only",
            "過去走の strictly-prior 集約。続36 で asof の date パース(和暦表記)不備を修復＝実体化可能に"),
    "SKB": ("IMPLEMENTED_NOT_APPLIED(prev_trouble)", "historical_only",
            "過去走特記(TROUBLE_TOKKI)。続36 で asof date 修復＝実体化可能(本線配線は別工程)"),
    "TYB": ("INGESTED_NOT_BRIDGED", "bet_time_contract",
            "直前オッズ/パドック/馬体重(T-15)。確定馬体重の正しい取得元。"
            "bet 決定時刻 <= 配信時刻 の契約が必須"),
    "CYB": ("INGESTED_NOT_BRIDGED", "direct_current", "調教分析: 追切/仕上/調教評価/コメント"),
    "CHA": ("INGESTED_NOT_BRIDGED", "direct_current", "本追切: テン/中間/終いF＋各指数＋併せ結果"),
    "KKA": ("INGESTED_NOT_BRIDGED", "direct_current", "条件別着度数＋父/母父産駒連対率"),
    "UKC": ("INGESTED_NOT_BRIDGED", "direct_current", "馬マスタ: 毛色/系統/生産者/産地(静的)"),
    "SRB": ("INGESTED_NOT_BRIDGED", "historical_only", "ハロンタイム/コーナー位置/バイアス(過去走集約)"),
    "KAB": ("INGESTED_NOT_BRIDGED", "direct_current", "開催: 天候/馬場状態/馬場差/草丈/降水量"),
    "BAC": ("INGESTED_NOT_BRIDGED", "direct_current", "番組: 賞金/発走時刻/馬券発売フラグ"),
    "HJC": ("OUTCOME_ONLY", "outcome_only", "払戻(return 側・特徴でない)"),
    "KSA": ("HISTORICAL_ONLY(2026のみ)", "direct_current", "騎手master。今週分のみ＝時系列履歴不足(KZA要)"),
    "CSA": ("HISTORICAL_ONLY(2026のみ)", "direct_current", "調教師master。今週分のみ(CZA要)"),
    "KTA": ("INGESTION_MISSING(0行)", "direct_current", "登録馬。未取込＝file/glob/parser/store を要確認"),
}


def _l1_layout_validity():
    import src.jrdb._layouts as L
    print("=" * 88)
    print("[L1] 仕様妥当性: JRDB 形式のオフセット境界＋**定義byte被覆率**（=parser がrecord内の")
    print("     何byteをフィールド定義しているか。取得率/正解率ではない）。データ不要")
    RL = L.RECORD_LEN
    dict_layouts = ["KYI", "SED", "TYB", "CYB", "CHA", "KKA", "UKC", "SRB",
                    "KSA", "CSA", "KTA", "BAC", "KAB", "SKB"]
    print(f"  {'型':<5}{'reclen':>7}{'項目':>5}{'max終端':>8}{'境界':>8}{'定義byte被覆':>11}")
    over = []
    for t in dict_layouts:
        lay = getattr(L, t)
        covered = set()
        mx = 0
        for (s, ln) in lay.values():
            covered.update(range(s, s + ln))
            mx = max(mx, s + ln - 1)
        rl = RL[t]
        ok = "OK" if mx <= rl else f"OVER+{mx - rl}"
        if mx > rl:
            over.append(t)
        print(f"  {t:<5}{rl:>7}{len(lay):>5}{mx:>8}{ok:>8}{len(covered) / rl * 100:>9.0f}%")
    # 繰り返し系
    hj = max(st + occ * (cl + pl) - 1 for (_p, st, occ, cl, pl) in L.HJC_GROUPS)
    print(f"  {'HJC':<5}{RL['HJC']:>7}{len(L.HJC_GROUPS):>5}{hj:>8}"
          f"{('OK' if hj <= RL['HJC'] else 'OVER'):>8}{'(券種繰返)':>10}")
    print(f"  → 境界違反: {over if over else 'なし（全形式 record 長内）'}")
    print("  ※ 定義byte被覆率が低い形式（KYI~59%/TYB~51%/SKB~9%）は未 parse 領域が残る。SKB 9% は必要な")
    print("    特記コードのみを意図 parse なら異常でない（未利用領域に有用項目がある可能性の指標）。")


def _load_store():
    try:
        from src.jrdb._store import JrdbStore
        return JrdbStore()
    except Exception as e:  # noqa: BLE001
        print(f"  [情報] JRDB store 読込不可: {e}", file=sys.stderr)
        return None


def _l2_store():
    import pandas as pd
    from src.jrdb._store import RECORD_TYPES
    print("\n" + "=" * 88)
    print("[L2] store 保存: 各テーブル行数・キー・年別 coverage（要ローカル）")
    store = _load_store()
    if store is None:
        print("  store を読めません（ローカルで実行）。")
        return
    print(f"  {'型':<5}{'行数':>10}{'race_id':>8}{'ketto':>7}{'年範囲':>16}")
    for rt in RECORD_TYPES:
        try:
            df = store.read(rt)
        except Exception as e:  # noqa: BLE001
            print(f"  {rt:<5}  読込不可: {e}")
            continue
        rid = "race_id" if "race_id" in df.columns else ("race_key" if "race_key" in df.columns else "-")
        ket = "あり" if "ketto" in df.columns else "-"
        yr = "-"
        for yc in ("ymd", "data_ymd", "chokyo_ymd", "comment_ymd", "birth_ymd"):
            if yc in df.columns:
                y = pd.to_numeric(df[yc].astype(str).str[:4], errors="coerce").dropna()
                if len(y):
                    yr = f"{int(y.min())}-{int(y.max())}"
                    break
        print(f"  {rt:<5}{len(df):>10,}{rid:>8}{ket:>7}{yr:>16}")


def _l3_featured(path=None):
    import numpy as np
    import pandas as pd
    from app._model_eval import load_featured_data
    from src.training._feature_materialization import (CONTEXT_JRDB, CURRENT_ACTIVE_JRDB,
                                                       EXPECTED_JRDB_FULL, HISTORY_JRDB)
    print("\n" + "=" * 88)
    tag = f"（--featured {path}）" if path else "（default 本線 featured）"
    print(f"[L3] featured 実体化: 定義 JRDB 特徴が featured に実在するか{tag}・3契約で判定（定義≠実体化）")
    feat = load_featured_data(path) if path else load_featured_data()
    if feat is None or feat.empty:
        print("  featured を読めません（ローカルで実行・--featured data/featured_jrdb.pkl も可）。")
        return
    rid = pd.Series(feat.index.astype(str))
    year = pd.to_numeric(rid.str[:4], errors="coerce")
    jra = rid.str[4:6].isin({f"{i:02d}" for i in range(1, 11)})
    recent = (jra & (year >= 2020)).to_numpy()          # 直近 JRA で materialization を見る

    def _klass(c):
        return "CONTEXT" if c in CONTEXT_JRDB else ("HISTORY" if c in HISTORY_JRDB else "ACTIVE")

    def _vf(col):                                        # race 内で >1 値を持つ割合（馬間分散有率）
        s = col.groupby(feat.index[recent]).nunique(dropna=True)
        return float((s > 1).mean()) if len(s) else 0.0

    absent, fails = [], {"ACTIVE": [], "CONTEXT": [], "HISTORY": []}
    n_ok = {"ACTIVE": 0, "CONTEXT": 0, "HISTORY": 0}
    print(f"  {'特徴':<24}{'群':>8}{'実在':>5}{'非欠測':>8}{'sentinel':>9}{'分散有率':>9}{'判定':>8}")
    for c in EXPECTED_JRDB_FULL:
        kl = _klass(c)
        if c not in feat.columns:
            absent.append(c)
            print(f"  {c:<24}{kl:>8}{'無':>5}{'—':>8}{'—':>9}{'—':>9}{'ABSENT':>8}")
            continue
        col = pd.to_numeric(feat.loc[recent, c], errors="coerce")
        nm = float(col.notna().mean()) if len(col) else 0.0
        sent = float((col <= -99).mean()) if len(col) else 0.0
        vf = _vf(col)
        if kl == "ACTIVE":
            v = "OK" if (nm >= 0.3 and sent < 0.2 and vf > 0.1) else ("DEAD" if nm < 0.02 else "薄い")
            good = v == "OK"
        elif kl == "CONTEXT":
            v = "CTX_OK" if (nm >= 0.3 and sent < 0.2) else ("DEAD" if nm < 0.02 else "薄い")
            good = v == "CTX_OK"
        else:
            v = "HIST_OK" if nm > 0.02 else "DEAD"
            good = v == "HIST_OK"
        n_ok[kl] += int(good)
        if not good:
            fails[kl].append(c)
        print(f"  {c:<24}{kl:>8}{'有':>5}{nm:>8.3f}{sent:>9.3f}{vf:>9.3f}{v:>8}")
    print(f"\n  [3契約] CURRENT_ACTIVE {n_ok['ACTIVE']}/{len(CURRENT_ACTIVE_JRDB)}"
          f"  CONTEXT {n_ok['CONTEXT']}/{len(CONTEXT_JRDB)}"
          f"  HISTORY {n_ok['HISTORY']}/{len(HISTORY_JRDB)}  ABSENT={len(absent)}"
          f"  / EXPECTED={len(EXPECTED_JRDB_FULL)}")
    for grp, cs in fails.items():
        if cs:
            print(f"  [{grp} 未達] {cs}")
    if absent:
        print(f"  [ABSENT] 定義あるが featured に無い（default は 5列のみが正常・完全 augment は "
              f"--featured data/featured_jrdb.pkl を指定）: {absent[:12]}{' …' if len(absent) > 12 else ''}")


def _l4_utilization():
    from src.jrdb._store import RECORD_TYPES
    print("\n" + "=" * 88)
    print("[L4] source 利用状態（7状態＋時点クラス。『ブリッジ済』は L3 と矛盾するため廃止）")
    print(f"  {'型':<5}{'状態':<42}{'時点クラス':<18}説明")
    for rt in RECORD_TYPES:
        st, timing, note = SOURCE_STATE.get(rt, ("?", "?", "?"))
        print(f"  {rt:<5}{st:<42}{timing:<18}{note}")
    print("  凡例: MATERIALIZED=featured実在 / IMPLEMENTED_NOT_APPLIED=attach実装済だが本番build未適用 /")
    print("        INGESTED_NOT_BRIDGED=store済・未橋渡し / HISTORICAL_ONLY=今週分のみ /")
    print("        INGESTION_MISSING=未取込 / OUTCOME_ONLY=レース後。時点=direct_current/bet_time_contract/")
    print("        historical_only/outcome_only（同名 ten_idx/agari_idx が KYI(予想)とSED(実測)に両在＝")
    print("        列名でなく source×timestamp で時点判定し feature contract に持たせる）。")


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description="JRDB データ項目監査（L1 byte / L2 store / L3 featured / L4 source）")
    ap.add_argument("--featured", default=None,
                    help="L3 の対象 featured pickle（既定=本線 featured。完全 augment は "
                         "data/featured_jrdb.pkl を指定＝42列を監査）")
    args = ap.parse_args()
    _l1_layout_validity()
    try:
        _l2_store()
    except Exception as e:  # noqa: BLE001
        print(f"[L2] スキップ: {e}", file=sys.stderr)
    try:
        _l3_featured(args.featured)
    except Exception as e:  # noqa: BLE001
        print(f"[L3] スキップ: {e}", file=sys.stderr)
    _l4_utilization()
    print("\n" + "=" * 88)
    print("[総括] L1 妥当性はここで判定。L2-L3 の実体化・sentinel と L4 の未ブリッジは"
          "ローカル実行で認定（定義≠実体化＝DEAD/ABSENT を最優先で確認）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
