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

# featured へブリッジ済みの source（L4 の裏返し）
BRIDGED_TO_FEATURED = {
    "KYI": "jrdb_*(指数群)+jrdb_pace_hms+jrdb_kijun_gap",
    "SED": "prev_deokure/soten MySpeed(jrdb_ms_*)",
    "SKB": "prev_trouble(特記TROUBLE_TOKKI)",
}
# 保存済だが featured へ未ブリッジ（latent・網羅性の伸びしろ）
UNBRIDGED_NOTE = {
    "TYB": "直前オッズ/パドック指数/馬体重（発走15分前）",
    "CYB": "調教分析: 追切指数/仕上指数/調教量評価/調教評価/コメント",
    "CHA": "本追切: テン/中間/終いF＋各指数＋併せ結果",
    "KKA": "条件別着度数(24群×1-2-3-着外)＋父/母父産駒連対率",
    "UKC": "馬マスタ: 毛色/系統コード/生産者/産地",
    "SRB": "ハロンタイム18/コーナー位置取り/トラックバイアス/ペースアップ位置",
    "KAB": "開催: 天候/芝ダ馬場状態/馬場差/草丈/降水量",
    "BAC": "番組: 賞金/発走時刻/馬券発売フラグ（race_info 供給）",
    "KSA": "騎手マスタ: 年別/通算 平地・障害成績",
    "CSA": "調教師マスタ: 年別/通算成績",
    "KTA": "登録馬(馬番確定前): IDM/脚質/距離適性/前走リンク",
    "HJC": "払戻(return 側・featured 特徴ではない)",
}


def _l1_layout_validity():
    import src.jrdb._layouts as L
    print("=" * 88)
    print("[L1] 仕様妥当性: JRDB 形式のオフセット境界＋取得 byte 率（データ不要）")
    RL = L.RECORD_LEN
    dict_layouts = ["KYI", "SED", "TYB", "CYB", "CHA", "KKA", "UKC", "SRB",
                    "KSA", "CSA", "KTA", "BAC", "KAB", "SKB"]
    print(f"  {'型':<5}{'reclen':>7}{'項目':>5}{'max終端':>8}{'境界':>8}{'取得byte率':>10}")
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
    print("  ※ 取得 byte 率が低い形式（KYI~59%/TYB~51%/SKB~9%）は未 parse バイトが残る（下記 L4 と併読）。")


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


def _l3_featured():
    import numpy as np
    import pandas as pd
    from app._model_eval import load_featured_data
    from src.jrdb._augment import JRDB_COLS, KYI_FEATURE_MAP, MYSPEED_COLS
    print("\n" + "=" * 88)
    print("[L3] featured 実体化: 定義された JRDB 特徴が featured に**実在**するか（定義≠実体化）")
    feat = load_featured_data()
    if feat is None or feat.empty:
        print("  featured を読めません（ローカルで実行）。")
        return
    rid = pd.Series(feat.index.astype(str))
    year = pd.to_numeric(rid.str[:4], errors="coerce")
    jra = rid.str[4:6].isin({f"{i:02d}" for i in range(1, 11)})
    recent = jra & (year >= 2020)                      # 直近 JRA で materialization を見る
    expected = list(dict.fromkeys(list(KYI_FEATURE_MAP.values())
                    + ["jrdb_pace_hms", "jrdb_kijun_gap"] + list(MYSPEED_COLS)
                    + ["prev_deokure", "prev_trouble"]))
    present, absent, dead = [], [], []
    print(f"  {'特徴':<26}{'実在':>5}{'非欠測(2020+JRA)':>16}{'sentinel率':>10}{'判定':>8}")
    for c in expected:
        if c not in feat.columns:
            absent.append(c)
            print(f"  {c:<26}{'無':>5}{'—':>16}{'—':>10}{'ABSENT':>8}")
            continue
        col = pd.to_numeric(feat.loc[recent.to_numpy(), c], errors="coerce")
        nm = float(col.notna().mean()) if len(col) else 0.0
        sent = float((col <= -99).mean()) if len(col) else 0.0   # -99.9 等 JRDB fill
        verdict = "OK" if nm >= 0.5 and sent < 0.2 else ("DEAD" if nm < 0.05 else "薄い")
        (present if verdict == "OK" else dead).append(c)
        print(f"  {c:<26}{'有':>5}{nm:>15.3f}{sent:>10.3f}{verdict:>8}")
    print(f"\n  実体化 OK={len(present)}  薄い/DEAD={len(dead)}  ABSENT(列なし)={len(absent)}")
    if absent:
        print(f"  [ABSENT] 定義されているが featured に無い（attach 未適用の疑い）: {absent}")
    print(f"  ※ 定義総数={len(expected)}（KYI_FEATURE_MAP {len(KYI_FEATURE_MAP)}＋pace_hms/kijun_gap"
          f"＋MySpeed {len(MYSPEED_COLS)}＋prev_*）")


def _l4_unbridged():
    from src.jrdb._store import RECORD_TYPES
    print("\n" + "=" * 88)
    print("[L4] 未ブリッジ（保存済だが featured へ橋渡し無し＝latent 信号・網羅性の伸びしろ）")
    print("  [ブリッジ済]")
    for k, v in BRIDGED_TO_FEATURED.items():
        print(f"    {k}: {v}")
    print("  [未ブリッジ（ingested-but-unused）]")
    for rt in RECORD_TYPES:
        if rt not in BRIDGED_TO_FEATURED:
            print(f"    {rt}: {UNBRIDGED_NOTE.get(rt, '?')}")


def main() -> int:
    _l1_layout_validity()
    try:
        _l2_store()
    except Exception as e:  # noqa: BLE001
        print(f"[L2] スキップ: {e}", file=sys.stderr)
    try:
        _l3_featured()
    except Exception as e:  # noqa: BLE001
        print(f"[L3] スキップ: {e}", file=sys.stderr)
    _l4_unbridged()
    print("\n" + "=" * 88)
    print("[総括] L1 妥当性はここで判定。L2-L3 の実体化・sentinel と L4 の未ブリッジは"
          "ローカル実行で認定（定義≠実体化＝DEAD/ABSENT を最優先で確認）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
