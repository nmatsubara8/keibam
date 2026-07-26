"""odds_snapshots.pkl の健全性診断。

evaluate-odds-dynamics が全 NaN（n_test=0）になる原因を切り分ける。力学評価には各レースに
**複数の時間位相（T0＋それ以前）** が要る。ここで実際の蓄積を覗き、どこで単一位相に潰れているかを見る:
  - 総数・保存パス（そもそも溜まっているか）
  - captured_at の期間（4週間分あるか）
  - phase / bet_type の分布（一位相に偏っていないか・TANSHO があるか）
  - minutes_to_post の範囲（post_time が正しく効いているか。全部 0 付近や巨大なら post_time 異常）
  - レースあたり位相数（≥2 位相のレースが何本あるか＝評価に使える母数）

実行: python diag_odds_snapshots.py
"""
from __future__ import annotations

from collections import Counter
from collections import defaultdict

from src.constants._local_paths import LocalPaths
from src.preparing.odds_scheduler import load_snapshots


def diag_db() -> None:
    """DB `raw_odds_snapshots` を直読みし、captured_at 単位の真の時系列を診断する。

    pickle は (race_id, bet_type, combo, **phase**) で dedup するため、同一レースを
    同一 phase 内で複数回取得しても最新1件へ潰れる（＝時間解像度を失う）。一方 DB は
    主キーに **captured_at** を含むため取得時刻ごとの全行が残る。ユーザーの言う
    「蓄積されたデータ」から軌跡を復元できるかは、ここ（DB の captured_at 粒度）で決まる。
    """
    import pandas as pd
    from sqlalchemy import text

    from src.storage._db import get_engine

    try:
        eng = get_engine()
        with eng.connect() as conn:
            has = conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='raw_odds_snapshots'"
            )).fetchone()
            if not has:
                print("\n[DB] raw_odds_snapshots テーブル無し（このマシンで DB 未生成）。")
                return
            df = pd.read_sql("SELECT race_id, captured_at, bet_type FROM raw_odds_snapshots", conn)
    except Exception as e:  # noqa: BLE001
        print(f"\n[DB] 読込失敗（非致命）: {e}")
        return

    print(f"\n{'='*60}\n[DB raw_odds_snapshots] 行数 = {len(df):,}")
    if df.empty:
        print("→ DB も空。pickle と同様、蓄積そのものが無い。")
        return

    df["captured_at"] = pd.to_datetime(df["captured_at"], errors="coerce", utc=False)
    # レースごとの「異なる取得時刻」の数と広がり（phase 非依存・生 captured_at）。
    g = df.groupby("race_id")["captured_at"]
    ndistinct = g.nunique()
    span_min = (g.max() - g.min()).dt.total_seconds() / 60.0
    nd = sorted(ndistinct.tolist())
    n = len(nd)
    print(f"レース数 = {n:,}")
    print(f"異なる captured_at 数/レース: min={nd[0]} p50={nd[n//2]} p90={nd[min(n-1, 9*n//10)]} max={nd[-1]}")
    for k in (2, 3, 5):
        c = int((ndistinct >= k).sum())
        print(f"  captured_at が {k}種類以上のレース = {c:,} / {n:,}（={c/n:.1%}）")
    sp = sorted(span_min.dropna().tolist())
    if sp:
        m = len(sp)
        print(f"取得時刻の広がり span(分): min={sp[0]:.1f} p50={sp[m//2]:.1f} "
              f"p90={sp[min(m-1, 9*m//10)]:.1f} max={sp[-1]:.1f}")
        for thr in (5.0, 10.0, 30.0):
            c = sum(1 for s in sp if s >= thr)
            print(f"  span ≥ {thr:>4.0f}分 のレース = {c:,} / {m:,}（={c/m:.1%}: 早→遅の軌跡を復元できる候補）")
    print("→ captured_at が2種類以上 & span が十分なレースがあれば、pickle が潰した軌跡を")
    print("  DB から復元し、post_time で phase を振り直して評価データを生成できる（＝ユーザー仮説が成立）。")


def main() -> None:
    path = LocalPaths.RAW_ODDS_SNAPSHOT_PATH
    snaps = load_snapshots(path)
    print(f"path = {path}")
    print(f"total snapshots = {len(snaps):,}")
    if not snaps:
        print("→ pickle は空（phase dedup 済みのビュー）。真の時系列は DB を見る。")
        diag_db()
        return

    caps = [s.captured_at for s in snaps if s.captured_at is not None]
    if caps:
        print(f"captured_at 期間 = {min(caps)}  〜  {max(caps)}  （{(max(caps) - min(caps)).days} 日間）")

    print(f"phase 分布   = {dict(Counter(s.phase for s in snaps))}")
    print(f"bet_type分布 = {dict(Counter(s.bet_type for s in snaps))}")

    mtps = [s.minutes_to_post for s in snaps]
    mtps_sorted = sorted(mtps)
    print(f"minutes_to_post: min={mtps_sorted[0]} p50={mtps_sorted[len(mtps)//2]} "
          f"max={mtps_sorted[-1]}  （負=締切超過 / 全部同値なら post_time 異常）")

    # レースあたり位相数（全 bet_type / TANSHO 限定）
    all_rp: dict[str, set] = defaultdict(set)
    tansho_rp: dict[str, set] = defaultdict(set)
    for s in snaps:
        all_rp[s.race_id].add(s.phase)
        if s.bet_type == "tansho":
            tansho_rp[s.race_id].add(s.phase)
    npr = sorted(len(v) for v in all_rp.values())
    print(f"\nレース数 = {len(all_rp):,}")
    print(f"位相数/レース: min={npr[0]} p50={npr[len(npr)//2]} max={npr[-1]}")
    print(f"≥2 位相のレース = {sum(1 for v in all_rp.values() if len(v) >= 2):,}  "
          f"（うち TANSHO で ≥2 位相 = {sum(1 for v in tansho_rp.values() if len(v) >= 2):,}）")
    print("→ 評価に使えるのは『TANSHO かつ ≥2 位相』のレース。ここが 0 なら全 NaN の直接原因。")

    # --- 生タイムスタンプ粒度の検証（phase バケットを無視した実測の広がり）---------------
    # 「captured_at × race_id から正解を作れるのでは？」の厳密検証。phase が単一でも、
    # 各レース内の minutes_to_post が実際に広がっていれば（＝別時刻に複数回取得できていれば）、
    # phase 再定義で軌跡を復元できる余地がある。逆に span≈0 なら、どう再バケットしても
    # 単一時刻のスナップショットしか無く、早→遅の軌跡は物理的に存在しない。
    race_mtp: dict[str, list[float]] = defaultdict(list)
    for s in snaps:
        if s.minutes_to_post is not None:
            race_mtp[s.race_id].append(float(s.minutes_to_post))
    spans = sorted((max(v) - min(v)) for v in race_mtp.values() if v)
    counts = sorted(len(v) for v in race_mtp.values() if v)
    if spans:
        n = len(spans)
        print("\n[生タイムスタンプ粒度] レース内 minutes_to_post の広がり（phase 非依存）:")
        print(f"  取得回数/レース: min={counts[0]} p50={counts[n//2]} max={counts[-1]}")
        print(f"  span(分): min={spans[0]:.1f} p50={spans[n//2]:.1f} "
              f"p90={spans[min(n-1, 9*n//10)]:.1f} max={spans[-1]:.1f}")
        for thr in (2.0, 5.0, 10.0):
            k = sum(1 for sp in spans if sp >= thr)
            print(f"  span ≥ {thr:>4.0f}分 のレース = {k:,} / {n:,}  "
                  f"（={k/n:.1%}: 早→遅の軌跡を復元できる候補）")
        print("→ span がほぼ 0 のレースばかりなら、再バケットしても単一時刻しか無く軌跡は作れない。")

    print("\n例（先頭5レースの位相内訳）:")
    for rid, phs in list(all_rp.items())[:5]:
        print(f"  {rid}: {sorted(phs)}")

    # pickle は phase 単位で dedup 済み。真の時系列は DB（captured_at 粒度）にある。
    diag_db()


if __name__ == "__main__":
    main()
