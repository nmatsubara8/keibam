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


def main() -> None:
    path = LocalPaths.RAW_ODDS_SNAPSHOT_PATH
    snaps = load_snapshots(path)
    print(f"path = {path}")
    print(f"total snapshots = {len(snaps):,}")
    if not snaps:
        print("→ 空。cron の保存先がこのパスと違う可能性（別マシン/別 RAW_DIR）。")
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


if __name__ == "__main__":
    main()
