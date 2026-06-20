"""オッズ力学モデル（投票シェアの確率過程）のドメイン定数（単一の定義元、I/O なし）。

単勝市場は控除率付きゼロサム構造で、シェアベクトル p（Σp=1）はシンプレックス上を
発走に向かって拡散する。経験的に「市場の重力」（1 番人気 2.4 倍は 2.0〜2.3 へ収束、
12 番人気 80 倍は 50〜120 倍の範囲でしか動かない等）が人気順位別に存在するため、
人気順バケット別の遷移統計（drift / vol）を事前分布として全モデルで共有する。
"""

from ._odds_phases import OddsPhase

# ---------------------------------------------------------------------------
# チェックポイント（タイマー取得の対象時点）
# ---------------------------------------------------------------------------

# オッズ取得スケジュール（odds_watch / select_checkpoint_races が使用）。
# 締切から離れた区間はオッズ変動が小さいため疎な基準点で取得し、締切直前は毎ティック
# 取得して推移を密に記録する（実際の取得間隔は odds_watch の --interval に一致。
# --interval を下げるほど締切直前が密になる）。
#   - SPARSE_CHECKPOINT_MINUTES: 早期の基準チェックポイント（発走 N 分前 ±許容幅）。
#   - DENSE_WINDOW_MIN: この残り分数以内は毎ティック取得（締切直前を密に）。
# フェーズ分類（オッズ力学モデル用）は classify_phase が minutes_to_post から独立に行うため、
# この取得スケジュールの変更はモデルのフェーズ構造（thirty_min/t10/t5/t0）に影響しない。
SPARSE_CHECKPOINT_MINUTES = (60, 30)
DENSE_WINDOW_MIN = 15

# 早期チェックポイントの許容幅（分）。cron/ループの実行間隔より広くとる。
CHECKPOINT_TOLERANCE_MIN = 1.5

# ---------------------------------------------------------------------------
# 人気順バケット（「市場の重力」の集計単位）
# ---------------------------------------------------------------------------

# (最小人気順位, 最大人気順位) の閉区間。18 頭立てまで対応。
RANK_BUCKETS = ((1, 1), (2, 2), (3, 3), (4, 5), (6, 8), (9, 12), (13, 18))


def bucket_for_rank(rank: int) -> int:
    """人気順位（1 始まり）→ バケット番号（0 始まり）。範囲外は最終バケット。"""
    for i, (lo, hi) in enumerate(RANK_BUCKETS):
        if lo <= rank <= hi:
            return i
    return len(RANK_BUCKETS) - 1


# ---------------------------------------------------------------------------
# 拡散の既定値（データが無い/少ない場合の事前分布）
# ---------------------------------------------------------------------------

# CLR 空間での 1 遷移あたりの drift 既定値（= 動かない）
DEFAULT_DRIFT = 0.0

# CLR 空間での隣接フェーズ遷移あたりのボラティリティ既定値。
# 締切から遠い遷移ほどプールが薄く変動が大きい（経験的な diffuse prior）。
DEFAULT_VOL_PER_STEP = {
    (OddsPhase.PREV_DAY, OddsPhase.HOURS_BEFORE): 0.30,
    (OddsPhase.HOURS_BEFORE, OddsPhase.THIRTY_MIN): 0.20,
    (OddsPhase.THIRTY_MIN, OddsPhase.T10): 0.12,
    (OddsPhase.T10, OddsPhase.T5): 0.08,
    (OddsPhase.T5, OddsPhase.T0): 0.06,
}
DEFAULT_VOL_FALLBACK = 0.15

# フェーズ別の観測ノイズ標準偏差（CLR 空間）。早期ほどプールが薄くノイジー。
DEFAULT_OBS_NOISE = {
    OddsPhase.PREV_DAY: 0.25,
    OddsPhase.HOURS_BEFORE: 0.15,
    OddsPhase.THIRTY_MIN: 0.08,
    OddsPhase.T10: 0.05,
    OddsPhase.T5: 0.04,
    OddsPhase.T0: 0.03,
}

# この観測数未満のバケットは既定値へ縮小推定する
MIN_BUCKET_COUNT = 30

# ---------------------------------------------------------------------------
# 永続化ファイル名（models/ 配下）
# ---------------------------------------------------------------------------

GRAVITY_FILENAME = "odds_gravity.json"
DYNAMICS_EVAL_FILENAME = "odds_dynamics_eval.json"

# 単勝の控除率（policies/_odds_provider.py の takeout 既定と整合）
TAKEOUT_RATE = 0.2
