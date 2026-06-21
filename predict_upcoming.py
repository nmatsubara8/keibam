"""未実施レース（明日・当日）の出馬表＋暫定オッズで予測・推奨を出す CLI。

既存の予測ページ（app/pages/2_prediction.py）は featured_data に既にあるレース＝
取込済み（=実施済み）レースしか予測できない。本スクリプトは「まだ実施していないレース」を
出馬表（shutuba）ページからスクレイプし、暫定（前売り）単勝オッズを使って予測する。

フロー（tests/integration/test_live_prediction.py の実証済み経路を踏襲）:
  出馬表スクレイプ → ShutubaTableProcessor → ShutubaDataMerger（過去成績/血統/レース情報を結合）
  → FeatureEngineering → model.effective_model → run_prediction（config.yaml の検証済み戦略）

推奨は config.yaml の戦略（単勝・EV>1.1・オッズ≤15・1/4ケリー）に従う。暫定オッズで
選定するため、これは docs/betting_strategy.md §6 で言う「締切前オッズでの実戦」そのもの。

実行:
  # 明日の全レースを予測（既定）
  python predict_upcoming.py

  # 日付指定（開催日 YYYYMMDD）
  python predict_upcoming.py --date 20260621

  # 特定レースだけ
  python predict_upcoming.py --date 20260621 --race-id 202605030211 202605030212

  # スクレイプ済みの出馬表 pickle を使う（ネット不要・予測部の単体確認用）
  python predict_upcoming.py --shutuba-pkl data/tmp/shutuba.pkl
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
import tempfile
import warnings

# 推論時は特徴量を numpy 配列（列名なし）で渡すため LightGBM が毎回出す無害な警告を抑止する。
# （モデルは列名つきで学習済み。列整合は _align_features で担保しているため問題ない。）
warnings.filterwarnings(
    "ignore", message="X does not have valid feature names", category=UserWarning,
)

logger = logging.getLogger(__name__)


_ORANGE = "38;5;208"  # オレンジ: 消滅可能性が高い（推奨中だが EV が閾値近接＆下落中）
_GRAY = "90"          # グレー: 既に消滅（推奨から外れた・参考表示）
_AT_RISK_EV = 1.15    # EV がこの未満かつ前ティックより下落 → 消滅可能性が高い


def _color_for_count(count: int) -> str:
    """出現回数→ANSI色コード。1回目=赤・2〜5回目=黄・6回目以降=緑。"""
    if count <= 1:
        return "31"  # 赤
    if count <= 5:
        return "33"  # 黄
    return "32"  # 緑


def _ansi(text: str, code: str, enable: bool) -> str:
    return f"\033[{code}m{text}\033[0m" if enable else text


def _colorize(text: str, count: int, enable: bool) -> str:
    return _ansi(text, _color_for_count(count), enable)


_JRA_PLACE = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟", "05": "東京",
    "06": "中山", "07": "中京", "08": "京都", "09": "阪神", "10": "小倉",
}


def _race_label(race_id, post_str=None) -> str:
    """race_id（年4+競馬場2+開催2+日2+R2）から『競馬場NR』表記を作る。post_str があれば発走時刻も付す。"""
    rid = str(race_id)
    place = _JRA_PLACE.get(rid[4:6], f"場{rid[4:6]}") if len(rid) >= 12 else "?"
    rno = str(int(rid[-2:])) if rid[-2:].isdigit() else rid[-2:]
    label = f"{place}{rno}R"
    if post_str:
        label += f" 発走{post_str}"
    return label


def _scrape_shutuba_day(date_yyyymmdd: str, race_ids: list[str] | None):
    """開催日の出馬表（暫定オッズ込み）をスクレイプし、(結合DataFrame, 発走時刻dict) を返す。

    race_ids 未指定なら当日の全レースを自動検出する。取得失敗レースはスキップ。
    """
    import pandas as pd

    from src.preparing._scrape_shutuba import scrape_race_id_race_time_list
    from src.preparing._scrape_shutuba import scrape_shutuba_table

    date_str = f"{date_yyyymmdd[:4]}/{date_yyyymmdd[4:6]}/{date_yyyymmdd[6:8]}"
    # 開催レースと発走時刻を取得（--race-id 指定時はその部分集合に絞る）。
    all_ids, all_times = scrape_race_id_race_time_list(date_yyyymmdd)
    if not all_ids:
        logger.error("開催レースが見つかりません: %s（非開催日 or 出馬表未公開）", date_yyyymmdd)
        return None, {}
    post_of = {str(r): t for r, t in zip(all_ids, all_times, strict=False)}
    ids = [str(r) for r in race_ids] if race_ids else [str(r) for r in all_ids]

    def _post_dt(rid: str):
        t = post_of.get(rid)
        if not t:
            return None
        try:
            return dt.datetime.strptime(f"{date_yyyymmdd} {t}", "%Y%m%d %H:%M")
        except ValueError:
            return None

    # 発走順（早い順）に並べて「次のレース」を分かりやすくする。
    ids.sort(key=lambda r: _post_dt(r) or dt.datetime.max)
    print(f"出馬表・暫定オッズを取得: {date_yyyymmdd} / {len(ids)} レース（発走順）")

    # ポライトネス: レース間に間隔を入れ、窓内レースが重なってもバースト送信にしない。
    # 1時間あたり上限は scrape_shutuba_table→PlaywrightScraper.fetch の hourly limiter が別途担保。
    import time as _time

    from src.preparing._rate_limiter import polite_interval
    delay = float(os.environ.get("KEIBA_SCRAPE_DELAY", "1.0"))

    frames = []
    for i, rid in enumerate(ids, 1):
        pdt = _post_dt(rid)
        post_s = post_of.get(rid, "??:??")
        if pdt is not None:
            mtp = int((pdt - dt.datetime.now()).total_seconds() // 60)
            when = f"発走{post_s}・あと{mtp}分" + ("（発走済み）" if mtp < 0 else "")
        else:
            when = f"発走{post_s}"
        label = _race_label(rid)
        print(f"  [{i:2d}/{len(ids)}] {label} [{rid}]  {when} … 取得中", flush=True)
        tmp = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tf:
                tmp = tf.name
            scrape_shutuba_table(rid, date_str, tmp)
            df = pd.read_pickle(tmp)
            frames.append(df)
            print(f"  [{i:2d}/{len(ids)}] {label} [{rid}]  ✓ 取得済み（{len(df)}頭）", flush=True)
        except Exception as e:  # noqa: BLE001 — 1レースの失敗で全体を止めない
            print(f"  [{i:2d}/{len(ids)}] {label} [{rid}]  ✗ 取得失敗: {e}", flush=True)
        finally:
            if tmp and os.path.exists(tmp):
                os.unlink(tmp)
        # 次レースまで待機（最後のレースの後は待たない）。delay<=0 で無効化可。
        if i < len(ids):
            wait = polite_interval(delay)
            if wait > 0:
                _time.sleep(wait)
    if not frames:
        return None, post_of
    return pd.concat(frames), post_of


def _build_featured(shutuba_df):
    """出馬表 DataFrame → 学習と同じ特徴量セットの featured_data を構築する。

    特徴量チェーンは tests/integration/test_live_prediction.py（出馬表データで動作実証済み）に
    準拠。学習時チェーン（src/pipeline/run_pipeline.py）と差異が出て列数が合わない場合は、
    そちらに合わせて調整する（予測の特徴量整合が崩れた兆候）。
    """
    from src.preprocessing._feature_engineering import FeatureEngineering
    from src.preprocessing._horse_info_processor import HorseInfoProcessor
    from src.preprocessing._horse_results_processor import HorseResultsProcessor
    from src.preprocessing._peds_processor import PedsProcessor
    from src.preprocessing._race_info_processor import RaceInfoProcessor
    from src.preprocessing._shutuba_data_merger import ShutubaDataMerger
    from src.preprocessing._shutuba_table_processor import ShutubaTableProcessor
    from src.constants._local_paths import LocalPaths

    paths = LocalPaths()
    # 発走前は馬体重が未発表（空欄）のことがある。'480(+2)' 形式を前提とする
    # ResultsProcessor の体重パースが落ちるため、空欄を '0(0)'（体重=体重変化=0 の
    # 中立値）に正規化してから渡す。
    from src.constants._results_cols import ResultsCols
    wcol = ResultsCols.WEIGHT_AND_DIFF
    if wcol in shutuba_df.columns:
        s = shutuba_df[wcol].astype(str).str.strip()
        blank = ~s.str.contains("(", regex=False) | s.isin(["", "nan", "None", "NaN"])
        if blank.any():
            shutuba_df = shutuba_df.copy()
            shutuba_df.loc[blank, wcol] = "0(0)"
    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tf:
        shutuba_pkl = tf.name
    shutuba_df.to_pickle(shutuba_pkl)
    try:
        stp = ShutubaTableProcessor(shutuba_pkl)
        merger = ShutubaDataMerger(
            stp,
            HorseResultsProcessor(paths.RAW_HORSE_RESULTS_PATH),
            HorseInfoProcessor(paths.RAW_HORSE_INFO_PATH),
            PedsProcessor(paths.RAW_PEDS_PATH),
            target_cols=["着順"],
            group_cols=["騎手"],
            race_info_processor=RaceInfoProcessor(paths.RAW_RACE_INFO_PATH),
        )
        merger.merge()
        return (
            FeatureEngineering(merger)
            .add_interval().add_agedays()
            .add_interaction_features().add_race_level_zscore()
            .dumminize_kaisai().dumminize_sex().dumminize_weather()
            .dumminize_race_type()
            .dumminize_ground_state1().dumminize_ground_state2()
            .dumminize_ground_state()
            .dumminize_around().dumminize_race_class()
            .encode_horse_id().encode_jockey_id().encode_trainer_id()
            .encode_owner_id().encode_breeder_id()
        ).featured_data
    finally:
        if os.path.exists(shutuba_pkl):
            os.unlink(shutuba_pkl)


def _minutes_to_post(date_yyyymmdd, post_s, now):
    if not post_s:
        return None
    try:
        pdt = dt.datetime.strptime(f"{date_yyyymmdd} {post_s}", "%Y%m%d %H:%M")
        return int((pdt - now).total_seconds() // 60)
    except (ValueError, TypeError):
        return None


def _save_live_odds(shutuba_df, post_of, date_yyyymmdd, captured_at):
    """スクレイプした全レース・全馬の暫定単勝オッズを CSV に追記蓄積する。"""
    import os

    import pandas as pd

    from src.constants._results_cols import ResultsCols

    cap = captured_at.isoformat(timespec="seconds")
    rows = []
    for race_id in shutuba_df.index.unique():
        rid = str(race_id)
        post_s = post_of.get(rid, "")
        mtp = _minutes_to_post(date_yyyymmdd, post_s, captured_at)
        rno = int(rid[-2:]) if rid[-2:].isdigit() else rid[-2:]
        for _, r in shutuba_df.loc[[race_id]].iterrows():
            rows.append({
                "captured_at": cap, "race_id": rid,
                "場": _JRA_PLACE.get(rid[4:6], rid[4:6]), "R": rno,
                "発走": post_s, "残り分": mtp,
                "馬番": r.get(ResultsCols.UMABAN), "単勝": r.get(ResultsCols.TANSHO_ODDS),
            })
    if not rows:
        return
    os.makedirs("data/live", exist_ok=True)
    path = f"data/live/odds_{date_yyyymmdd}.csv"
    pd.DataFrame(rows).to_csv(path, mode="a", header=not os.path.exists(path),
                              index=False, encoding="utf-8-sig")
    logger.info("オッズ蓄積: %d 行を %s に追記", len(rows), path)


def _save_live_bets(bet_rows, date_yyyymmdd):
    """推奨買い目（100円単位の購入額）を CSV に追記する（発走後の結果検証用）。"""
    import os

    import pandas as pd

    if not bet_rows:
        return
    os.makedirs("data/live", exist_ok=True)
    path = f"data/live/bets_{date_yyyymmdd}.csv"
    pd.DataFrame(bet_rows).to_csv(path, mode="a", header=not os.path.exists(path),
                                  index=False, encoding="utf-8-sig")
    logger.info("買い目記録: %d 件を %s に追記", len(bet_rows), path)


def _load_appearance_counts(date_yyyymmdd):
    """既存の bets_<date>.csv から (counts, prev_ev) を復元する。

    --loop を再起動しても過去ティックの出現回数（色分け赤→黄→緑）と、各買い目の直近 EV
    （消滅可能性=EV下落判定）を引き継ぐため。
      counts  : {(race_id,馬番): 出現回数}（= distinct captured_at 数）
      prev_ev : {(race_id,馬番): 直近 captured_at の EV}
    ファイルが無ければ空。--save-odds で bets が追記され続けるのが前提。
    """
    import os

    path = f"data/live/bets_{date_yyyymmdd}.csv"
    if not os.path.exists(path):
        return {}, {}
    import pandas as pd

    try:
        df = pd.read_csv(path)
    except Exception as e:  # noqa: BLE001
        logger.warning("bets履歴の読込失敗（カウント0から開始）: %s", e)
        return {}, {}
    if df.empty or not {"race_id", "馬番", "captured_at"} <= set(df.columns):
        return {}, {}
    g = df.groupby(["race_id", "馬番"])["captured_at"].nunique()
    counts = {(str(rid), int(uma)): int(n) for (rid, uma), n in g.items()}
    prev_ev = {}
    if "EV" in df.columns:
        last = df.sort_values("captured_at").groupby(["race_id", "馬番"]).tail(1)
        for _, r in last.iterrows():
            try:
                prev_ev[(str(r["race_id"]), int(r["馬番"]))] = float(r["EV"])
            except (TypeError, ValueError):
                pass
    return counts, prev_ev


def _print_recommendations(race_id, candidates, bankroll: float, unit: int = 100, post_of=None,
                           counts=None, race_odds=None, prev_ev=None):
    """1レースの推奨馬券を表示し、(投資合計, 採用行) を返す。

    JRA は unit 円（既定100）単位でしか購入できないため、ケリーの生額を unit 単位に
    丸める。丸めて 0 になる薄いベット（< unit/2 円）は購入不可のため見送る。
    色分け（端末出力時のみ。counts/prev_ev を渡したとき）:
      赤=初出 / 黄=2〜5回 / 緑=6回以上（出現回数＝安定度）
      オレンジ=消滅可能性が高い（EV が閾値近接かつ前ティックより下落＝剥がれかけ）
      グレー=既に消滅（前回まで推奨→今回外れた。現在オッズ付き・参考表示）
    """
    from src.constants._bet_types import BetType

    tansho = [c for c in candidates if c.bet_type in ("tansho", BetType.TANSHO)]
    # unit 単位に丸め、最小単位未満は見送り。
    rows = []
    for c in sorted(tansho, key=lambda c: -c.expected_value):
        stake = int(round(c.stake / unit)) * unit
        if stake >= unit:
            rows.append((c, stake))

    # 消滅検出: 過去に推奨された（counts にある）が今回は外れた買い目。
    gone = []
    if counts is not None and race_odds is not None:
        cur = {c.combo[0] for c, _ in rows}
        rid_s = str(race_id)
        gone = sorted(u for (r, u) in counts if r == rid_s and u not in cur)

    if not rows and not gone:
        return 0.0, []

    use_color = counts is not None and sys.stdout.isatty()
    post_str = (post_of or {}).get(str(race_id))
    print(f"\n■ {_race_label(race_id, post_str)}  [{race_id}]")
    print(f"  {'馬番':>4}{'オッズ':>8}{'勝率':>8}{'EV':>7}{'購入額':>10}{'回数':>6}{'状態':>6}")
    total = 0.0
    for c, stake in rows:
        total += stake
        key = (str(race_id), c.combo[0])
        n = counts.get(key, 0) + 1 if counts is not None else 1
        if counts is not None:
            counts[key] = n
        # 消滅可能性: EV が閾値近接かつ前ティックより下落（剥がれかけ）。
        at_risk = False
        if prev_ev is not None:
            pe = prev_ev.get(key)
            at_risk = c.expected_value < _AT_RISK_EV and pe is not None and c.expected_value < pe
            prev_ev[key] = float(c.expected_value)
        mark = "⚠落" if at_risk else ""
        line = (f"  {c.combo[0]:>4}{c.odds:>8.1f}{c.probability:>8.3f}"
                f"{c.expected_value:>7.2f}{stake:>10,d}{n:>6}{mark:>6}")
        print(_ansi(line, _ORANGE, use_color) if at_risk else _colorize(line, n, use_color))
    for u in gone:
        od = (race_odds or {}).get(u)
        n = counts[(str(race_id), u)]
        od_s = f"{od:>8.1f}" if isinstance(od, (int, float)) else f"{'?':>8}"
        line = f"  {u:>4}{od_s}{'—':>8}{'—':>7}{'—':>10}{n:>6}{'消滅':>6}"
        print(_ansi(line, _GRAY, use_color))
    if rows:
        print(f"  → 購入合計 {int(total):,} 円（{unit}円単位 / bankroll {int(bankroll):,} 円の "
              f"{total / bankroll * 100:.1f}%）")
    return total, rows


def _align_features(featured, model):
    """featured の特徴量列をモデル学習時(feature_names_)の順序・セットに揃える。

    KeibaAI.calc_score と同じ整合（不足列は0埋め・余分列は除外・score_policy が参照する
    メタ列=馬番/枠番/単勝等は保持）を施す。run_prediction は effective_model を直接使い
    この整合を経ないため、ここで明示的に行わないと学習時と列数が合わず predict が落ちる。
    """
    import pandas as pd

    names = getattr(model, "feature_names_", None)
    if not names:
        logger.warning("モデルに feature_names_ が無く列整合をスキップ（旧モデル？）")
        return featured
    from src.policies._score_policy import META_COLS

    meta = [c for c in META_COLS if c in featured.columns]
    feat = [c for c in names if c not in meta]
    missing = [c for c in feat if c not in featured.columns]
    extra = [c for c in featured.columns if c not in names and c not in meta]
    if missing or extra:
        logger.info("列整合: 不足%d列を0埋め / 余分%d列を除外", len(missing), len(extra))
    x_feat = featured.reindex(columns=feat, fill_value=0)
    out = pd.concat([featured[meta], x_feat], axis=1)
    out.index = featured.index
    return out


def _race_schedule(date_yyyymmdd: str):
    """当日の (race_id, post_datetime) リストを取得する。"""
    from src.preparing._scrape_shutuba import scrape_race_id_race_time_list

    ids, times = scrape_race_id_race_time_list(date_yyyymmdd)
    sched = []
    for r, t in zip(ids or [], times or [], strict=False):
        try:
            pdt = dt.datetime.strptime(f"{date_yyyymmdd} {t}", "%Y%m%d %H:%M")
        except (ValueError, TypeError):
            pdt = None
        sched.append((str(r), pdt))
    return sched


def _in_window(schedule, now, lo: float, hi: float):
    """発走まで [lo, hi] 分のレース race_id を発走順で返す。"""
    out = []
    for rid, pdt in schedule:
        if pdt is None:
            continue
        mtp = (pdt - now).total_seconds() / 60
        if lo <= mtp <= hi:
            out.append((mtp, rid))
    return [rid for _, rid in sorted(out)]


def _predict_for_races(date_yyyymmdd, race_ids, shutuba_pkl, model, op_config, unit=100,
                       save_odds=False, counts=None, prev_ev=None):
    """指定レース（race_ids、None で全レース）を予測し推奨を表示。(推奨数, 投資合計) を返す。

    save_odds=True なら、スクレイプした全馬の暫定オッズ（data/live/odds_*.csv）と推奨買い目
    （data/live/bets_*.csv）を captured_at・残り分つきで追記蓄積する。
    """
    import pandas as pd

    from app._prediction_service import run_prediction

    captured_at = dt.datetime.now()
    if shutuba_pkl:
        shutuba_df, post_of = pd.read_pickle(shutuba_pkl), {}
    else:
        shutuba_df, post_of = _scrape_shutuba_day(date_yyyymmdd, race_ids)
    if shutuba_df is None or shutuba_df.empty:
        logger.error("出馬表が空のため予測スキップ")
        return 0, 0.0
    if save_odds:
        _save_live_odds(shutuba_df, post_of, date_yyyymmdd, captured_at)
    featured = _build_featured(shutuba_df)
    if featured is None or featured.empty:
        logger.error("featured が空のため予測スキップ")
        return 0, 0.0
    # run_prediction は effective_model（生LGBM）を使い KeibaAI.calc_score の列整合を
    # 経ないため、学習時(feature_names_)の列順・セットに揃える（不足0埋め・余分除外・メタ保持）。
    featured = _align_features(featured, model)

    # 現在オッズ（race_id→{馬番:単勝}）。消滅した買い目のオレンジ表示に使う（loop時のみ構築）。
    odds_by_race = {}
    if counts is not None:
        from src.constants._results_cols import ResultsCols
        for rid in shutuba_df.index.unique():
            m = {}
            for _, r in shutuba_df.loc[[rid]].iterrows():
                try:
                    m[int(r[ResultsCols.UMABAN])] = float(r[ResultsCols.TANSHO_ODDS])
                except (TypeError, ValueError):
                    pass
            odds_by_race[str(rid)] = m

    grand_total = 0.0
    n_reco = 0
    bet_rows = []
    cap = captured_at.isoformat(timespec="seconds")
    for race_id in sorted(featured.index.unique()):
        X = featured.loc[[race_id]]
        try:
            candidates = run_prediction(model.effective_model, X, op_config)
        except Exception as e:  # noqa: BLE001
            logger.warning("予測失敗 race_id=%s: %s", race_id, e)
            continue
        spent, placed = _print_recommendations(race_id, candidates, op_config.bankroll, unit,
                                                post_of, counts, odds_by_race.get(str(race_id)),
                                                prev_ev)
        if spent > 0:
            grand_total += spent
            n_reco += 1
        if save_odds:
            rid = str(race_id)
            post_s = post_of.get(rid, "")
            mtp = _minutes_to_post(date_yyyymmdd, post_s, captured_at)
            rno = int(rid[-2:]) if rid[-2:].isdigit() else rid[-2:]
            for c, stake in placed:
                bet_rows.append({
                    "captured_at": cap, "race_id": rid,
                    "場": _JRA_PLACE.get(rid[4:6], rid[4:6]), "R": rno,
                    "発走": post_s, "残り分": mtp, "馬番": c.combo[0],
                    "単勝": c.odds, "勝率": round(float(c.probability), 4),
                    "EV": round(float(c.expected_value), 3), "購入額": stake,
                })
    if save_odds:
        _save_live_bets(bet_rows, date_yyyymmdd)
    return n_reco, grand_total


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="未実施レースを暫定オッズで予測（検証済み単勝戦略）")
    ap.add_argument("--date", default=None, help="開催日 YYYYMMDD（既定=明日）")
    ap.add_argument("--race-id", dest="race_ids", nargs="+", default=None,
                    help="対象 race_id を絞る（未指定なら当日全レース）")
    ap.add_argument("--window", default=None,
                    help="発走まで LO-HI 分のレースだけ予測（例: 30-60）。実戦の自動再予測用")
    ap.add_argument("--loop", action="store_true",
                    help="常駐ループ。--window 内のレースを --interval 秒ごとに再予測（既定窓 30-60分前）")
    ap.add_argument("--interval", type=int, default=300, help="--loop 時の実行間隔（秒、既定300）")
    ap.add_argument("--unit", type=int, default=100,
                    help="購入単位（円、既定100）。ケリー額をこの単位に丸め、未満は見送り")
    ap.add_argument("--save-odds", action="store_true",
                    help="取得した全馬の暫定オッズと推奨買い目を data/live/ に追記蓄積する")
    ap.add_argument("--shutuba-pkl", default=None,
                    help="スクレイプ済み出馬表 pickle を使う（ネット不要。予測部の確認用）")
    args = ap.parse_args()

    from app._data_loader import load_latest_model
    from app._data_loader import load_operation_config

    op_config = load_operation_config("config.yaml")
    date_yyyymmdd = args.date or (dt.date.today() + dt.timedelta(days=1)).strftime("%Y%m%d")
    model = load_latest_model()

    lo = hi = None
    if args.window:
        try:
            lo_s, hi_s = args.window.split("-")
            lo, hi = float(lo_s), float(hi_s)
        except ValueError:
            ap.error("--window は LO-HI 形式で指定（例: 30-60）")

    odds_cap = "なし" if op_config.max_odds == float("inf") else f"{op_config.max_odds:.0f}倍"
    header = (f"EV下限={op_config.tansho_ev_threshold or 'BetThresholds既定'} / オッズ上限={odds_cap}"
              f" / ケリー×{op_config.kelly_fraction_ratio} / bankroll={int(op_config.bankroll):,}円")

    def _run_once(race_ids, counts=None, prev_ev=None):
        print("=" * 70)
        print(f"予測（{date_yyyymmdd}） — 検証済み単勝戦略  {header}")
        if counts is not None:
            print("  色: 赤=初出/黄=2〜5回/緑=6回以上 ・ オレンジ=消滅可能性高(EV下落) ・ 灰=消滅")
        print("=" * 70)
        n, total = _predict_for_races(date_yyyymmdd, race_ids, args.shutuba_pkl, model,
                                      op_config, args.unit, args.save_odds, counts, prev_ev)
        print("\n" + "=" * 70)
        if n == 0:
            print("推奨馬券なし（EV>閾値 かつ オッズ≤上限 を満たす馬がいない）")
        else:
            print(f"推奨 {n} レース / 投資合計 {int(total):,} 円")
        print("※ 暫定オッズでの推奨。締切に近いほど確定オッズに収束します。")
        print("=" * 70)

    # --- ループ運用: 窓内レースを締切接近まで定期再予測 ---
    if args.loop:
        import time
        if lo is None:
            lo, hi = 30.0, 60.0  # 既定の窓（発走 30〜60 分前）
        schedule = _race_schedule(date_yyyymmdd)
        if not schedule:
            logger.error("開催レースが見つかりません: %s（非開催日 or 出馬表未公開）", date_yyyymmdd)
            return
        logger.info("ループ開始: %s / %d レース / 窓 %.0f-%.0f分前 / 間隔 %d秒",
                    date_yyyymmdd, len(schedule), lo, hi, args.interval)
        # 出現回数（色分け赤→黄→緑）と直近EV（消滅可能性=EV下落判定）。
        # 既存の bets_<date>.csv 履歴から復元 → --loop 再起動でも回数・トレンドを引き継ぐ。
        appear_counts, prev_ev = _load_appearance_counts(date_yyyymmdd)
        if appear_counts:
            logger.info("bets履歴から %d 件の買い目の状態を復元（再起動時も継続）",
                        len(appear_counts))
        # 起動時に現時点の全レースを1回予測（即座に現状把握。--save-odds なら現在オッズも保存）。
        # その後、窓ベースのスケジュール待機に入る。
        print(f"\n[{dt.datetime.now():%H:%M}] 起動時スナップショット: 全 {len(schedule)} レースを予測 …")
        _run_once(None, appear_counts, prev_ev)
        print(f"\n[{dt.datetime.now():%H:%M}] スケジュール待機開始（窓 {lo:.0f}-{hi:.0f}分前 / {args.interval}秒）…")
        while True:
            now = dt.datetime.now()
            ids = _in_window(schedule, now, lo, hi)
            if ids:
                print(f"\n[{now:%H:%M}] 窓内 {len(ids)} レースを再予測 …")
                _run_once(ids, appear_counts, prev_ev)
            # 終了: 窓上限(lo)以上のレースがもう無い＝今後再予測対象が出ない。
            if not any(pdt is not None and (pdt - now).total_seconds() / 60 >= lo
                       for _, pdt in schedule):
                logger.info("全レースが窓を通過。ループ終了。")
                break
            time.sleep(args.interval)
        return

    # --- 単発実行 ---
    race_ids = args.race_ids
    if lo is not None and race_ids is None:
        race_ids = _in_window(_race_schedule(date_yyyymmdd), dt.datetime.now(), lo, hi)
        if not race_ids:
            print(f"発走まで {lo:.0f}-{hi:.0f}分のレースは現在ありません。")
            return
    _run_once(race_ids)


if __name__ == "__main__":
    main()
