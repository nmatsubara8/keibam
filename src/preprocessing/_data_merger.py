"""§2c/2d/2e/2i/2j 特徴量生成を含むデータマージクラス。

§2i: 多窓（5/9/20R）× 多統計量（mean/std/max/min/median）集計
§2c: 騎手・調教師集計特徴量（jockey_win_rate, jockey_avg_rank, trainer_win_rate, trainer_avg_rank）
§2d: 脚質集計特徴量（pace_median, leg_type_binary, pace_at_distance）
§2e: コース条件別集計特徴量（win_rate_at_distance, avg_rank_at_course_type）
§2j: 種牡馬集計特徴量（sire_win_rate, sire_avg_rank, sire_recent_win_rate）
"""

import logging
import sys
from typing import ClassVar
from typing import Optional

import pandas as pd
from tqdm.auto import tqdm

from src.constants._feature_cols import (
    JOCKEY_RECENT_N,
    N_RACES_LIST,
    SIRE_RECENT_YEARS,
)
from src.constants._horse_results_cols import HorseResultsCols as HRCols
from src.preprocessing import _horse_features as _hf
from src.preprocessing._data_cleaner import convert_column_types
from src.preprocessing._data_cleaner import dict_selector
from src.preprocessing._horse_info_processor import HorseInfoProcessor
from src.preprocessing._horse_results_processor import HorseResultsProcessor
from src.preprocessing._peds_processor import PedsProcessor
from src.preprocessing._race_info_processor import RaceInfoProcessor
from src.preprocessing._results_processor import ResultsProcessor

sys.maxsize = sys.maxsize

logger = logging.getLogger(__name__)


class DataMerger:
    def __init__(
        self,
        results_processor: ResultsProcessor,
        race_info_processor: RaceInfoProcessor,
        horse_results_processor: HorseResultsProcessor,
        horse_info_processor: HorseInfoProcessor,
        peds_processor: PedsProcessor,
        target_cols: list,
        group_cols: list,
        training_df: Optional[pd.DataFrame] = None,
        paddock_df: Optional[pd.DataFrame] = None,
        comment_df: Optional[pd.DataFrame] = None,
        yoso_marks_df: Optional[pd.DataFrame] = None,
        person_yearly_df: Optional[pd.DataFrame] = None,
        yoso_predictor_df: Optional[pd.DataFrame] = None,
        odds_signals_df: Optional[pd.DataFrame] = None,
    ):
        self._results = results_processor.preprocessed_data
        self._race_info = race_info_processor.preprocessed_data
        self._horse_results = horse_results_processor.preprocessed_data
        self._horse_info = horse_info_processor.preprocessed_data.drop(["owner_id"], axis=1)
        self._peds = peds_processor.preprocessed_data
        # レース当日ノート（(race_id, 馬番) 属性）。未提供なら空＝マージは no-op。
        self._training = training_df if training_df is not None else pd.DataFrame()
        self._paddock = paddock_df if paddock_df is not None else pd.DataFrame()
        self._comment = comment_df if comment_df is not None else pd.DataFrame()
        # 予想印ロング（race_id×馬番×予想家）。未提供なら空＝マージは no-op。
        self._yoso_marks = yoso_marks_df if yoso_marks_df is not None else pd.DataFrame()
        # 人物の年度別成績（entity_id×year）。未提供なら空＝マージは no-op。
        self._person_yearly = person_yearly_df if person_yearly_df is not None else pd.DataFrame()
        # 予想家スキル prior（predictor_yid×1行）。未提供なら空＝マージは no-op。
        self._yoso_predictor = yoso_predictor_df if yoso_predictor_df is not None else pd.DataFrame()
        # 市場歪み特徴（(race_id, 馬番) × overlay 群）。未提供なら空＝マージは no-op。
        self._odds_signals = odds_signals_df if odds_signals_df is not None else pd.DataFrame()
        self._target_cols = target_cols
        # (horse_id, group_col) 集計は着順のみに限定して列爆発を防ぐ（馬×騎手の組合せは
        # 多窓×多統計で膨らみやすい）。馬単独の多窓集計は target_cols 全体を使う。
        self._group_target_cols = ["着順"] if "着順" in target_cols else target_cols
        self._group_cols = group_cols
        self._merged_data = pd.DataFrame()
        self._separated_results_dict: dict = {}
        self._separated_horse_results_dict: dict = {}
        self._separated_hr_with_sire_dict: dict = {}

    def merge(self):
        import os as _os
        import time as _time

        def _step(name: str, fn) -> None:
            t0 = _time.perf_counter()
            fn()
            logger.info("[merge] %s: %.1fs", name, _time.perf_counter() - t0)

        self._normalize_join_keys()
        _step("race_info", self._merge_race_info)
        _step("race_day_notes", self._merge_race_day_notes)
        _step("yoso_marks", self._merge_yoso_marks)
        _step("yoso_skill", self._add_yoso_predictor_skill)
        _step("yoso_profile", self._add_yoso_profile_skill)
        _step("odds_signals", self._merge_odds_signals)
        _step("person_yearly", self._merge_person_yearly)
        _step("horse_results", self._merge_horse_results)
        _step("horse_info", self._merge_horse_info)
        _step("peds", self._merge_peds)

        # 旧実装にあった巨大データ(数十万行)の debug CSV ダンプは to_csv だけで数分かかる
        # ため既定で無効化（純粋なサンドボックス用）。KEIBA_DUMP_MERGE_CSV=1 でのみ書き出す。
        if _os.environ.get("KEIBA_DUMP_MERGE_CSV") == "1":
            _os.makedirs("./data/tmp/for_sandbox", exist_ok=True)
            t0 = _time.perf_counter()
            self.merged_data.to_csv("./data/tmp/for_sandbox/test_df.csv", index=True)
            logger.info("[merge] debug CSV dump: %.1fs", _time.perf_counter() - t0)

    @staticmethod
    def _to_id_str(values: pd.Series | pd.Index) -> pd.Series | pd.Index:
        """ID（horse_id/race_id）を統一フォーマットの文字列へ正規化する。

        DB 復元由来（object 文字列）と pickle 由来（Int64/float64）が混在しても
        merge キーの dtype が一致するよう、全て文字列化し float の末尾 ``.0`` を除去する。
        欠損は文字列化で "nan"/"<NA>" 等になり得るが、それらは元々ジョインしないため許容する。
        """
        as_str = values.astype(str)
        return as_str.str.replace(r"\.0$", "", regex=True)

    def _normalize_join_keys(self) -> None:
        """horse_id / race_id の merge キーをソース横断で文字列に正規化する。

        netkeiba(pickle) と DB 復元データが混在しても join できるよう、
        DataMerger が参照する全テーブルの horse_id 列・index を文字列へ揃える。
        """
        if "horse_id" in self._results.columns:
            self._results["horse_id"] = self._to_id_str(self._results["horse_id"])
        if self._horse_results.index.name == "horse_id":
            self._horse_results.index = self._to_id_str(self._horse_results.index)
        if self._horse_info.index.name == "horse_id":
            self._horse_info.index = self._to_id_str(self._horse_info.index)
        if self._peds.index.name == "horse_id":
            self._peds.index = self._to_id_str(self._peds.index)

    def _merge_race_info(self):
        # race_id インデックスの dtype 不一致（int64/float64 vs str）によるジョイン失敗を防ぐ
        self._results.index = self._results.index.astype(str).str.replace(r'\.0$', '', regex=True)
        self._race_info.index = self._race_info.index.astype(str).str.replace(r'\.0$', '', regex=True)
        self._results = self._results.merge(self._race_info, left_index=True, right_index=True, how="left")
        dict_ = dict_selector("_results")
        self._results = convert_column_types(self._results, dict_)

    def _merge_race_day_notes(self):
        """調教評価/パドック/厩舎コメントを (race_id, 馬番) で results に左結合する。

        各ソースは race_id を index に持つ raw（馬番=列）。未提供（空）はスキップ。
        horse_id 等の重複列は持ち込まず、値列のみを結合する。race_info の後・
        horse_results の前に実行し、当日属性として date ループに乗せて伝播させる。
        """
        specs = [
            (self._training, ["調教評価", "映像グレード"]),
            (self._paddock, ["パドック評価", "パドックコメント"]),
            (self._comment, ["厩舎コメント", "コメント評価"]),
        ]
        if all(df is None or df.empty for df, _ in specs):
            return
        if "馬番" not in self._results.columns:
            return

        base = self._results
        base.index = base.index.astype(str).str.replace(r"\.0$", "", regex=True)
        base.index.name = "race_id"
        left = base.reset_index()
        left["_umaban_key"] = pd.to_numeric(left["馬番"], errors="coerce").astype("Int64")

        for df, value_cols in specs:
            if df is None or df.empty:
                continue
            notes = df.reset_index()
            if "race_id" not in notes.columns:
                notes = notes.rename(columns={notes.columns[0]: "race_id"})
            notes["race_id"] = notes["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
            if "馬番" not in notes.columns:
                continue
            notes["_umaban_key"] = pd.to_numeric(notes["馬番"], errors="coerce").astype("Int64")
            cols = ["race_id", "_umaban_key"] + [c for c in value_cols if c in notes.columns]
            notes = notes[cols].drop_duplicates(["race_id", "_umaban_key"])
            left = left.merge(notes, on=["race_id", "_umaban_key"], how="left")

        self._results = left.drop(columns=["_umaban_key"]).set_index("race_id")

    def _merge_yoso_marks(self):
        """予想印（ロング）を (race_id, 馬番) のコンセンサス特徴に集約して左結合する。

        予想家の顔ぶれはレースで変動するため個別列でなく集約量（印数/◎数/スコア）を使う。
        発走前確定＝リーク無し。未提供（空）はスキップ。
        """
        if self._yoso_marks is None or self._yoso_marks.empty:
            return
        if "馬番" not in self._results.columns:
            return
        from src.preprocessing._yoso_consensus import aggregate_consensus

        long = self._yoso_marks.reset_index()
        if "race_id" not in long.columns:
            long = long.rename(columns={long.columns[0]: "race_id"})
        if "馬番" not in long.columns:
            return
        long["race_id"] = long["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
        consensus = aggregate_consensus(long)
        if consensus.empty:
            return
        consensus["race_id"] = consensus["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
        consensus["_umaban_key"] = pd.to_numeric(consensus["馬番"], errors="coerce").astype("Int64")
        value_cols = [c for c in consensus.columns if c.startswith("yoso_")]
        consensus = consensus[["race_id", "_umaban_key"] + value_cols].drop_duplicates(
            ["race_id", "_umaban_key"]
        )

        base = self._results
        base.index = base.index.astype(str).str.replace(r"\.0$", "", regex=True)
        base.index.name = "race_id"
        left = base.reset_index()
        left["_umaban_key"] = pd.to_numeric(left["馬番"], errors="coerce").astype("Int64")
        merged = left.merge(consensus, on=["race_id", "_umaban_key"], how="left")
        self._results = merged.drop(columns=["_umaban_key"]).set_index("race_id")

    def _add_yoso_predictor_skill(self):
        """予想家の as-of ◎的中率で◎を加重した特徴を追加する（自前計算・リーク無し）。

        取得済み yoso_marks（予想家の印）と results（着順）から、各予想家の「◎を付けた馬が
        1着になった率」を**当該レース日より前**で expanding 集計（自分の当該行は除外）し、
        ◎を付けた予想家の skill を馬ごとに合算/最大する。スクレイプ不要・追加取得なし。
        - ``yoso_honmei_skill_sum`` : ◎を付けた予想家の as-of 的中率の合計（質×量）
        - ``yoso_best_skill``       : 同・最大（最も当てる予想家が◎を付けたか）
        skill 不明（履歴ゼロの予想家）の寄与は NaN（best）/0（sum・skipna）。
        """
        import numpy as np  # noqa: F401 — where 経由で利用

        if self._yoso_marks is None or self._yoso_marks.empty:
            return
        if "馬番" not in self._results.columns or "着順" not in self._results.columns:
            return
        long = self._yoso_marks.reset_index()
        if "race_id" not in long.columns:
            long = long.rename(columns={long.columns[0]: "race_id"})
        if not {"馬番", "predictor_yid", "mark"}.issubset(long.columns):
            return
        long["race_id"] = long["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
        long["_umaban_key"] = pd.to_numeric(long["馬番"], errors="coerce").astype("Int64")

        base = self._results
        base.index = base.index.astype(str).str.replace(r"\.0$", "", regex=True)
        base.index.name = "race_id"
        res = base.reset_index()
        res["_umaban_key"] = pd.to_numeric(res["馬番"], errors="coerce").astype("Int64")
        res["_chaku"] = pd.to_numeric(res["着順"], errors="coerce")
        race_date = res.groupby("race_id")["date"].first() if "date" in res.columns else None

        hon = long[long["mark"] == "◎"].copy()
        if hon.empty or race_date is None:
            return
        hon = hon.merge(
            res[["race_id", "_umaban_key", "_chaku"]], on=["race_id", "_umaban_key"], how="left"
        )
        hon["_date"] = hon["race_id"].map(race_date)
        hon["_hit"] = (hon["_chaku"] == 1).astype(float)
        hon = hon.sort_values(["_date", "race_id"])
        grp = hon.groupby("predictor_yid")
        n_prior = grp.cumcount()                       # 当該行より前の◎数
        hits_prior = grp["_hit"].cumsum() - hon["_hit"]  # 当該行を除く的中数
        hon["_skill"] = hits_prior / n_prior.where(n_prior > 0)  # 履歴ゼロは NaN

        sk = hon.groupby(["race_id", "_umaban_key"]).agg(
            yoso_honmei_skill_sum=("_skill", "sum"),
            yoso_best_skill=("_skill", "max"),
        ).reset_index()

        merged = res.merge(sk, on=["race_id", "_umaban_key"], how="left")
        self._results = merged.drop(
            columns=["_umaban_key", "_chaku"], errors="ignore"
        ).set_index("race_id")

    def _add_yoso_profile_skill(self):
        """予想家プロフィール由来スキル（prior）で◎を加重した特徴を追加する。

        各予想家の profile_honmei_winrate（◎1着率の直近集計）を、◎を付けた馬ごとに合算/最大。
        方式A（自前 as-of）が直近窓のみなのに対し、こちらは予想家自身のログ由来で広くカバー
        （現時点スナップショット＝軽微リーク許容。ユーザー指定 B1）。未提供（空）はスキップ。
        """
        if self._yoso_predictor is None or self._yoso_predictor.empty:
            return
        if self._yoso_marks is None or self._yoso_marks.empty:
            return
        if "馬番" not in self._results.columns:
            return
        prior = self._yoso_predictor.reset_index()
        if "predictor_yid" not in prior.columns:
            prior = prior.rename(columns={prior.columns[0]: "predictor_yid"})
        if "profile_honmei_winrate" not in prior.columns:
            return
        prior["predictor_yid"] = prior["predictor_yid"].astype(str)
        skill = prior.set_index("predictor_yid")["profile_honmei_winrate"]

        long = self._yoso_marks.reset_index()
        if "race_id" not in long.columns:
            long = long.rename(columns={long.columns[0]: "race_id"})
        if not {"馬番", "predictor_yid", "mark"}.issubset(long.columns):
            return
        hon = long[long["mark"] == "◎"].copy()
        if hon.empty:
            return
        hon["race_id"] = hon["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
        hon["_umaban_key"] = pd.to_numeric(hon["馬番"], errors="coerce").astype("Int64")
        hon["_sk"] = hon["predictor_yid"].astype(str).map(skill)
        agg = hon.groupby(["race_id", "_umaban_key"]).agg(
            yoso_profile_skill_sum=("_sk", "sum"),
            yoso_profile_best=("_sk", "max"),
        ).reset_index()

        base = self._results
        base.index = base.index.astype(str).str.replace(r"\.0$", "", regex=True)
        base.index.name = "race_id"
        res = base.reset_index()
        res["_umaban_key"] = pd.to_numeric(res["馬番"], errors="coerce").astype("Int64")
        merged = res.merge(agg, on=["race_id", "_umaban_key"], how="left")
        self._results = merged.drop(columns=["_umaban_key"], errors="ignore").set_index("race_id")

    def _merge_odds_signals(self):
        """市場歪み特徴（複勝/三連複/三連単 overlay）を (race_id, 馬番) で左結合する。

        確定オッズ由来でリーク無し（``単勝`` と同じ前提）。run_pipeline 側で
        ``build_market_signal_frame`` により事前計算された DataFrame を受け取り、値列のみ
        左結合する。未提供（空）はスキップ。
        """
        if self._odds_signals is None or self._odds_signals.empty:
            return
        if "馬番" not in self._results.columns:
            return
        from src.preprocessing._market_signals import MARKET_SIGNAL_COLS

        sig = self._odds_signals.copy()
        if "race_id" not in sig.columns or "馬番" not in sig.columns:
            return
        sig["race_id"] = sig["race_id"].astype(str).str.replace(r"\.0$", "", regex=True)
        sig["_umaban_key"] = pd.to_numeric(sig["馬番"], errors="coerce").astype("Int64")
        value_cols = [c for c in MARKET_SIGNAL_COLS if c in sig.columns]
        sig = sig[["race_id", "_umaban_key", *value_cols]].drop_duplicates(
            ["race_id", "_umaban_key"]
        )

        base = self._results
        base.index = base.index.astype(str).str.replace(r"\.0$", "", regex=True)
        base.index.name = "race_id"
        left = base.reset_index()
        left["_umaban_key"] = pd.to_numeric(left["馬番"], errors="coerce").astype("Int64")
        merged = left.merge(sig, on=["race_id", "_umaban_key"], how="left")
        self._results = merged.drop(columns=["_umaban_key"], errors="ignore").set_index("race_id")

    # 人物年度別成績から featured に乗せる統計（前年=as-of 結合）
    _PERSON_STAT_COLS: ClassVar[tuple] = ("勝率", "複勝率", "芝勝率", "ダート勝率", "重賞勝利", "出走回数")

    def _merge_person_yearly(self):
        """騎手/調教師の『前年』年度別成績を as-of 結合する（リーク無し）。

        当該レース年 Y に対し year=Y-1（完了済みの前年）の成績を結合する。当年は集計途中
        （リーク）なので使わない。jockey_py_* / trainer_py_* を付与。未提供（空）はスキップ。
        """
        if self._person_yearly is None or self._person_yearly.empty:
            return
        if "date" not in self._results.columns:
            return
        # index を列に正規化（_pkey index は捨て、旧 entity_id index は復元）。二重移行で
        # _pkey 列が残っていても安全に除去する。
        py = self._person_yearly
        if "entity_id" in py.columns:
            py = py.reset_index(drop=True)
        else:
            py = py.reset_index()
            if "entity_id" not in py.columns:
                py = py.rename(columns={py.columns[0]: "entity_id"})
        py = py.drop(columns=["_pkey"], errors="ignore")
        if "entity_type" not in py.columns or "year" not in py.columns:
            return

        base = self._results
        base.index = base.index.astype(str).str.replace(r"\.0$", "", regex=True)
        base.index.name = "race_id"
        res = base.reset_index()
        res["_pry"] = pd.to_datetime(res["date"], errors="coerce").dt.year - 1

        from src.preprocessing._person_id import canon_person_id

        # breeder_id は results に無く horse_info にある。後段の horse_info マージと衝突しない
        # よう一時列 _breeder_tmp に horse_id 経由で引き、最後に drop する。
        if (
            "horse_id" in res.columns
            and self._horse_info is not None
            and not self._horse_info.empty
            and "breeder_id" in self._horse_info.columns
        ):
            bi = self._horse_info[["breeder_id"]].rename(columns={"breeder_id": "_breeder_tmp"}).copy()
            bi.index = bi.index.astype(str)
            res["horse_id"] = res["horse_id"].astype(str)
            res = res.merge(bi, left_on="horse_id", right_index=True, how="left")

        stat_cols = [c for c in self._PERSON_STAT_COLS if c in py.columns]
        for etype, idcol, prefix in (
            ("jockey", "jockey_id", "jockey_py"),
            ("trainer", "trainer_id", "trainer_py"),
            ("owner", "owner_id", "owner_py"),
            ("breeder", "_breeder_tmp", "breeder_py"),
        ):
            if idcol not in res.columns or not stat_cols:
                continue
            sub = py[py["entity_type"] == etype][["entity_id", "year"] + stat_cols].copy()
            if sub.empty:
                continue
            sub = sub.rename(columns={c: f"{prefix}_{c}" for c in stat_cols})
            sub = sub.rename(columns={"entity_id": idcol, "year": "_pry"})
            # 結合キーを正準化（jockey/trainer は5桁ゼロ埋め、owner/breeder は素通し）
            sub[idcol] = sub[idcol].map(lambda v, _e=etype: canon_person_id(_e, v))
            sub["_pry"] = pd.to_numeric(sub["_pry"], errors="coerce")
            res[idcol] = res[idcol].map(lambda v, _e=etype: canon_person_id(_e, v))
            res = res.merge(sub.drop_duplicates([idcol, "_pry"]), on=[idcol, "_pry"], how="left")

        self._results = res.drop(columns=["_pry", "_breeder_tmp"], errors="ignore").set_index("race_id")

    def _merge_horse_results(self):
        """日付ごとに horse_results / results をスライスしてマージする。

        全ての「date < target_date」フィルタを searchsorted に統一し、
        ループごとの全件コピーを排除してメモリを節約する。
        """
        import numpy as np

        logger.info("separating horse results by date")
        dict_ = dict_selector("_horse_results")
        self._horse_results = convert_column_types(self._horse_results, dict_)

        # peds_0=父(sire), peds_32=母父(broodmare sire)。実データ検証で母=peds_31(各馬ほぼ固有)、
        # その次の peds_32 が母父(199ユニーク・父と12%重複)と確認。存在する血統列を horse_results に
        # 付与し、過去走の産駒成績から sire/damsire 集計に使う。
        ped_cols = [c for c in ("peds_0", "peds_32") if c in self._peds.columns]
        if ped_cols:
            hr_with_sire = self._horse_results.join(self._peds[ped_cols], how="left")
        else:
            hr_with_sire = self._horse_results.copy()

        # horse_results / hr_with_sire を日付ソート済みで保持
        hr_sorted = self._horse_results.sort_values("date").reset_index()
        hr_dates = hr_sorted["date"].values
        hrs_sorted = hr_with_sire.sort_values("date").reset_index()
        hrs_dates = hrs_sorted["date"].values

        # 騎手・調教師統計を self._results に直接付与（ループ内の全件コピーを排除）。
        # race_id をキーにした merge は (race_id, trainer_id) が非一意になり得るため使わず、
        # 位置ベースで列を付与してインデックス（race_id）と整合させる。
        self._attach_jockey_trainer_stats()

        logger.info("merging horse_results")
        output_list: list = []

        for date, df_by_date in tqdm(self._results.groupby("date")):
            horse_id_list = df_by_date["horse_id"].unique()

            cut = int(np.searchsorted(hr_dates, date, side="left"))
            past_hr = (
                hr_sorted.iloc[:cut].set_index("horse_id")
                if cut > 0
                else hr_sorted.iloc[:0].set_index("horse_id")
            )
            horse_results = past_hr[past_hr.index.isin(horse_id_list)]

            cut2 = int(np.searchsorted(hrs_dates, date, side="left"))
            self._separated_hr_with_sire_dict[date] = (
                hrs_sorted.iloc[:cut2].set_index("horse_id") if cut2 > 0
                else hrs_sorted.iloc[:0].set_index("horse_id")
            )

            # df_by_date は既に jockey/trainer 統計列を含む（race_id インデックス保持）
            results = df_by_date.copy()

            for n_races in [*N_RACES_LIST, None]:
                results = self._merge_aggregates(results, horse_results, n_races)

            latest = horse_results.groupby("horse_id")["date"].max().rename("latest")
            results = results.merge(latest, left_on="horse_id", right_index=True, how="left")

            results = self._add_pace_stats(results, horse_results)
            results = self._add_growth_stats(results, horse_results)
            results = self._add_prev_race_features(results, horse_results)
            results = self._add_aptitude_stats(results, horse_results)
            results = self._add_speed_figure_stats(results, horse_results)
            results = self._add_course_condition_stats(results, horse_results)
            results = self._add_type_ground_stats(results, horse_results)
            results = self._add_race_class_stats(results, horse_results)
            results = self._add_career_stats(results, horse_results)
            results = self._add_recent_form_stats(results, horse_results)
            results = self._add_opponent_strength_stats(results, horse_results)
            results = self._add_sire_stats(results, date)
            results = self._add_damsire_stats(results, date)

            output_list.append(results)
            del self._separated_hr_with_sire_dict[date]

        # ignore_index=False で race_id インデックスを温存する
        # （下流の FeatureEngineering は race_id をインデックスから参照する）
        self._merged_data = pd.concat(output_list)

    def _attach_jockey_trainer_stats(self) -> None:
        """騎手・調教師・馬主の直近 JOCKEY_RECENT_N レース統計列を self._results に付与する。

        groupby + rolling で全レースを1回で計算する（ループ内の全件コピーを排除）。
        race_id は self._results の（非一意な）インデックスなので、列ではなく
        位置ベースで結果を書き戻し、インデックスとの不整合を防ぐ。
        shift(1) で自レースを除外し、未来情報のリークを防ぐ。

        馬主は従来「生の owner_id（ラベル符号化）」しか特徴量が無く、GBDT が ID を
        丸暗記して過学習しやすかった（重要度診断で owner_id が突出）。騎手・調教師と
        同じく平滑な勝率/平均着順を与え、汎化可能な馬主シグナルにする。
        """
        rank_col = "着順"
        n_horses_col = "n_horses"
        has_rank = rank_col in self._results.columns and n_horses_col in self._results.columns
        has_jockey = "jockey_id" in self._results.columns
        if not has_rank or not has_jockey:
            return
        has_trainer = "trainer_id" in self._results.columns
        has_owner = "owner_id" in self._results.columns

        # 位置を保持したまま計算するため reset_index（race_id は捨てて位置で戻す）
        res = self._results.reset_index(drop=True).copy()
        res["_pos"] = range(len(res))
        rank_num = pd.to_numeric(res[rank_col], errors="coerce")
        n_horses_num = pd.to_numeric(res[n_horses_col], errors="coerce")
        res["_is_win"] = (rank_num == 1).astype("float32")
        res["_rel_rank"] = (rank_num / n_horses_num).astype("float32")

        def _recent(id_col: str, win_name: str, rank_name: str) -> None:
            nonlocal res
            res = res.sort_values([id_col, "date"], kind="stable")
            res[win_name] = res.groupby(id_col)["_is_win"].transform(
                lambda x: x.shift(1).rolling(JOCKEY_RECENT_N, min_periods=1).mean()
            )
            res[rank_name] = res.groupby(id_col)["_rel_rank"].transform(
                lambda x: x.shift(1).rolling(JOCKEY_RECENT_N, min_periods=1).mean()
            )

        _recent("jockey_id", "jockey_win_rate", "jockey_avg_rank")
        if has_trainer:
            _recent("trainer_id", "trainer_win_rate", "trainer_avg_rank")
        if has_owner:
            _recent("owner_id", "owner_win_rate", "owner_avg_rank")

        # 元の行順に戻して self._results へ位置ベースで列を付与
        res = res.sort_values("_pos")
        cols = ["jockey_win_rate", "jockey_avg_rank"]
        if has_trainer:
            cols += ["trainer_win_rate", "trainer_avg_rank"]
        if has_owner:
            cols += ["owner_win_rate", "owner_avg_rank"]
        for c in cols:
            self._results[c] = res[c].to_numpy()

    # ──────────────────────────────────────────
    # §2c: Jockey / trainer aggregate features
    # ──────────────────────────────────────────

    def _add_jockey_trainer_stats(self, results: pd.DataFrame, target_date) -> pd.DataFrame:
        """直近 JOCKEY_RECENT_N レースの騎手・調教師集計特徴量を追加する。"""
        if "jockey_id" not in self._results.columns:
            return results

        past = self._results[self._results["date"] < target_date].copy()

        # Use actual finishing position for win rate and relative rank.
        rank_col = "着順"  # HRCols-equivalent in results (added back to ResultsProcessor)
        n_horses_col = "n_horses"
        has_actual_rank = rank_col in past.columns and n_horses_col in past.columns

        if has_actual_rank:
            past["_is_win"] = (past[rank_col] == 1).astype(float)
            past["_rel_rank"] = past[rank_col] / past[n_horses_col]
        else:
            # Fallback: use binary rank (top-3 indicator)
            past["_is_win"] = past[rank_col].astype(float) if rank_col in past.columns else 0.0
            past["_rel_rank"] = 1.0 - past[rank_col].astype(float) if rank_col in past.columns else 0.5

        # Jockey stats (last N races per jockey_id)
        jockey_recent = (
            past.sort_values("date", ascending=False)
            .groupby("jockey_id")
            .head(JOCKEY_RECENT_N)
        )
        jockey_stats = jockey_recent.groupby("jockey_id").agg(
            jockey_win_rate=("_is_win", "mean"),
            jockey_avg_rank=("_rel_rank", "mean"),
        )
        results = results.merge(jockey_stats, left_on="jockey_id", right_index=True, how="left")

        # Trainer stats
        if "trainer_id" not in past.columns:
            return results
        trainer_recent = (
            past.sort_values("date", ascending=False)
            .groupby("trainer_id")
            .head(JOCKEY_RECENT_N)
        )
        trainer_stats = trainer_recent.groupby("trainer_id").agg(
            trainer_win_rate=("_is_win", "mean"),
            trainer_avg_rank=("_rel_rank", "mean"),
        )
        results = results.merge(trainer_stats, left_on="trainer_id", right_index=True, how="left")

        return results

    # ──────────────────────────────────────────
    # §2d: Pace / leg-type features
    # ──────────────────────────────────────────

    def _add_pace_stats(self, results, horse_results):
        return _hf.add_pace_stats(results, horse_results)

    # ──────────────────────────────────────────
    # §2k: Growth / form-trajectory features
    # ──────────────────────────────────────────

    def _add_growth_stats(self, results, horse_results):
        return _hf.add_growth_stats(results, horse_results)

    # ──────────────────────────────────────────
    # §2m: Previous-race comparison features (Batch A)
    # ──────────────────────────────────────────

    def _add_prev_race_features(self, results, horse_results):
        return _hf.add_prev_race_features(results, horse_results)

    # ──────────────────────────────────────────
    # §2n: Aptitude features — wet track & racecourse (Batch B)
    # ──────────────────────────────────────────

    def _add_aptitude_stats(self, results, horse_results):
        return _hf.add_aptitude_stats(results, horse_results)

    # ──────────────────────────────────────────
    # §2l: Speed-figure features (Batch C)
    # ──────────────────────────────────────────

    def _add_speed_figure_stats(self, results, horse_results):
        return _hf.add_speed_figure_stats(results, horse_results)

    # ──────────────────────────────────────────
    # §2n: As-of career aggregate features（過去キャリア累計・リーク無し）
    # ──────────────────────────────────────────

    def _add_career_stats(self, results, horse_results):
        return _hf.add_career_stats(results, horse_results)

    def _add_recent_form_stats(self, results, horse_results):
        return _hf.add_recent_form_stats(results, horse_results)

    def _add_opponent_strength_stats(self, results, horse_results):
        return _hf.add_opponent_strength_stats(results, horse_results)

    # ──────────────────────────────────────────
    # §2e: Course condition aggregate features
    # ──────────────────────────────────────────

    def _add_course_condition_stats(self, results, horse_results):
        return _hf.add_course_condition_stats(results, horse_results)

    def _add_type_ground_stats(self, results, horse_results):
        return _hf.add_type_ground_stats(results, horse_results)

    def _add_race_class_stats(self, results, horse_results):
        return _hf.add_race_class_stats(results, horse_results)

    # ──────────────────────────────────────────
    # §2j: Sire aggregate features
    # ──────────────────────────────────────────

    def _add_sire_stats(self, results: pd.DataFrame, target_date) -> pd.DataFrame:
        """種牡馬（父=peds_0）産駒の集計特徴量を追加（sire_win_rate / sire_avg_rank / sire_recent_win_rate）。"""
        return self._add_pedigree_stats(results, target_date, "peds_0", "sire")

    def _add_damsire_stats(self, results: pd.DataFrame, target_date) -> pd.DataFrame:
        """母父（broodmare sire=peds_32）の産駒集計特徴量を追加（damsire_*）。

        母父は距離・ダート適性に効く競馬の重要軸。父(sire)と同じく過去走の産駒成績で集計する。
        血統表は行順フラット化のため母=peds_31・母父=peds_32（実データで検証済み。
        peds_2 は父父父であり母父ではない）。
        """
        return self._add_pedigree_stats(results, target_date, "peds_32", "damsire")

    def _add_pedigree_stats(
        self, results: pd.DataFrame, target_date, peds_col: str, prefix: str
    ) -> pd.DataFrame:
        """血統（peds_col）単位の産駒集計特徴量を追加する（父/母父で共用、リーク無し）。

        _separated_hr_with_sire_dict は _separate_by_date() 内で peds_0/peds_2 を付与した
        horse_results のサブセット（date < target_date）を保持する。
        - ``{prefix}_win_rate``        : 産駒の全期間勝率
        - ``{prefix}_avg_rank``        : 産駒の全期間平均相対着順（着順/頭数）
        - ``{prefix}_recent_win_rate`` : 直近 SIRE_RECENT_YEARS 年の産駒勝率
        """
        if target_date not in self._separated_hr_with_sire_dict:
            return results

        phs = self._separated_hr_with_sire_dict[target_date]
        rank_col = HRCols.RANK  # '着順'
        n_horses_col = HRCols.N_HORSES  # '頭数'

        if peds_col not in phs.columns or phs.empty or rank_col not in phs.columns:
            return results

        win_col, rank_name, recent_col = f"{prefix}_win_rate", f"{prefix}_avg_rank", f"{prefix}_recent_win_rate"

        phs = phs.copy()
        # category dtype の groupby 問題を避けるため血統キーは str 化する
        phs["_ped_key"] = phs[peds_col].astype(str)
        phs["_is_win"] = (phs[rank_col] == 1).astype(float)
        if n_horses_col in phs.columns:
            phs["_rel_rank"] = phs[rank_col] / phs[n_horses_col]

        agg_dict: dict = {"_is_win": "mean"}
        if "_rel_rank" in phs.columns:
            agg_dict["_rel_rank"] = "mean"

        ped_all = phs.groupby("_ped_key").agg(agg_dict)
        ped_all.columns = [win_col if c == "_is_win" else rank_name for c in ped_all.columns]

        # 直近 N 年
        cutoff = pd.Timestamp(target_date) - pd.DateOffset(years=SIRE_RECENT_YEARS)
        recent = phs[phs["date"] >= cutoff]
        if not recent.empty:
            ped_recent = recent.groupby("_ped_key")["_is_win"].mean().rename(recent_col)
            ped_all = ped_all.join(ped_recent, how="left")
        else:
            ped_all[recent_col] = float("nan")

        # 現役馬（出馬表）の血統キーを peds から引く
        if peds_col not in self._peds.columns:
            return results
        horse_ped = self._peds[[peds_col]].reset_index()
        horse_ped["_ped_key"] = horse_ped[peds_col].astype(str)

        horse_ped_indexed = horse_ped[["horse_id", "_ped_key"]].set_index("horse_id")
        # horse_id の型を揃える（results は str、peds 由来 index は DB 復元で Int64 になりうる）
        horse_ped_indexed.index = horse_ped_indexed.index.astype(str)
        results = results.copy()
        results["horse_id"] = results["horse_id"].astype(str)
        results = results.merge(horse_ped_indexed, left_on="horse_id", right_index=True, how="left")
        results = results.merge(ped_all, left_on="_ped_key", right_index=True, how="left")
        results = results.drop(columns=["_ped_key"], errors="ignore")

        return results

    # ──────────────────────────────────────────
    # Core helpers
    # ──────────────────────────────────────────

    def _merge_horse_info(self):
        self._merged_data["horse_id"] = self._merged_data["horse_id"].astype(str)
        info = self._horse_info.copy()
        info.index = info.index.astype(str)
        self._merged_data = self._merged_data.merge(
            info, left_on="horse_id", right_index=True, how="left"
        )

    def _merge_peds(self):
        self._merged_data["horse_id"] = self._merged_data["horse_id"].astype(str)
        peds = self._peds.copy()
        peds.index = peds.index.astype(str)
        self._merged_data = self._merged_data.merge(
            peds, left_on="horse_id", right_index=True, how="left"
        )

    @property
    def merged_data(self):
        return self._merged_data

    def _merge_aggregates(
        self, results: pd.DataFrame, horse_results: pd.DataFrame, n_races: int | None
    ) -> pd.DataFrame:
        """horse_id / (horse_id, group_col) の集計結果を results にマージする。

        n_races=None のとき全レース集計（suffix: _allR）、整数のとき直近 n_races 集計（suffix: _NR）。
        """
        if n_races is None:
            hr = horse_results
            suffix = "_allR"
        else:
            hr = self._filter_horse_results(horse_results, n_races)
            suffix = f"_{n_races}R"

        summarized = self._summarize(hr, self._target_cols).add_suffix(suffix)
        results = results.merge(summarized, left_on="horse_id", right_index=True, how="left")

        for group_col in self._group_cols:
            if group_col not in results.columns:
                continue
            summarized_with = self._summarize_with(hr, self._group_target_cols, group_col).add_suffix(
                f"_{group_col}{suffix}"
            )
            results = results.merge(
                summarized_with, left_on=["horse_id", group_col], right_index=True, how="left"
            )
        return results

    def _filter_horse_results(self, horse_results, n_races):
        return _hf.filter_horse_results(horse_results, n_races)

    def _summarize(self, horse_results, target_cols):
        return _hf.summarize(horse_results, target_cols)

    def _summarize_with(self, horse_results, target_cols, group_col):
        return _hf.summarize_with(horse_results, target_cols, group_col)
