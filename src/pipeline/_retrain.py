"""週次再学習ジョブ（拡張窓 walk-forward 全再学習）。

終了レースが日次取込で蓄積されるたびに、全データを使って Model を再学習し
バージョン付きで保存する。旧版は保持されロールバック可能。

副作用の隔離:
- 純粋ロジック (version_name / evaluate_test / save/load_metadata) は
  sklearn のみ依存で単体テスト可能（optuna / selenium 不要）。
- I/O (featured_data の pickle 読み書き / KeibaAIFactory.save) は
  DI で差し込む factory / 標準的な pickle 操作に閉じる。

レイヤ: pipeline（training より上位）。
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import logging
import os
from typing import Any, Protocol

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

from src.constants._local_paths import LocalPaths
from src.constants._model_category import ALL_CATEGORIES
from src.constants._model_category import COMBINED

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 設定 DTO（frozen）
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class RetrainConfig:
    models_dir: str = "models"
    featured_data_path: str = LocalPaths.FEATURED_DATA_PATH
    test_size: float = 0.2
    valid_size: float = 0.2
    meta_ratio: float = 0.3
    use_stacking: bool = True
    # 6 分割（全国/地方 × 芝/ダート/障害）のカテゴリ別モデルも学習するか。
    train_categories: bool = True
    # カテゴリ別モデルを学習する最小レース数。これ未満のカテゴリは学習をスキップし
    # 推論時は統合モデルへフォールバックする（障害など少数カテゴリの過学習・分割失敗を防ぐ）。
    min_category_races: int = 300


# ---------------------------------------------------------------------------
# 純粋ロジック
# ---------------------------------------------------------------------------


def version_name(prefix: str = "keibam", today: dt.date | None = None) -> str:
    """YYYYMMDD_prefix 形式のバージョン名を生成する（純粋関数）。"""
    d = today or dt.date.today()
    return f"{d.strftime('%Y%m%d')}_{prefix}"


def evaluate_test(model, X_test: pd.DataFrame, y_test: pd.Series) -> dict:
    """テストセットの AUC を算出する（純粋関数）。

    X_test は TANSHO_ODDS を含んでよい（ここで除去する）。
    """
    from src.constants._results_cols import ResultsCols

    X = X_test.drop([ResultsCols.TANSHO_ODDS], axis=1, errors="ignore")
    proba = np.asarray(model.predict_proba(X))[:, 1]
    auc = float(roc_auc_score(np.asarray(y_test), proba))
    return {"auc_test": round(auc, 4)}


def _meta_key(m: dict) -> tuple:
    """履歴エントリの一意キー（version, category）。

    category 未設定の旧エントリは "combined"（統合モデル）とみなす。これにより
    6 分割のカテゴリ別モデルが同一 version 内で共存でき、同日再学習では
    (version, category) 単位で置き換えられる。
    """
    return (m.get("version"), m.get("category") or "combined")


def save_metadata(meta: dict, path: str) -> None:
    """バージョン情報・メトリクスを JSON で履歴追記する（旧版は削除しない）。

    同一 (version, category) のエントリが既にある場合は置き換える。version は
    日付ベース（YYYYMMDD_prefix）で、同日の再学習はモデル pickle を上書きするため、
    履歴も現存する実体を指すエントリ 1 件に保つ。
    """
    existing: list = []
    if os.path.exists(path):
        with open(path) as f:
            existing = json.load(f)
    existing = [m for m in existing if _meta_key(m) != _meta_key(meta)]
    existing.append(meta)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)


def load_metadata(path: str) -> list:
    """バージョン履歴 JSON を読み込む（無ければ空リスト）。"""
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)


def metadata_path(models_dir: str) -> str:
    return os.path.join(models_dir, "version_history.json")


# ---------------------------------------------------------------------------
# DI 境界（プロトコル）
# ---------------------------------------------------------------------------


class AIFactory(Protocol):
    """KeibaAIFactory の抽象（テスト時はスタブを注入）。"""

    def create(self, featured_data: pd.DataFrame, test_size: float, valid_size: float): ...

    def save(
        self, ai, version_name: str, category: str | None = ..., models_dir: str = ...
    ) -> Any: ...


# ---------------------------------------------------------------------------
# ジョブ
# ---------------------------------------------------------------------------


class RetrainJob:
    """週次再学習ジョブ（DI で factory を受け取る）。

    拡張窓 walk-forward:
    - 日次取込で蓄積された featured_data を毎週全量で再学習する。
    - test は最新期間の固定比率スライス（DataSplitter の __split_by_date に委譲）。
    - 旧モデルは削除せず version_history.json に残してロールバックを可能にする。
    """

    def __init__(self, factory: AIFactory, config: RetrainConfig | None = None) -> None:
        self._factory = factory
        self._cfg = config or RetrainConfig()

    def run(
        self,
        featured_data: pd.DataFrame,
        vname: str | None = None,
        with_tuning: bool = False,
        lgb_params: dict | None = None,
        params_rank: int | None = None,
    ) -> dict:
        """featured_data で全再学習し、バージョン付きモデルを保存する。

        統合（全レース）モデルを必ず 1 個学習・保存し、`train_categories=True` の
        場合はさらに 6 分割（全国/地方 × 芝/ダート/障害）のカテゴリ別モデルを、
        レース数が `min_category_races` 以上のカテゴリについて学習・保存する。
        データが無い/少ないカテゴリはスキップし、推論時は統合モデルへフォールバックする。

        Parameters
        ----------
        featured_data : 全レース特徴量 DataFrame（race_id インデックス）。
        vname : バージョン名（未指定なら日付自動生成）。
        with_tuning : True なら Optuna ハイパラ探索（統合モデルのみに適用）。
            探索後は全 trial を成績順で tuning_history.json に保存する。
        lgb_params : 指定時はこのパラメータを LightGBM に注入して学習する
            （統合モデルのみに適用。with_tuning とは排他）。
        params_rank : lgb_params の出所 rank（メタデータ記録用）。

        Returns
        -------
        dict : 統合モデルのメタ（version, trained_at, n_races, use_stacking, auc_test,
            category="combined", name）に、学習できたカテゴリ slug のリストを
            "categories" キーで加えたもの。
        """
        vname = vname or version_name()

        # 1) 統合（全レース）モデル — 従来どおり。tuning / lgb_params はここだけに適用。
        combined_meta = self._train_one(
            featured_data,
            vname,
            category=COMBINED,
            with_tuning=with_tuning,
            lgb_params=lgb_params,
            params_rank=params_rank,
        )

        # 2) カテゴリ別モデル（データが十分にあるものだけ）
        category_metas: dict[str, dict] = {}
        if self._cfg.train_categories:
            from src.training._category_split import split_featured_by_category

            groups = split_featured_by_category(featured_data)
            for cat in ALL_CATEGORIES:
                sub = groups.get(cat)
                n_races = int(sub.index.nunique()) if sub is not None else 0
                if n_races < self._cfg.min_category_races:
                    logger.info(
                        "[retrain] category=%s スキップ（races=%d < min=%d）",
                        cat, n_races, self._cfg.min_category_races,
                    )
                    continue
                try:
                    category_metas[cat] = self._train_one(sub, vname, category=cat)
                except Exception as e:  # noqa: BLE001 — 1 カテゴリの失敗で全体を止めない
                    logger.warning(
                        "[retrain] category=%s 学習失敗（統合モデルへフォールバック）: %s", cat, e
                    )

        # 3) メタデータ書き込み: カテゴリ → 統合 の順（統合を最後にして履歴先頭に来させる）
        for cat in ALL_CATEGORIES:
            if cat in category_metas:
                save_metadata(category_metas[cat], metadata_path(self._cfg.models_dir))
        save_metadata(combined_meta, metadata_path(self._cfg.models_dir))

        logger.info(
            "[retrain] version=%s auc_test=%s categories=%s",
            vname, combined_meta["auc_test"], list(category_metas),
        )
        combined_meta["categories"] = list(category_metas)
        return combined_meta

    def _train_one(
        self,
        featured_data: pd.DataFrame,
        vname: str,
        *,
        category: str,
        with_tuning: bool = False,
        lgb_params: dict | None = None,
        params_rank: int | None = None,
    ) -> dict:
        """1 つの featured_data で 1 モデルを学習・保存し、メタを返す（保存はまだ）。

        統合モデルは category="combined"（サフィックス無し）、カテゴリ別モデルは
        category=slug（`__slug` サフィックス付き）で保存される。メタの save_metadata は
        呼び出し側（run）が順序を制御して行う。
        """
        ai = self._factory.create(
            featured_data,
            test_size=self._cfg.test_size,
            valid_size=self._cfg.valid_size,
        )
        if lgb_params:
            with_tuning = False
            if hasattr(ai, "set_lgb_params"):
                ai.set_lgb_params(lgb_params)

        if self._cfg.use_stacking:
            ai.train_with_stacking(meta_ratio=self._cfg.meta_ratio, with_tuning=with_tuning)
        else:
            if with_tuning:
                ai.train_with_tuning()
            else:
                ai.train_without_tuning()

        # Optuna 探索を行った場合は全 trial を成績順で保存（統合モデルのみ）
        study = getattr(ai, "tuning_study_", None)
        if with_tuning and study is not None:
            try:
                from src.training._tuning_history import save_tuning_history
                from src.training._tuning_history import trials_to_records
                from src.training._tuning_history import tuning_history_path

                records = trials_to_records(study, vname)
                save_tuning_history(records, tuning_history_path(self._cfg.models_dir))
            except Exception as e:  # noqa: BLE001 — 履歴保存失敗で学習結果を失わない
                logger.warning("[retrain] tuning_history 保存失敗 (non-fatal): %s", e)

        metrics = evaluate_test(ai.effective_model, ai.datasets.X_test, ai.datasets.y_test)
        suffix = "" if category == COMBINED else f"__{category}"
        meta: dict[str, Any] = {
            "version": vname,
            "name": f"{vname}{suffix}",
            "category": category,
            "trained_at": dt.datetime.now().isoformat(),
            "n_races": int(len(featured_data.index.unique())),
            "use_stacking": self._cfg.use_stacking,
            **metrics,
        }
        if lgb_params:
            meta["params_rank"] = params_rank
            meta["lgb_params"] = lgb_params

        self._factory.save(ai, vname, category=category, models_dir=self._cfg.models_dir)
        return meta
