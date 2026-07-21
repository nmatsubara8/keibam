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
    # Win ヘッド（target=rank_win=1着）を併せて学習し <version>__win.pickle に保存するか。
    # Place ヘッド（既定 target=rank=top3）は常に学習・保存する。連系の Harville に
    # 真の勝率を供給するために使う（Stage B）。
    train_win_head: bool = True
    # Place ヘッドを 6 分割（全国/地方 × 芝/ダート/障害）でカテゴリ別にも学習し
    # <version>__<category>.pickle に保存するか。統合 Place ヘッド（<version>.pickle）は常に学習し、
    # 該当カテゴリのモデルが無い/データ不足のレースは推論時に統合へフォールバックする。
    train_categories: bool = True
    # カテゴリ別 Place ヘッドを学習する最小レース数（未満はスキップ→統合へフォールバック）。
    min_category_races: int = 300
    # カテゴリ別 Place ヘッドを LightGBMTuner（Optuna 段階探索）で個別にハイパラ探索して学習するか。
    # 探索結果は tuning_history.json に (version, category) 単位で保存される。
    tune_categories: bool = False


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


def save_metadata(meta: dict, path: str) -> None:
    """バージョン情報・メトリクスを JSON で履歴追記する（旧版は削除しない）。

    同一 version のエントリが既にある場合は置き換える。version は日付ベース
    （YYYYMMDD_prefix）で、同日の再学習はモデル pickle を上書きするため、
    履歴も現存する実体を指すエントリ 1 件に保つ。
    """
    existing: list = []
    if os.path.exists(path):
        with open(path) as f:
            existing = json.load(f)
    existing = [m for m in existing if m.get("version") != meta.get("version")]
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

    def create(
        self, featured_data: pd.DataFrame, test_size: float, valid_size: float, target_col: str = "rank"
    ): ...

    def save(self, ai, version_name: str, suffix: str = "") -> None: ...


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
        tuning_config=None,
        base_models_config=None,
    ) -> dict:
        """featured_data で全再学習し、バージョン付きモデルを保存する。

        Parameters
        ----------
        featured_data : 全レース特徴量 DataFrame（race_id インデックス）。
        vname : バージョン名（未指定なら日付自動生成）。
        with_tuning : True なら Optuna ハイパラ探索（週次運用では False 推奨）。
            探索後は全 trial を成績順で tuning_history.json に保存する。
        lgb_params : 指定時はこのパラメータを LightGBM に注入して学習する
            （保存済みチューニング履歴から選んだもの。with_tuning とは排他）。
        params_rank : lgb_params の出所 rank（メタデータ記録用）。
        tuning_config : TuningConfig（探索範囲・回数の制御）。None なら LightGBMTuner。

        Returns
        -------
        dict : version, trained_at, n_races, use_stacking, auc_test。
        """
        vname = vname or version_name()
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
            ai.train_with_stacking(
                meta_ratio=self._cfg.meta_ratio,
                with_tuning=with_tuning,
                tuning_config=tuning_config,
                base_models_config=base_models_config,
            )
        else:
            if with_tuning:
                ai.train_with_tuning(tuning_config=tuning_config)
            else:
                ai.train_without_tuning()

        # Optuna 探索を行った場合は全 trial を成績順で保存（ユーザーが後から選択できる）
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

        # tune_per_model（lightgbm/xgboost/catboost/nn の探索）を行った場合、4 モデル全部の best を
        # 反映した完成 config を JSON 保存する（§5⑤ の「書き戻して固定運用へ」を機械化）。
        # 歴代 best を守るため、既存ファイルより auc_test が良い時だけ上書きする（初回は無条件保存）。
        # 保存 JSON は from_dict が未知キー（_auc_test/_version）を無視するため、そのまま
        # --base-models-config で読み戻せる。
        # 探索の前後サマリ（末尾に表示するため情報を退避する）。tune_per_model 実行時のみ。
        search_summary: dict | None = None
        tuned_cfg = getattr(ai, "tuned_base_models_config", None)
        if tuned_cfg is not None:
            try:
                out_path = os.path.join(self._cfg.models_dir, "tuned_base_models.json")
                os.makedirs(self._cfg.models_dir, exist_ok=True)
                new_auc = float(metrics.get("auc_test", float("nan")))
                prev_auc = float("-inf")
                if os.path.exists(out_path):
                    with open(out_path, encoding="utf-8") as f:
                        prev_auc = float(json.load(f).get("_auc_test", float("-inf")))
                first_time = not os.path.exists(out_path)
                adopted = first_time or new_auc >= prev_auc
                if adopted:
                    payload = {**tuned_cfg.to_dict(), "_auc_test": new_auc, "_version": vname}
                    with open(out_path, "w", encoding="utf-8") as f:
                        json.dump(payload, f, ensure_ascii=False, indent=2)
                    logger.info(
                        "[retrain] tuned_base_models.json 更新: auc_test=%.4f（前回 best=%.4f）→ %s",
                        new_auc, prev_auc, out_path,
                    )
                else:
                    logger.info(
                        "[retrain] 今回 auc_test=%.4f ≤ 既存 best=%.4f → tuned_base_models.json 据え置き",
                        new_auc, prev_auc,
                    )
                search_summary = {
                    "prev": None if first_time else prev_auc,
                    "new": new_auc,
                    "adopted": adopted,
                    "path": out_path,
                }
            except Exception as e:  # noqa: BLE001 — 保存失敗で学習結果を失わない
                logger.warning("[retrain] tuned_base_models.json 保存失敗 (non-fatal): %s", e)

        _fd = featured_data.gbdt if hasattr(featured_data, "gbdt") else featured_data
        meta: dict[str, Any] = {
            "version": vname,
            "trained_at": dt.datetime.now().isoformat(),
            "n_races": int(len(_fd.index.unique())),
            "use_stacking": self._cfg.use_stacking,
            "base_models": list(ai.base_model_names_) if hasattr(ai, "base_model_names_") else ["LightGBM"],
            **metrics,
        }
        if lgb_params:
            meta["params_rank"] = params_rank
            meta["lgb_params"] = lgb_params

        self._factory.save(ai, vname)

        # Win ヘッド（target=rank_win=1着）を併せて学習・保存（Stage B）。
        # Place ヘッド（top3）の保存後に行い、失敗しても本体 retrain は壊さない。
        # Win ヘッドは Optuna を再実行しない設計なので、--with-tuning 時は Place 側で探索した
        # best params を流用する（無いと Win ヘッドだけ既定パラメータのまま＝評価/Benter 合成が
        # 使う Win 勝率にチューニング効果が全く反映されない）。明示 lgb_params 優先。
        win_lgb_params = lgb_params
        if not win_lgb_params and with_tuning and study is not None:
            try:
                win_lgb_params = dict(study.best_params)
                logger.info("[retrain] Win ヘッドに Place 探索の best params を流用します")
            except Exception as e:  # noqa: BLE001 — 取得失敗時は既定にフォールバック
                logger.warning("[retrain] best params 取得失敗、Win ヘッドは既定で学習: %s", e)
        if self._cfg.train_win_head:
            win_metrics = self._train_and_save_win_head(
                featured_data, vname, win_lgb_params, base_models_config
            )
            if win_metrics is not None:
                meta["win_head"] = win_metrics

        # 6 分割（全国/地方 × 芝/ダート/障害）のカテゴリ別 Place ヘッド（②(a) Place のみ）。
        # 統合 Place ヘッドの後に学習し、失敗しても本体 retrain は壊さない。
        # --tune-categories 指定時は各カテゴリを LightGBMTuner で個別探索して学習する。
        if self._cfg.train_categories:
            category_heads = self._train_category_place_heads(
                featured_data,
                vname,
                with_tuning=self._cfg.tune_categories,
                tuning_config=tuning_config,
                base_models_config=base_models_config,
            )
            if category_heads:
                meta["categories"] = list(category_heads)
                meta["category_heads"] = category_heads

        save_metadata(meta, metadata_path(self._cfg.models_dir))

        # 末尾サマリ: 大量の学習ログの後でも探索の前後が一目で分かるよう最後にまとめて出す。
        self._log_final_summary(vname, metrics, meta.get("win_head"), search_summary)
        return meta

    @staticmethod
    def _log_final_summary(vname, metrics, win_head, search_summary) -> None:
        """実行末尾に「モデル探索の前後」を一目で分かる形で出力する。

        search_summary（tune_per_model 時のみ非 None）は prev（前回 best auc_test・初回は None）、
        new（今回 auc_test）、adopted（採用=更新したか）、path を持つ。
        """
        place_auc = metrics.get("auc_test")
        lines = ["===== モデル探索の前後（サマリ）====="]
        if search_summary is not None:
            prev = search_summary["prev"]
            new = search_summary["new"]
            if prev is None:
                lines.append(f"Place(stacking) auc_test: 初回 → {new:.4f}")
            else:
                delta = new - prev
                verdict = "更新" if search_summary["adopted"] else "据え置き"
                lines.append(
                    f"Place(stacking) auc_test: 前回best {prev:.4f} → 今回 {new:.4f} "
                    f"（{delta:+.4f}・{verdict}）"
                )
            best_kept = new if search_summary["adopted"] else prev
            if best_kept is not None:
                lines.append(f"保持中の best: {best_kept:.4f} → {search_summary['path']}")
        else:
            # tune_per_model 以外（既定/LightGBM 探索のみ）は今回値のみ
            lines.append(f"Place(stacking) auc_test: {place_auc}")
        if win_head and win_head.get("auc_test") is not None:
            lines.append(f"Win head auc_test: {win_head['auc_test']:.4f}")
        lines.append(f"version={vname}")
        lines.append("====================================")
        logger.info("\n%s", "\n".join(lines))

    def _train_and_save_win_head(
        self, featured_data, vname, lgb_params, base_models_config
    ) -> dict | None:
        """Win ヘッド（1着予測）を学習し <version>__win.pickle に保存する。

        連系（単勝/馬連/馬単/三連複/三連単）の Harville に真の勝率を供給するためのモデル。
        Place ヘッドと同じ featured_data・学習経路を target=rank_win で再利用する
        （Optuna 再探索はしない＝with_tuning=False）。失敗は non-fatal でスキップ。
        """
        try:
            win_ai = self._factory.create(
                featured_data,
                test_size=self._cfg.test_size,
                valid_size=self._cfg.valid_size,
                target_col="rank_win",
            )
        except TypeError:
            logger.warning("[retrain] factory が target_col 非対応のため Win ヘッドをスキップ")
            return None
        try:
            if lgb_params and hasattr(win_ai, "set_lgb_params"):
                win_ai.set_lgb_params(lgb_params)
            if self._cfg.use_stacking:
                win_ai.train_with_stacking(
                    meta_ratio=self._cfg.meta_ratio,
                    with_tuning=False,
                    base_models_config=base_models_config,
                )
            else:
                win_ai.train_without_tuning()
            win_metrics = evaluate_test(
                win_ai.effective_model, win_ai.datasets.X_test, win_ai.datasets.y_test
            )
            self._factory.save(win_ai, vname, suffix="__win")
            logger.info(
                "[retrain] Win ヘッド保存: %s__win auc_test=%s", vname, win_metrics["auc_test"]
            )
            return win_metrics
        except Exception as e:  # noqa: BLE001 — Win ヘッド失敗で Place 本体を失わない
            logger.warning("[retrain] Win ヘッド学習に失敗（スキップ）: %s", e)
            return None

    @staticmethod
    def _strip_nn(base_models_config):
        """カテゴリ別学習用に base_models_config から NN を外す。

        カテゴリ分割は gbdt DataFrame 上で行うため NN ストリーム（PreparedFeatures）が
        無く、NN base はそのままでは学習できない。NN を外し（全部外れたら lightgbm 単独に）
        GBDT 系だけでカテゴリ別 Place ヘッドを学習する。
        """
        if base_models_config is None:
            return None
        models = getattr(base_models_config, "models", None)
        if not models or "nn" not in models:
            return base_models_config
        import dataclasses

        kept = tuple(m for m in models if m != "nn") or ("lightgbm",)
        return dataclasses.replace(base_models_config, models=kept)

    def _train_category_place_heads(
        self, featured_data, vname, *, with_tuning, tuning_config, base_models_config
    ) -> dict[str, dict]:
        """6 分割の Place ヘッドをカテゴリ別に学習・保存する（データ十分なカテゴリのみ）。

        統合 Place ヘッドと同じ featured_data を、全国/地方 × 芝/ダート/障害 で分割して
        カテゴリごとに学習し <version>__<category>.pickle に保存する。min_category_races 未満の
        カテゴリはスキップ（推論時は統合へフォールバック）。個々の失敗は non-fatal。

        メモリ対策: 6 分割の部分 DataFrame を一度に全部作らず、行ごとのカテゴリラベルだけを
        先に計算し、1 カテゴリずつ切り出して学習→即解放する（全データ 177万行でも部分 DataFrame は
        同時に 1 つしか生存しない）。
        """
        import gc

        from src.constants._model_category import ALL_CATEGORIES
        from src.training._category_split import category_series

        fd = featured_data.gbdt if hasattr(featured_data, "gbdt") else featured_data
        try:
            cat_labels = category_series(fd).to_numpy()  # 行ごとのカテゴリ slug（None 含む）
        except Exception as e:  # noqa: BLE001 — 分類失敗で本体を壊さない
            logger.warning("[retrain] カテゴリ分類に失敗（カテゴリ別学習をスキップ）: %s", e)
            return {}

        cat_cfg = self._strip_nn(base_models_config)
        metas: dict[str, dict] = {}
        for cat in ALL_CATEGORIES:
            mask = cat_labels == cat
            n_races = int(fd.index[mask].nunique()) if mask.any() else 0
            if n_races < self._cfg.min_category_races:
                logger.info(
                    "[retrain] category=%s スキップ（races=%d < min=%d）",
                    cat, n_races, self._cfg.min_category_races,
                )
                continue
            sub = fd[mask]  # このカテゴリの部分 DataFrame（1 つずつのみ生存）
            try:
                metas[cat] = self._train_and_save_category_head(
                    sub, vname, cat,
                    with_tuning=with_tuning,
                    tuning_config=tuning_config,
                    base_models_config=cat_cfg,
                )
            except Exception as e:  # noqa: BLE001 — 1 カテゴリの失敗で全体を止めない
                logger.warning(
                    "[retrain] category=%s 学習失敗（統合モデルへフォールバック）: %s", cat, e
                )
            finally:
                del sub
                gc.collect()  # 次カテゴリの前に部分 DataFrame と DataSplitter を解放
        return metas

    def _train_and_save_category_head(
        self, sub_featured, vname, category, *, with_tuning, tuning_config, base_models_config
    ) -> dict:
        """1 カテゴリの Place ヘッドを学習し <version>__<category>.pickle に保存する。

        with_tuning=True（--tune-categories）のときは LightGBMTuner でこのカテゴリ専用に
        ハイパラ探索し、その全 trial を tuning_history.json に category タグ付きで保存する。
        """
        ai = self._factory.create(
            sub_featured,
            test_size=self._cfg.test_size,
            valid_size=self._cfg.valid_size,
            target_col="rank",
        )
        if self._cfg.use_stacking:
            ai.train_with_stacking(
                meta_ratio=self._cfg.meta_ratio,
                with_tuning=with_tuning,
                tuning_config=tuning_config,
                base_models_config=base_models_config,
            )
        else:
            if with_tuning:
                ai.train_with_tuning(tuning_config=tuning_config)
            else:
                ai.train_without_tuning()

        study = getattr(ai, "tuning_study_", None)
        if with_tuning and study is not None:
            try:
                from src.training._tuning_history import save_tuning_history
                from src.training._tuning_history import trials_to_records
                from src.training._tuning_history import tuning_history_path

                records = trials_to_records(study, vname, category=category)
                save_tuning_history(records, tuning_history_path(self._cfg.models_dir))
            except Exception as e:  # noqa: BLE001 — 履歴保存失敗で学習結果を失わない
                logger.warning("[retrain] tuning_history 保存失敗 (non-fatal, %s): %s", category, e)

        metrics = evaluate_test(ai.effective_model, ai.datasets.X_test, ai.datasets.y_test)
        self._factory.save(ai, vname, suffix=f"__{category}")
        logger.info(
            "[retrain] category=%s Place ヘッド保存: %s__%s auc_test=%s（races=%d）",
            category, vname, category, metrics["auc_test"], int(len(sub_featured.index.unique())),
        )
        return {"category": category, "n_races": int(len(sub_featured.index.unique())), **metrics}
