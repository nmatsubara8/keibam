"""レースIDリストのスクレイピング（差分ダウンロード対応）。

既存 pkl に含まれる開催日はスキップし、未取得分だけダウンロードして
既存データにマージする。カーネル再起動後も進捗が保持される。
"""

import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)


def _covered_dates(df: pd.DataFrame) -> set:
    """pkl 内の kaisai_date（インデックスまたは先頭列）を取得済み日付セットとして返す。

    race_id_list pkl の構造:
      - 先頭列 (Unnamed: 0 or 0列目) = kaisai_date (例: '20200105')
      - 末尾列 = race_id
    """
    # 先頭列がkaisai_date（CSV経由で 'Unnamed: 0' になることがある）
    first_col = df.iloc[:, 0].astype(str).str.strip()
    # kaisai_date は8桁数字のはず
    if first_col.str.match(r"^\d{8}$").any():
        return set(first_col.unique())
    # フォールバック: インデックスを使う
    return set(df.index.astype(str).str.strip().unique())


def _normalize_date(d) -> str:
    """日付文字列をハイフンなし8桁に統一する（"2020-01-01" → "20200101"）。NaN は空文字を返す。"""
    if d is None or (isinstance(d, float) and d != d):  # NaN check
        return ""
    return str(d).replace("-", "")[:8]


def scrape_race_id_list(kaisai_date_list=None, skip: bool = False):
    """開催日リストからレースIDリストを取得して返す。

    差分ダウンロード: 既存 pkl に含まれる開催日はスクレイピングをスキップし、
    未取得分だけダウンロードしてマージする。カーネル再起動後も進捗が保持される。

    Parameters
    ----------
    kaisai_date_list : DataFrame, optional
        scrape_kaisai_date() の戻り値。指定するとその開催日を対象にする。
    skip : bool
        True かつ全開催日が取得済みの場合、スクレイピングを完全省略する。
    """
    from src.constants._url_paths import UrlPaths
    from src.preparing.url_loader import KaisaiDateLoader

    from src.preparing._data_loader import DataLoader

    # list / tuple / Series で開催日が渡された場合は DataFrame に正規化する
    # （DataFrame の最終列に開催日を持たせる規約に合わせる）。
    if kaisai_date_list is not None and not isinstance(kaisai_date_list, pd.DataFrame):
        if isinstance(kaisai_date_list, pd.Series):
            kaisai_date_list = kaisai_date_list.to_frame(name="kaisai_data")
        else:
            kaisai_date_list = pd.DataFrame({"kaisai_data": list(kaisai_date_list)})

    url_paths = UrlPaths()
    rl = url_paths.RACE_LIST_URL
    save_path = os.path.join(DataLoader._abs(rl[4]), rl[5])

    # 既存 pkl を読み込む（部分完了分も活用する）
    existing_df: pd.DataFrame | None = None
    existing_dates: set = set()
    if os.path.exists(save_path):
        try:
            existing_df = pd.read_pickle(save_path)
            # skip=True の場合、pkl が存在すれば日付チェックせずそのまま返す
            if skip:
                logger.info("race_id_list: skip=True かつ pkl 存在 → キャッシュを返す (%d 件)", len(existing_df))
                return existing_df
            existing_dates = _covered_dates(existing_df)
        except Exception as e:
            logger.warning("既存 pkl の読み込みに失敗: %s", e)
            existing_df = None
            existing_dates = set()

    # ダウンロード対象の開催日を決定
    if kaisai_date_list is not None:
        date_col = kaisai_date_list.columns[-1]
        all_dates = kaisai_date_list[date_col].dropna().astype(str).tolist()
        missing_dates = [d for d in all_dates if _normalize_date(d) and _normalize_date(d) not in existing_dates]
    else:
        missing_dates = []

    # skip=True かつ未取得分がなければキャッシュを即返す
    if skip and not missing_dates and existing_df is not None:
        logger.info("race_id_list: 全開催日取得済みのためスキップ (%d 件)", len(existing_df))
        return _filter_requested(existing_df, kaisai_date_list)

    if not missing_dates:
        # kaisai_date_list 未指定 or 全カバー済み → そのまま全件ダウンロード（後方互換）
        missing_dates = None  # type: ignore[assignment]

    # 未取得分のみ対象に絞った kaisai_date_list を作成
    if missing_dates is not None and kaisai_date_list is not None:
        date_col = kaisai_date_list.columns[-1]
        to_scrape = kaisai_date_list[
            kaisai_date_list[date_col].astype(str).isin(missing_dates)
        ].reset_index(drop=True)
        logger.info(
            "race_id_list: %d 件中 %d 件は取得済み → %d 件をダウンロード",
            len(kaisai_date_list),
            len(existing_dates & set(missing_dates.__class__(missing_dates))),
            len(to_scrape),
        )
    else:
        to_scrape = kaisai_date_list

    if to_scrape is not None and len(to_scrape) == 0:
        # すべて取得済み
        return _filter_requested(existing_df, kaisai_date_list)

    # 入力 pkl として保存（ローダーが参照する）
    _abs = DataLoader._abs
    if to_scrape is not None:
        input_pkl_path = os.path.join(_abs(rl[7]), rl[8])
        os.makedirs(_abs(rl[7]), exist_ok=True)
        to_scrape.to_pickle(input_pkl_path)

    loader = KaisaiDateLoader(
        alias=rl[0],
        from_location=rl[1],
        to_temp_location=_abs(rl[2]),
        temp_save_file_name=rl[3],
        to_location=_abs(rl[4]),
        save_file_name=rl[5],
        batch_size=rl[6],
        from_local_location=_abs(rl[7]),
        from_local_file_name=rl[8],
    )
    loader.scrape_race_id_list()
    new_df = pd.read_pickle(save_path)

    # 既存データとマージ（新データ優先・重複除去）
    if existing_df is not None and not existing_df.empty:
        # kaisai_date 列の追加により race_id が最終列とは限らないため名前で解決する
        race_id_col = "race_id" if "race_id" in new_df.columns else new_df.columns[-1]
        if race_id_col in existing_df.columns:
            old_filtered = existing_df[~existing_df[race_id_col].isin(new_df[race_id_col])]
        else:
            # 旧スキーマに race_id 列が無い場合は重複除去せず温存する
            old_filtered = existing_df
        merged = pd.concat([old_filtered, new_df], ignore_index=True)
        merged.to_pickle(save_path)
        new_df = merged

    # 要求日付で絞って返す（save_path は全件のまま温存し、縮小データを書き戻さない）
    return _filter_requested(new_df, kaisai_date_list)


def _kaisai_date_column(df: pd.DataFrame):
    """kaisai_date 列名を返す（無ければ None）。

    優先順位:
      (1) 列名が 'kaisai_date' / 'kaisai_data'（旧データとの混在で 8桁日付の
          割合が下がっても確実に拾う）
      (2) 8桁日付（YYYYMMDD）を一定割合含む列（race_id は12桁なので桁数で区別）
    """
    # (1) 名前で確定
    for name in ("kaisai_date", "kaisai_data"):
        if name in df.columns:
            return name
    # (2) 内容で推定（8桁日付を1割以上含む列）
    best_col, best_ratio = None, 0.0
    for col in df.columns:
        vals = df[col].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
        ratio = vals.str.match(r"^\d{8}$").mean()
        if ratio > best_ratio:
            best_col, best_ratio = col, ratio
    return best_col if best_ratio > 0.1 else None


def _filter_requested(df: pd.DataFrame, kaisai_date_list) -> pd.DataFrame:
    """要求された kaisai_date に該当する race_id だけを返す（多段フォールバック）。

    優先順位:
      (1) kaisai_date 列（8桁日付）を内容で特定し、要求日付に完全一致する行
      (2) kaisai_date 列が無い場合のみ race_id 先頭4桁＝要求年で絞る
      (3) それでも空なら df 全体を返す（空返し事故を防ぐ）
    """
    if kaisai_date_list is None or df is None or df.empty:
        return df

    requested = set(
        kaisai_date_list.iloc[:, -1].astype(str).str.replace("-", "", regex=False).str[:8]
    )

    # (1) kaisai_date 列（8桁日付）を内容で特定して要求日付に完全一致で絞る。
    #     月単位リクエストでも当該日のレースだけが返るようにする。
    date_col = _kaisai_date_column(df)
    if date_col is not None:
        dates = df[date_col].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
        by_date = df[dates.isin(requested)]
        # 該当日付列が特定できた場合は、たとえ 0 件でもそれが正しい結果。
        # （年フォールバックで他月を混ぜない）
        return by_date.reset_index(drop=True)

    # (2) kaisai_date 列が無い → race_id 先頭4桁＝要求年で絞る
    valid_years = {d[:4] for d in requested if len(d) >= 4}
    race_id_col = df.columns[-1]
    by_year = df[df[race_id_col].astype(str).str[:4].isin(valid_years)]
    if not by_year.empty:
        return by_year.reset_index(drop=True)

    # (3) どのフィルタも空 → データはあるので全件返す（空返し事故を防ぐ）
    logger.warning(
        "race_id_list: 要求日付/年に一致する行が無いため全 %d 行を返します", len(df)
    )
    return df.reset_index(drop=True)
