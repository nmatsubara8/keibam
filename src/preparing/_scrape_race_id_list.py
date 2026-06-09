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


def _normalize_date(d: str) -> str:
    """日付文字列をハイフンなし8桁に統一する（"2020-01-01" → "20200101"）。"""
    return d.replace("-", "")[:8]


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

    from src.preparing.DataLoader import DataLoader

    url_paths = UrlPaths()
    rl = url_paths.RACE_LIST_URL
    save_path = os.path.join(DataLoader._abs(rl[4]), rl[5])

    # 既存 pkl を読み込む（部分完了分も活用する）
    existing_df: pd.DataFrame | None = None
    existing_dates: set = set()
    print(f"[DEBUG] save_path={save_path}, exists={os.path.exists(save_path)}")
    if os.path.exists(save_path):
        try:
            existing_df = pd.read_pickle(save_path)
            existing_dates = _covered_dates(existing_df)
            print(f"[DEBUG] existing_df cols={existing_df.columns.tolist()}, rows={len(existing_df)}, sample_dates={sorted(existing_dates)[:3]}")
        except Exception as e:
            logger.warning("既存 pkl の読み込みに失敗: %s", e)
            print(f"[DEBUG] pkl読み込みエラー: {e}")
            existing_df = None
            existing_dates = set()

    # ダウンロード対象の開催日を決定
    if kaisai_date_list is not None:
        date_col = kaisai_date_list.columns[-1]
        all_dates = kaisai_date_list[date_col].astype(str).tolist()
        missing_dates = [d for d in all_dates if _normalize_date(d) not in existing_dates]
        print(f"[DEBUG] date_col={date_col}, all_dates[:3]={all_dates[:3]}, missing={len(missing_dates)}/{len(all_dates)}")
    else:
        missing_dates = []

    # skip=True かつ未取得分がなければキャッシュを即返す
    if skip and not missing_dates and existing_df is not None:
        logger.info("race_id_list: 全開催日取得済みのためスキップ (%d 件)", len(existing_df))
        return _filter_by_years(existing_df, kaisai_date_list)

    if not missing_dates:
        # kaisai_date_list 未指定 or 全カバー済み → そのまま全件ダウンロード（後方互換）
        missing_dates = None

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
        return _filter_by_years(existing_df, kaisai_date_list)

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
        race_id_col = new_df.columns[-1]
        old_filtered = existing_df[~existing_df[race_id_col].isin(new_df[race_id_col])]
        merged = pd.concat([old_filtered, new_df], ignore_index=True)
        merged.to_pickle(save_path)
        new_df = merged

    # 年フィルタ適用
    new_df = _filter_by_years(new_df, kaisai_date_list)
    new_df.to_pickle(save_path)
    return new_df


def _filter_by_years(df: pd.DataFrame, kaisai_date_list) -> pd.DataFrame:
    """kaisai_date_list の年集合に含まれない race_id を除外して返す。"""
    if kaisai_date_list is None or df is None:
        return df
    valid_years = set(kaisai_date_list.iloc[:, -1].astype(str).str[:4].unique())
    race_id_col = df.columns[-1]
    mask = df[race_id_col].astype(str).str[:4].isin(valid_years)
    return df[mask].reset_index(drop=True)
