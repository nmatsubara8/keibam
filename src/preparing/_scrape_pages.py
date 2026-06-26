"""netkeiba ページのスクレイプ取得ループと HTML ユーティリティ。

modules.py（旧・神モジュール）から分割。self を取る関数は DataLoader 系クラスに
mixin される（bind は url_loader/table_creator 側）。パーサは _raw_parsers.py に分離。
"""

import logging
import os
import re
import time

import pandas as pd

from src.preparing._rate_limiter import polite_interval

logger = logging.getLogger(__name__)


_BLOCKED_BODY_RE = re.compile(r"<body[^>]*>\s*</body>", re.IGNORECASE)


def is_blocked_html(html) -> bool:
    """空/ブロックされた HTML かどうかを判定する。

    bot 検知時の空ページ（body 空・極端に短い）を「取得失敗」として扱い、
    39 バイト空ファイルの保存や古い temp の再処理を防ぐ。
    """
    if html is None:
        return True
    if isinstance(html, bytes):
        try:
            html = html.decode("utf-8", errors="ignore")
        except Exception:
            return True
    text = html.strip()
    if _BLOCKED_BODY_RE.search(text):  # body が空
        return True
    return False


def _fetch_with_retry(process_function, self, ref_id, driver, waiting_time,
                      max_retry: int = 4, backoff: float = 10.0):
    """process_function を呼び、ブロック時は指数バックオフでリトライする。

    Returns
    -------
    (return_data, blocked) : tuple
        成功時は (取得データ, False)。失敗時は (None, blocked) で
        blocked はブロック起因の失敗かどうか。
    """
    last_blocked = False
    for attempt in range(max_retry + 1):
        try:
            return process_function(self, ref_id, driver, waiting_time), False
        except Exception as e:
            blocked = "blocked" in str(e).lower()
            last_blocked = blocked
            if not blocked:
                # 通常エラー（中止レース等）はリトライせず即失敗として返す
                logger.error("Error at %s: %s", ref_id, e)
                return None, False
            if attempt < max_retry:
                wait = backoff * (2 ** attempt)  # 10, 20, 40, 80...
                logger.warning(
                    "ブロック検知 %s（%d/%d 回目）。%.0f 秒待機して再試行",
                    ref_id, attempt + 1, max_retry, wait,
                )
                time.sleep(wait)
            else:
                logger.error("ブロックでリトライ上限 %s（%d 回）", ref_id, max_retry)
    return None, last_blocked


def scrape_scheduled_race_html(self, ref_id):
    # 時刻とレースidの組みあわせからレースidだけを抽出
    # race_id_list = [element.split(",")[1] for element in time_race_id_list]

    query = ["?race_id=" + str(ref_id)]
    url = self.from_location + query[0]
    return get_soup(url)[0].read()


def _flush_batch_to_pkl(self):
    """バッチ完了後、累積 temp CSV を最終 pkl へ即時書き出す（既存データとマージ）。

    既存 pkl がある場合は新データとマージして上書き（新データ優先）。
    カーネル再起動後も中断前のデータが保持され、差分ダウンロードで再開できる。
    """
    temp_path = os.path.join(self.to_temp_location, self.temp_save_file_name)
    if not os.path.exists(temp_path):
        return
    try:
        new_df = self.csv_reader(temp_path)
        final_path = os.path.join(self.to_location, self.save_file_name)
        os.makedirs(self.to_location, exist_ok=True)
        # 既存 pkl とマージ（新データ優先）
        if os.path.exists(final_path):
            try:
                existing = pd.read_pickle(final_path)
                # 最後の列をキーとして重複除去
                key_col = new_df.columns[-1]
                if key_col in existing.columns:
                    old_only = existing[~existing[key_col].isin(new_df[key_col])]
                    new_df = pd.concat([old_only, new_df], ignore_index=True)
            except Exception:
                pass  # 既存 pkl が壊れていれば新データのみで上書き
        new_df.to_pickle(final_path)
        logger.debug("中間 pkl を書き出し（マージ済み）: %s (%d 行)", final_path, len(new_df))
    except Exception as e:
        logger.error("[FLUSH ERROR] %s: %s", self.alias, e, exc_info=True)


def storing_process(self):
    self_path = os.path.join(self.to_temp_location, self.temp_save_file_name)
    df = pd.read_csv(self_path)
    # CSV 往復で付く index 由来のゴミ列（"Unnamed: N"）を除去する。
    # これが最終列に残ると ID 列（kaisai_date / race_id）を取り違える。
    df = df.loc[:, ~df.columns.astype(str).str.match(r"^Unnamed")]
    # 最終列（ID列: race_id 等）で正規化・重複除去する。
    # race_id は大きな整数で float 表記（2.0e+11）になり得るため to_numeric→int64→str。
    # kaisai_date 等の他列は捨てずに保持する（月単位フィルタに必要）。
    id_col = df.columns[-1]
    df = df.copy()
    df[id_col] = pd.to_numeric(df[id_col], errors="coerce")
    df = df.dropna(subset=[id_col])
    df[id_col] = df[id_col].astype("int64").astype(str)
    df = df.drop_duplicates(subset=[id_col]).sort_values(id_col).reset_index(drop=True)
    self.target_data = df
    self.delete_files_tmp()
    self.save_temp_file(self.alias)

    self.transfer_temp_file()


def process_pkl_file(self, process_function):
    """
    pklファイルを受け取って、テーブルに変換する関数。
    process_functionを入れ替えながら汎用的に使う共通モジュール
    対象ファイルや処理のバッチサイズなどを読み取り、セットの上、処理する
    """
    # 前回実行の temp CSV が残っていると、今回のスクレイプが空でも古いデータを
    # 再処理してしまう（ブロック時に1年分の古い race_id が蘇る等）。実行前に消す。
    temp_path = os.path.join(self.to_temp_location, self.temp_save_file_name)
    if os.path.exists(temp_path):
        try:
            os.remove(temp_path)
            logger.debug("古い temp を削除: %s", temp_path)
        except OSError as e:
            logger.warning("temp 削除に失敗: %s (%s)", temp_path, e)

    if self.alias == "kaisai_date_list":
        # yyyy-mmの形式でfrom_とto_を指定すると、間のレース開催日一覧yyyy-mm-ddが返ってくる関数。
        # to_の月は含まないので注意。
        df = pd.DataFrame({"kaisai_data": []})
        target_all_files = pd.date_range(start=self.from_date, end=self.to_date, freq="ME").astype(str)
    else:
        df = self.load_file_pkl()
        # ID（日付/race_id 等）は最後の列に入る（load_file_pkl も iloc[:, -1] を
        # int 変換して扱う）。先頭列は "Unnamed: 0" などの連番のことがあるため
        # iloc[:, -1] を使う。
        target_all_files = df.iloc[:, -1]

        # print("target_pkl_fileはここから+''")
        logger.debug("%s", target_all_files.head())

    total_batches = (len(target_all_files) + self.batch_size - 1) // self.batch_size  # バッチ数の計算
    total_files = len(target_all_files)  # 処理対象の全データ数
    filetype = self.get_filetype()
    logger.info("filetype:%s", filetype)
    logger.info("# of input files: %s", total_files)
    logger.info("# of total_batches: %s", total_batches)
    processed_files = 0  # 処理済みのファイル数
    # print(f"start {self.alias} processing")
    # target_data_name = {}

    # tqdmインスタンスの作成（Jupyter ではグラフィカルバー、端末ではテキストバーを自動選択）
    from tqdm.auto import tqdm
    # 🌐 ネットワーク取得フェーズ（netkeiba へ HTTP アクセス。ポライトネス制御が効く）
    logger.info(
        "🌐 ネット取得フェーズ: %s を %d 件 netkeiba からダウンロードします"
        "（ポライトネス制御: 間隔 ~%.1fs + 1時間上限 %s 件）",
        self.alias, total_files,
        max(float(os.environ.get("KEIBA_SCRAPE_DELAY", "1.0")), 1.0),
        os.environ.get("KEIBA_MAX_REQUESTS_PER_HOUR", "1000"),
    )
    pbar = tqdm(total=total_files, desc=f"🌐 {self.alias} ネット取得", unit="件", leave=True)

    # スクレイパーのインスタンス化（Playwright 全面移行）。
    # process_function には従来の driver 引数位置で AbstractScraper を渡す
    # （同期パイプライン互換のため fetch_sync 境界を内部で使う）。
    from src.preparing._scraper import PlaywrightScraper
    driver = PlaywrightScraper()
    # ブラウザをループ全体で 1 度だけ起動して使い回す（毎回の起動・終了を避け高速化）。
    driver.open_sync()
    # Playwright は wait_for_selector / domcontentloaded で描画完了を判定するため
    # implicitly_wait 相当は不要。waiting_time は後方互換のためのプレースホルダ。
    waiting_time = 30

    # レート制限対策（環境変数で調整可能）:
    #   KEIBA_SCRAPE_DELAY     リクエスト間の基準待機秒（デフォルト 1.0、最低 1.0 に切上げ。
    #                          0 以下で明示無効化。実際の待機は _rate_limiter.polite_interval が
    #                          ランダム揺らぎを加えて 1〜3 秒程度にする）
    #   KEIBA_SCRAPE_JITTER_MAX 揺らぎ上限秒（デフォルト 2.0）
    #   KEIBA_MAX_REQUESTS_PER_HOUR 1 時間あたりの自主上限（デフォルト 1000、fetch 側で制御）
    #   KEIBA_SCRAPE_MAX_RETRY ブロック時のリトライ回数（デフォルト 4）
    #   KEIBA_SCRAPE_BACKOFF   バックオフ基準秒（デフォルト 10、指数増）
    #   KEIBA_SCRAPE_ABORT_AFTER 連続ブロックがこの回数に達したら中断（デフォルト 5）
    delay = float(os.environ.get("KEIBA_SCRAPE_DELAY", "1.0"))
    max_retry = int(os.environ.get("KEIBA_SCRAPE_MAX_RETRY", "4"))
    backoff = float(os.environ.get("KEIBA_SCRAPE_BACKOFF", "10"))
    abort_after = int(os.environ.get("KEIBA_SCRAPE_ABORT_AFTER", "5"))
    consecutive_blocks = 0  # 連続ブロック数（解消したら 0 に戻す）

    for batch_index in range(total_batches):
        start_index = batch_index * self.batch_size
        end_index = min((batch_index + 1) * self.batch_size, len(target_all_files))
        batch_target_all_files = target_all_files[start_index:end_index]
        # print("ref_idの確認:", batch_target_all_files[0:2])
        batch_data = []
        return_data = None  # 例外で process_function が完了しない場合への初期化
        for ref_id in batch_target_all_files:  #
            self.processing_id = ref_id
            # ブロック検知時は指数バックオフでリトライする。
            return_data, blocked = _fetch_with_retry(
                process_function, self, ref_id, driver, waiting_time,
                max_retry=max_retry, backoff=backoff,
            )
            # netkeiba 自主規制: リクエスト間隔（最低 1 秒 + ランダム揺らぎ）。
            # 単一 fetch 経路（race/horse/ped の bin 取得）は PlaywrightScraper.fetch が
            # 1 時間上限のみで間隔待機を持たないため、取得ループ側で 1 件ごとに挟む。
            # delay<=0（テスト等の明示無効化）のときは polite_interval が 0 を返す。
            interval = polite_interval(delay)
            if interval > 0:
                time.sleep(interval)
            if return_data is None:
                # 取得失敗（通常エラー or ブロックでリトライ尽き）
                if blocked:
                    consecutive_blocks += 1
                    logger.warning(
                        "ブロック継続中（連続 %d 回）。レート制限の可能性: %s",
                        consecutive_blocks, ref_id,
                    )
                    if consecutive_blocks >= abort_after:
                        driver.close_sync()
                        raise RuntimeError(
                            f"連続 {consecutive_blocks} 回ブロックされたため中断します。"
                            f"netkeiba のレート制限の可能性が高いです。"
                            f"しばらく待ってから再実行してください"
                            f"（KEIBA_SCRAPE_DELAY を増やすと緩和されます）"
                        )
                self.obtained_last_key = ref_id
                pbar.update(1)
                continue

            consecutive_blocks = 0  # 成功したらリセット
            batch_data.append(return_data)
            processed_files += 1
            pbar.update(1)  # 処理済みのファイル数を1増やす
            self.obtained_last_key = ref_id

        # print(f"直前 filetype:{filetype}")
        if filetype != "bin":
            if batch_data:
                df = pd.concat(batch_data)
                self.target_data = df
                self.save_temp_file(self.alias)
                # バッチ完了ごとに中間 pkl を書き出してカーネル再起動時の再取得を防ぐ
                _flush_batch_to_pkl(self)
        else:
            if return_data is not None:
                self.processing_id = ref_id
                self.target_data = return_data
                self.save_temp_file(self.alias)

    if filetype != "bin":
        self_path = os.path.join(self.to_temp_location, self.temp_save_file_name)
        if not os.path.exists(self_path):
            driver.close_sync()
            raise RuntimeError(
                f"temp CSV が存在しません: {self_path}\n"
                "全リクエストがブロックされたか、データが 0 件の可能性があります。"
            )
        storing_process(self)

    # ループ全体で使い回したブラウザを終了する。
    driver.close_sync()

    logger.info("# of processed files: %s", processed_files)


def get_kaisai_date_list(self, ref_id, driver, waiting_time):
    match = re.match(r"^(\d{4})-(\d{2})-\d{2}$", ref_id)
    if match:
        year = match.group(1)
        month = match.group(2)
        # print("Year:", year)
        # print("Month:", month)
    else:
        logger.warning("Invalid date format")

    # 開催日一覧を入れるリスト
    kaisai_date_list = []
    # df = pd.DataFrame(columns=["kaisai_date"])

    # 取得したdate_rangeから、スクレイピング対象urlを作成する。
    # urlは例えば、https://race.netkeiba.com/top/calendar.html?year=2022&month=7 のような構造になっている。
    # netkeiba は month=01 のようなゼロ埋めだとカレンダーを返さないため、int に正規化して
    # ゼロ埋めを外す（month=1）。
    month = str(int(month))
    ref_id = "year=" + str(year) + "&month=" + month
    ref_ym = str(year) + month.zfill(2)

    url = str(self.from_location) + "?" + ref_id
    # print(f"url:{url}")
    soup = get_soup(url, driver, waiting_time)
    # 空/ブロックページなら「0件」と誤認せず明示的に失敗させる（レート制限の検知）。
    if is_blocked_html(str(soup)):
        raise ValueError(f"blocked/empty calendar page: {url}（レート制限の可能性）")
    calendar_table = soup.find("table", class_="Calendar_Table")
    if calendar_table is None:
        logger.warning("Calendar_Table が見つかりません: %s", url)
        return pd.DataFrame({"kaisai_data": []})
    a_list = calendar_table.find_all("a")
    for a in a_list:
        found = re.findall(r"(?<=kaisai_date=)\d+", a.get("href", ""))
        if found:
            kaisai_date_list.append(found[0])
        else:
            logger.debug("kaisai_date= が見つからないリンクをスキップ: %s", a.get("href", ""))
    # print(f"kaisai_date_list:{kaisai_date_list}")
    # DataFrameを作成し、インデックスをリセットして整形する
    df = pd.DataFrame({"kaisai_data": kaisai_date_list}, index=[ref_ym] * len(kaisai_date_list))
    # print(f"df:{df}")

    return df


def _polite_delay():
    """リクエスト前に KEIBA_SCRAPE_DELAY 秒待機する（ポライトネス制御）。"""
    delay = float(os.environ.get("KEIBA_SCRAPE_DELAY", "1.0"))
    if delay > 0:
        time.sleep(delay)


def get_soup(url, driver, waiting_time=None):
    """URL の HTML を取得して BeautifulSoup を返す（Playwright 同期ブリッジ）。

    driver は AbstractScraper（PlaywrightScraper）。JS 描画ページでも
    domcontentloaded まで待って HTML を取得する。waiting_time は後方互換の未使用引数。
    """
    from bs4 import BeautifulSoup

    _polite_delay()
    html = driver.fetch_sync(url)
    soup = BeautifulSoup(html, "lxml")
    return soup


def get_raw_horse_id_list(self, ref_id, driver, waiting_time):
    # この例ではtarget=horse ref_id=race_id
    target_id_list = []
    target_ids = []
    target_ids = []

    url = str(self.from_location) + str(ref_id)
    soup = get_soup(url, driver, waiting_time)

    target_td_list = soup.find_all("td", attrs={"class": "txt_l"})

    target_ids = [a["href"] for td in target_td_list if (a := td.find("a")) and "/horse/" in a["href"]]
    target_id = [re.search(r"/horse/(\d+)/", href).group(1) for href in target_ids]
    for id in range(len(target_id)):
        target_id_list.append(target_id[id])
    # print(f"target_id_list:{target_id_list}")
    df = pd.DataFrame({"horse_id": target_id_list}, index=[ref_id] * len(target_id_list))
    # print(f"df:{df}")
    return df


def scrape_race_id_list(self, ref_id, driver, waiting_time=None):
    """開催日のレース一覧ページから race_id を抽出する（Playwright + bs4）。

    driver は AbstractScraper。RaceList_Box が JS 描画されるまで待って HTML を取得し、
    アンカーの href から race_id を正規表現で抜き出す（selenium の find_element を廃止）。
    """
    from bs4 import BeautifulSoup

    url = f"{self.from_location}?kaisai_date={ref_id}"
    # リクエストした開催日の年（race_id 先頭4桁）。ページ読込失敗時に netkeiba が
    # 現在のデフォルトレース一覧を返すことがあるため、年が一致する race_id だけ残す。
    expected_year = str(ref_id)[:4]
    race_id_list = []
    # kaisai_date を named 列として保持する（index に置くと CSV 往復で Unnamed→NaN
    # となり月単位フィルタができなくなる）。列順は [kaisai_date, race_id] とし、
    # 「ID は最終列」という規約に合わせて race_id を末尾に置く。
    df = pd.DataFrame({"kaisai_date": [], "race_id": []})
    try:
        # RaceList_Box の描画完了を待ってから HTML を取得
        _polite_delay()
        html = driver.fetch_sync(url, wait_selector=".RaceList_Box")
        # 空/ブロックページなら明示的に失敗させる（レート制限の検知）。
        if is_blocked_html(html):
            raise ValueError(f"blocked/empty race_list page: {url}（レート制限の可能性）")
        soup = BeautifulSoup(html, "lxml")
        box = soup.find(class_="RaceList_Box")
        a_list = box.find_all("a") if box is not None else []
        for a in a_list:
            href = a.get("href", "")
            race_id = re.findall(
                r"(?<=shutuba.html\?race_id=)\d+|(?<=result.html\?race_id=)\d+", href
            )
            # 年が一致しない race_id（=フォールバックで返った現在レース）は除外
            if len(race_id) > 0 and race_id[0][:4] == expected_year:
                race_id_list.append(race_id[0])
        df = pd.DataFrame(
            {"kaisai_date": [str(ref_id)] * len(race_id_list), "race_id": race_id_list}
        )
    except Exception as e:
        logger.error("Error at %s: %s", ref_id, e)
        logger.error("error / obtained_last_key: %s", self.obtained_last_key)

    return df


_SCRAPE_WAIT_SELECTORS = {
    "horse_html": "table.db_h_race_results",
}


def scrape_html(self, ref_id, driver, waiting_time):
    """from_location + ref_id の HTML を取得する（race/horse/ped 共通）。

    db.netkeiba.com は素の urlopen（UA なし）に HTTP 400 を返すため、
    PlaywrightScraper（driver）経由で取得する。alias によっては JS 描画完了を
    待つ（_SCRAPE_WAIT_SELECTORS）。戻り値は bin ファイル書き込み用に UTF-8 bytes。
    """
    from src.preparing._scraper import _looks_empty

    url = str(self.from_location) + str(ref_id)
    wait_selector = _SCRAPE_WAIT_SELECTORS.get(self.alias)
    # ナビゲーション直後の空ページ（本文なし数十バイト）を稀に掴むため、
    # 空判定時は短い待機を挟んで最大 3 回まで再取得する。
    html_str = ""
    for attempt in range(3):
        html_str = driver.fetch_sync(url, wait_selector=wait_selector)
        if not _looks_empty(html_str):
            break
        logger.warning("scrape_html: 空ページを検出 (%s, attempt %d/3)", ref_id, attempt + 1)
        time.sleep(2 * (attempt + 1))
    # 再取得しても空/ブロックページ（39バイト等）なら保存しない。ValueError を投げると
    # process_bin_file 側でスキップされ、39バイト空 bin の量産を防ぐ。
    if is_blocked_html(html_str):
        raise ValueError(f"blocked/empty response for {ref_id} (len={len(html_str or '')})")
    return html_str.encode("utf-8")


scrape_html_race = scrape_html


scrape_html_horse = scrape_html


scrape_html_ped = scrape_html


def _filter_target_bins(bin_files, only_ids):
    """処理対象の bin を only_ids（race_id / horse_id の集合）に絞り込む（純粋関数）。

    only_ids が None のときは全件返す（従来挙動）。bin ファイル名は ``<id>.bin``
    （processing_id=ref_id で保存）なので拡張子を除いた stem を id と突き合わせる。
    増分取込（新規レース・新規馬のみ）で全 HTML コーパスの再パースを避けるために使う。
    """
    if only_ids is None:
        return bin_files
    only = {str(i) for i in only_ids}
    return [f for f in bin_files if os.path.splitext(os.path.basename(f))[0] in only]


def process_bin_file(self, process_function):
    """
    binファイルを受け取って、テーブルに変換する関数。
    process_functionを入れ替えながら汎用的に使う共通モジュール
    対象ファイルや処理のバッチサイズなどを読み取り、セットの上、処理する

    self.only_ids（任意）が設定されている場合は、その id（race_id / horse_id）の
    bin だけを処理する（増分取込で全コーパス再パースを避ける）。
    """

    all_bin_files = sorted(self.get_file_list(self.from_local_location))
    target_bin_files = _filter_target_bins(all_bin_files, getattr(self, "only_ids", None))
    if getattr(self, "only_ids", None) is not None:
        logger.info(
            "増分処理: %s を %d 件に絞り込み（ディレクトリ全 %d 件中）",
            self.alias, len(target_bin_files), len(all_bin_files),
        )
    total_batches = (len(target_bin_files) + self.batch_size - 1) // self.batch_size  # バッチ数の計算
    total_files = len(target_bin_files)  # 処理対象の全データ数
    logger.info("# of input files: %s", total_files)
    processed_files = 0  # 処理済みのファイル数
    skipped_files = 0  # テーブルなし・スキップ件数

    logger.info("start %s processing", self.alias)

    # 💾 ローカル解析フェーズ（保存済み HTML を解析するだけ。netkeiba へは一切アクセスしない）
    logger.info(
        "💾 ローカル解析フェーズ: %s を %d 件のローカル HTML から作成します"
        "（ネットワーク非アクセス＝ポライトネス対象外）",
        self.alias, total_files,
    )
    # tqdmインスタンスの作成（Jupyter ではグラフィカルバー、端末ではテキストバーを自動選択）
    from tqdm.auto import tqdm
    pbar = tqdm(total=total_files, desc=f"💾 {self.alias} ローカル解析", unit="件", leave=True)

    for batch_index in range(total_batches):
        start_index = batch_index * self.batch_size
        end_index = min((batch_index + 1) * self.batch_size, len(target_bin_files))
        batch_target_bin_files = target_bin_files[start_index:end_index]

        for target_bin_file in batch_target_bin_files:  # race_html binファイル
            target_bin_file_path = os.path.join(self.from_local_location, target_bin_file)

            self.target_data = pd.DataFrame()
            try:
                self.target_data = process_function(target_bin_file_path)  # , target_data_name)
                # time.sleep(1)
            except (ValueError, IndexError):
                # テーブルなし / テーブル数不足（中止レース等）はカウントのみ
                skipped_files += 1
                pbar.update(1)
                continue
            except Exception as e:
                logger.error("Error at %s: %s", target_bin_file_path, e)
                pbar.update(1)
                continue

            processed_files += 1
            pbar.update(1)  # 処理済みのファイル数を1増やす
            # temp_df = pd.concat([temp_df[key] for key in temp_df])

            # if self.alias == "race_results_table":
            #    self.target_data = trim_function(temp_df)

            self.save_temp_file(self.alias)
            # target_data_name = {}  # バッチ処理が完了したので辞書をクリア

        self.obtained_last_key = target_bin_files[-1]

    temp_path = os.path.join(self.to_temp_location, self.temp_save_file_name)
    if processed_files > 0 and os.path.exists(temp_path):
        self.transfer_temp_file()
        self.copy_files()
    elif processed_files == 0:
        logger.warning("%s: 処理成功ファイルが 0 件のため pkl を更新しません", self.alias)

    logger.info("# of processed files: %s / %s (skipped: %s)", processed_files, total_files, skipped_files)
    if skipped_files:
        logger.warning("%s: %d 件をスキップ（テーブルなし・中止レース等）", self.alias, skipped_files)
