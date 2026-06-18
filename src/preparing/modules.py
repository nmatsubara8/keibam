import io
import logging
import os
import re
import time

import pandas as pd

from src.constants._master import Master
from src.preparing._rate_limiter import polite_interval


def _re_first_int(text: str, default: str = "0") -> str:
    """text から最初の連続数字を返す。見つからなければ default を返す。

    re.search(...).group() が None.group() でクラッシュするのを防ぐ
    （古い/特殊なレース名で数字が無いケースに対応）。
    """
    m = re.search(r"\d+", text or "")
    return m.group() if m else default


# netkeiba が bot 検知・レート制限時に返す空ページ
# （例: "<html><head></head><body></body></html>" ≒ 39 bytes）を判定する。
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


NaN = float("nan")

logger = logging.getLogger(__name__)


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


################################# Done ####################################
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
    pbar = tqdm(total=total_files, desc=f"{self.alias} 取得", unit="件", leave=True)

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


################################# Done ####################################
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


################################# Done ####################################
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


################################# Done ####################################
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


################################# Done ####################################
# alias ごとの JS 描画完了セレクタ。db.netkeiba.com の馬ページは戦績テーブル
# （table.db_h_race_results）を JS で描画するため、domcontentloaded 直後の HTML には
# 含まれない。セレクタ未出現（新馬等で戦績なし）の場合は selector_timeout 後に
# 取得済み内容をそのまま返す（PlaywrightScraper.fetch の仕様）。
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


# race/horse/ped は同一実装。後方互換のため import 可能な別名として公開する
# （url_loader.py が名前で import し process_function として渡すため）。
scrape_html_race = scrape_html
scrape_html_horse = scrape_html
scrape_html_ped = scrape_html


################################# Done ####################################
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

    # tqdmインスタンスの作成（Jupyter ではグラフィカルバー、端末ではテキストバーを自動選択）
    from tqdm.auto import tqdm
    pbar = tqdm(total=total_files, desc=f"{self.alias} 取得", unit="件", leave=True)

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


################################# Done ####################################
def trim_function(df):
    """
    process_bin_file()のヘルパー関数

    process_functionとして呼び出したエイリアスによって異なる後処理を定義している
    """
    # 列名に半角スペースがあれば除去する
    trimmed_df = df.columns = df.columns.str.replace(r"\s+", "")
    return trimmed_df


################################# Done ####################################
def create_raw_race_results(target_bin_file_path):
    from bs4 import BeautifulSoup
    race_results = {}
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()

        # メインとなるレース結果テーブルデータを取得。
        # 旧年代（〜1990s）のページは空テーブルを含み、全テーブル一括の read_html が
        # IndexError で落ちるため、summary 属性で対象テーブルだけを解析する
        # （summary="レース結果" は 1986〜現在まで全年代で共通）。
        df = pd.read_html(io.BytesIO(html), attrs={"summary": "レース結果"})[0]

        # htmlをsoupオブジェクトに変換
        soup = BeautifulSoup(html, "lxml")

        # 馬・騎手・調教師・馬主の ID をスクレイピング。
        # 旧年代（1970s 等）はリンクの無い行（未登録馬・データ未整備）があり、
        # テーブル一括の find_all では行数と ID 数がずれるため、行単位で対応付ける
        # （リンクが無いセルは None）。また旧 horse_id は "1972z00735" のような
        # 英字混じりがあるため \d+ ではなく URL パスから \w+ で抽出する。
        result_table = soup.find("table", attrs={"summary": "レース結果"})
        body_rows = [tr for tr in result_table.find_all("tr") if tr.find("td") is not None]
        if len(body_rows) != len(df):
            raise ValueError(
                f"レース結果テーブルの行数不一致 rows={len(body_rows)} df={len(df)}: {target_bin_file_path}"
            )

        def _row_id(tr, href_pattern, id_regex):
            a = tr.find("a", attrs={"href": re.compile(href_pattern)})
            if a is None:
                return None
            m = re.search(id_regex, a["href"])
            return m.group(1) if m else None

        df["horse_id"] = [_row_id(tr, "^/horse", r"/horse/(\w+)") for tr in body_rows]
        df["jockey_id"] = [_row_id(tr, "^/jockey", r"jockey/result/recent/(\w+)") for tr in body_rows]
        df["trainer_id"] = [_row_id(tr, "^/trainer", r"trainer/result/recent/(\w+)") for tr in body_rows]
        df["owner_id"] = [_row_id(tr, "^/owner", r"owner/result/recent/(\w+)") for tr in body_rows]

        race_id = re.findall(r"\d+", os.path.basename(target_bin_file_path))[0]
        df["race_id"] = race_id
        df["race_id"].astype(int)

        # インデックスをrace_idにする
        df.index = [race_id] * len(df)
        # df.set_index("race_id", inplace=True)  # この行を削除

        race_results[race_id] = df
        # 各レースの結果データフレームを結合して race_results_df を生成
        race_results_df = pd.concat([race_results[key] for key in race_results])
        race_results_df.set_index("race_id", inplace=True)
        race_results_df = race_results_df.rename(columns=lambda x: x.replace(" ", ""))

    return race_results_df


# パターンにマッチする部分を抽出する関数を定義
def convert_string(value):
    # 旧年代（〜1990s）の馬戦績は 開催 列が欠損（NaN=float）のことがあるため、
    # 文字列以外はそのまま返す（地方・海外遠征のない時代の欠損値）。
    if not isinstance(value, str):
        return value
    # 正規表現パターンを定義
    pattern = r"\d{0,2}([^\d]+)\d{0,2}"
    match = re.search(pattern, value)
    if match:
        return match.group(1)  # マッチした部分の文字列を返す
    else:
        return value  # マッチしなかった場合は元の値を返す


################################# Done ####################################
def create_raw_horse_results(target_bin_file_path):
    horse_result = {}
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()

        html_str = html.decode("utf-8", errors="replace")
        df = pd.read_html(io.StringIO(html_str))[3]
        # 受賞歴がある馬の場合、3番目に受賞歴テーブルが来るため、4番目のデータを取得する
        if df.columns[0] == "受賞歴":
            df = pd.read_html(io.StringIO(html_str))[4]
            # print(f"test df:{df.iloc[:,1]}")

        # 新馬の競走馬レビューが付いた場合、
        # 列名に0が付与されるため、次のhtmlへ飛ばす
        if df.columns[0] == 0:
            logger.warning("horse_results empty case1 %s", target_bin_file_path)
            # continue

        # インデックスをhorse_idにする
        horse_id = re.findall(r"\d+", os.path.basename(target_bin_file_path))[0]
        df.index = [horse_id] * len(df)
        df["horse_id"] = df.index
        df["horse_id"].astype(int)
        # "R"列の値が数値を表す文字列であるかを判定し、数値を表す文字列の場合にintに変換する
        for index, value in df["R"].items():
            if isinstance(value, str) and value.isdigit():
                df.at[index, "R"] = int(value)

        # "R"列のデータ型をintに変換する
        df["R"] = pd.to_numeric(df["R"], errors="coerce").astype(float).astype("Int64")

        df.columns = df.columns.str.replace(" ", "")
        df.iloc[:, 1] = df.iloc[:, 1].apply(convert_string)
        horse_result[horse_id] = df
        # 各レースの結果データフレームを結合して race_results_df を生成
        horse_result_df = pd.concat([horse_result[key] for key in horse_result])
        horse_result_df.set_index("horse_id", inplace=True)

    return horse_result_df


################################# Done ####################################
def create_raw_horse_info(target_bin_file_path):
    from bs4 import BeautifulSoup
    horse_info = {}
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()

        # 馬の基本情報を取得（bin は UTF-8 で保存されているため StringIO で解析）
        html_str = html.decode("utf-8", errors="replace")
        tables = pd.read_html(io.StringIO(html_str))
        # プロフィールテーブルは「生年月日」行を持つ 2 列テーブルで特定する。
        # JS 描画後のページは 2 列テーブルが複数あり（次走予定・血統ミニ表等）、
        # 「最初の 2 列テーブル」では血統表を誤って拾うことがある。
        profile_table = None
        for t in tables:
            if t.shape[1] == 2 and (t.iloc[:, 0].astype(str) == "生年月日").any():
                profile_table = t
                break
        if profile_table is None:
            for t in tables:
                if t.shape[1] == 2 and t.iloc[:, 0].dtype == object:
                    profile_table = t
                    break
        if profile_table is None:
            if len(tables) < 2:
                raise IndexError(f"馬プロフィールテーブルが見つかりません: {target_bin_file_path}")
            profile_table = tables[1]
        df = profile_table.set_index(0).T

        # htmlをsoupオブジェクトに変換
        soup = BeautifulSoup(html_str, "lxml")
        # 列に "募集情報" が含まれているかを調べる
        funding_info = df.apply(lambda x: x.str.contains("募集情報")).any()

        # 列に "募集情報" がある場合、その列の値を "募集情報" 列に代入する

        if funding_info.any():
            df["募集情報"] = df.loc[:, funding_info].values.flatten()
        else:
            # 列に "募集情報" が含まれていない場合、"募集情報" 列に NaN を入れる
            df["募集情報"] = NaN

        # print(f"soup:{soup}")
        # user_input = input()
        # if user_input == " ":
        #    pass

        # 調教師IDをスクレイピング
        try:
            trainer_a_list = soup.find("table", attrs={"summary": "のプロフィール"}).find_all(
                "a", attrs={"href": re.compile("^/trainer")}
            )
            trainer_id = re.findall(r"trainer/(\w*)", trainer_a_list[0]["href"])[0]
        except IndexError:
            # 調教師IDを取得できない場合
            # print('trainer_id empty {}'.format(race_html))
            trainer_id = NaN
        df["trainer_id"] = trainer_id

        # 馬主IDをスクレイピング
        try:
            owner_a_list = soup.find("table", attrs={"summary": "のプロフィール"}).find_all(
                "a", attrs={"href": re.compile("^/owner")}
            )
            owner_id = re.findall(r"owner/(\w*)", owner_a_list[0]["href"])[0]
        except IndexError:
            # 馬主IDを取得できない場合
            # print('owner_id empty {}'.format(race_html))
            owner_id = NaN
        df["owner_id"] = owner_id
        # df["owner_id"] = df["owner_id"].astype(str)

        # 生産者IDをスクレイピング
        try:
            breeder_a_list = soup.find("table", attrs={"summary": "のプロフィール"}).find_all(
                "a", attrs={"href": re.compile("^/breeder")}
            )
            breeder_id = re.findall(r"breeder/(\w*)", breeder_a_list[0]["href"])[0]
        except IndexError:
            # 生産者IDを取得できない場合
            # print('breeder_id empty {}'.format(race_html))
            breeder_id = NaN
        df["breeder_id"] = breeder_id

        # インデックスをhorse_idにする
        horse_id = re.findall(r"\d+", os.path.basename(target_bin_file_path))[0]
        df.index = [horse_id] * len(df)
        df["horse_id"] = df.index
        df["horse_id"].astype(int)

        horse_info[horse_id] = df
        # 各レースの結果データフレームを結合して race_results_df を生成
        horse_info_df = pd.concat([horse_info[key] for key in horse_info])
        horse_info_df.set_index("horse_id", inplace=True)

    return horse_info_df


################################# Done ####################################
def create_raw_horse_ped(target_bin_file_path):
    from bs4 import BeautifulSoup
    horse_ped = {}
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()
        # horse_idを取得

        # htmlをsoupオブジェクトに変換
        horse_id = re.findall(r"\d+", os.path.basename(target_bin_file_path))[0]
        html_str = html.decode("utf-8", errors="replace")
        soup = BeautifulSoup(html_str, "html.parser")
        df = pd.DataFrame()
        peds_id_list = []
        # 血統データからhorse_idを取得する
        ped_table = soup.find("table", attrs={"summary": "5代血統表"})
        if ped_table is None:
            # Try finding by class or other attributes for newer page formats
            ped_table = soup.find("table", class_="blood_table")
        if ped_table is None:
            raise ValueError(f"血統テーブルが見つかりません: {target_bin_file_path}")
        # 血統セルの馬リンクは相対（/horse/<id>）と絶対（https://db.netkeiba.com/horse/<id>/）
        # の両形式がある。各祖先には /horse/ped/<id>/・/horse/sire/<id>/ も併設される
        # ため、パスが /horse/<id> で終わる直接リンクだけを対象にする。
        ped_href_re = re.compile(r"(?:^|netkeiba\.com)/horse/(\w{10})/?$")
        horse_a_list = ped_table.find_all("a", attrs={"href": ped_href_re})

        for a in horse_a_list:
            # 血統データのhorse_idを抜き出す
            m = ped_href_re.search(a["href"])
            if m is None:
                continue
            peds_id_list.append(m.group(1))

        df[horse_id] = peds_id_list

        # pd.DataFrame型にして一つのデータにまとめて、列と行の入れ替えして、列名をpeds_0, ..., peds_61にする
        df = df.transpose()
        df.columns = ["peds_" + str(i) for i in range(len(df.columns))]
        df["horse_id"] = horse_id
        df["horse_id"].astype(int)
        # print("df", df)
        df["horse_id"] = df.index

        horse_ped[horse_id] = df
        # 各レースの結果データフレームを結合して race_results_df を生成
        horse_ped_df = pd.concat([horse_ped[key] for key in horse_ped])
        horse_ped_df.set_index("horse_id", inplace=True)

    return horse_ped_df


################################# Done ####################################
def create_raw_race_return(target_bin_file_path):
    race_return = {}
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()
        html_bytes = html.replace(b"<br />", b"br")
        # 払戻テーブルは summary="払い戻し" で特定する（1986〜現在まで全年代で共通、
        # 通常 2 表: 単勝〜馬連 / ワイド〜三連単。旧年代は馬券種が少ない）。
        # テーブル位置 (dfs[1], dfs[2]) 前提だと旧ページの空テーブルで崩れる。
        dfs = pd.read_html(io.BytesIO(html_bytes), attrs={"summary": "払い戻し"})
        df = pd.concat(dfs)
        # 列名を整数に変更

        # インデックスをrace_idにする
        race_id = re.findall(r"\d+", os.path.basename(target_bin_file_path))[0]
        df.index = [race_id] * len(df)
        # df["race_id"].astype(int)
        df["race_id"] = df.index
        # race_id列を除外して、他の列名のみを整数に変換する辞書を作成
        new_columns = {col: int(col) for col in df.columns if col != "race_id"}
        # 列名を変換
        df.rename(columns=new_columns, inplace=True)
        race_return[race_id] = df
        # 各レースの結果データフレームを結合して race_results_df を生成
        race_return_df = pd.concat([race_return[key] for key in race_return])
        race_return_df.set_index("race_id", inplace=True)

    return race_return_df


def dart_checker(text1):
    if text1.split("/")[0].strip()[:2] == "障芝":
        dart = True
    else:
        dart = False
    return dart


def count_ground_state(text1):
    return text1.split("/")[2].count(":")


def create_raw_race_info(target_bin_file_path):
    from bs4 import BeautifulSoup
    # print(f"target_bin_file_path:{target_bin_file_path}")
    with open(target_bin_file_path, "rb") as f:
        # 保存してあるbinファイルを読み込む
        html = f.read()

        # htmlをsoupオブジェクトに変換
        soup = BeautifulSoup(html, "lxml")

        # 天候、レースの種類、コースの長さ、馬場の状態、日付、回り、レースクラスをスクレイピング
        data_intro = soup.find("div", attrs={"class": "data_intro"})
        if data_intro is None:
            raise ValueError(f"data_intro not found (cancelled race?): {target_bin_file_path}")
        text1 = data_intro.find_all("p")[0].text
        text2 = data_intro.find_all("p")[1].text
        # print(f"text1:{text1}")
        # print(f"text2:{text2}")
        race_id = str(re.findall(r"\d+", os.path.basename(target_bin_file_path))[0])
        # print(f"race_id :{race_id}")
        # netkeiba に実体のない空ページ（race_name 空・日付 1970-01-01 等）は
        # 中止/欠番レースと同様にクリーンにスキップする。各種パース（天候など）の
        # 前に判定し、空ページに対する無意味な警告や place_id 未確定クラッシュを防ぐ。
        race_date = text2.split(" ")[0]
        race_name = text2.split(" ")[1]
        if not race_name.strip():
            raise ValueError(f"empty race page (cancelled/invalid?): {target_bin_file_path}")
        # テキスト情報を解析してDataFrameに変換
        race_distance = _re_first_int(text1.split("/")[0])
        weather = text1.split("/")[1].split(":")[1].strip()

        if weather in Master.WEATHER_LIST:
            pass
        else:
            logger.warning("unknown weather definition appeared:%s", race_id)

        race_type = text1.split("/")[2].split(":")[0].strip()
        # 発走時刻
        start_time = text1.split("/")[-1].split(":")[1:3]
        start_time = ":".join(start_time).strip().split("\n\n")[0]

        dart = dart_checker(text1)
        # print(f"dart:{dart}")
        # print(f"count:{count_ground_state(text1)}")
        # test = text1.split("/")[2].split(":")[2]
        # print(f"gs2:{test}")

        for around in Master.AROUND_LIST:
            if around in text1.split("/")[0]:
                around_info = around
            else:
                around_info = None

        # 開催日数と開催回数を取得
        race_day_count = _re_first_int(race_name.split("日目")[0])  # 開催日数
        race_round_count = _re_first_int(race_name.split("回")[0])  # 開催回数

        # 開催場所を取得
        # 未知の開催場所（海外・地方の表記揺れ等）でも UnboundLocalError で
        # クラッシュせず DataFrame 構築まで到達できるよう NaN で初期化する
        # （ground_state と同じ防御パターン）。未知時のみ警告を残す。
        place_id = NaN
        place_name = NaN
        for key, value in Master.PLACE_DICT.items():
            if key in race_name:
                place_id = value
                place_name = key
        if place_name is NaN:
            logger.warning("unknown place definition appeared:%s", race_id)
        # 未知の馬場状態でも DataFrame 構築（後段 768-769 行）まで到達できるよう、
        # スクレイプ生値をそのまま使うフォールバックを置く（既存は NameError でクラッシュ）。
        ground_state1 = NaN
        ground_state2 = NaN
        if count_ground_state(text1) == 1:
            temp_ground_state0 = text1.split("/")[2].split(":")[1].strip()
            # 既知/未知に関わらず生値を採用。未知時のみ警告を残す。
            ground_state1 = temp_ground_state0
            ground_state2 = temp_ground_state0
            if temp_ground_state0 not in Master.GROUND_STATE_LIST:
                logger.warning(
                    "unknown GROUND_STATE definition appeared1:%s%s", race_id, temp_ground_state0
                )
        elif dart_checker(text1) and count_ground_state(text1) == 2:
            temp_ground_state1 = text1.split("/")[2].split(":")[1].split()[0].strip()
            ground_state1 = temp_ground_state1
            if temp_ground_state1 not in Master.GROUND_STATE_LIST:
                logger.warning("unknown GROUND_STATE definition appeared2:%s%s", race_id, temp_ground_state1)
            temp_ground_state2 = text1.split("/")[2].split(":")[2].strip()
            ground_state2 = temp_ground_state2
            if temp_ground_state2 not in Master.GROUND_STATE_LIST:
                logger.warning("unknown GROUND_STATE definition appeared3:%s%s", race_id, temp_ground_state2)
        # 不要な部分を削除
        # レース条件から年齢、性別、レースクラスを削除
        # 馬齢を取得
        race_condition = text2.split(" ")[2]

        # レース条件に基づいてフラグを設定
        race_flags = {}
        if race_condition is not None:
            # 性別を取得
            sex_info = None
            for sex in Master.SEX_LIST:
                if sex in race_condition:
                    sex_info = sex

            # レースクラスを取得
            race_class_info = None
            for race_class in Master.RACE_CLASS_LIST:
                if race_class in race_condition:
                    race_class_info = race_class
            if race_class_info is None:
                # 2019 年のクラス名称変更以前（500万下/1000万下/1600万下 等）は
                # 現行クラスへ正規化する
                for legacy, modern in Master.RACE_CLASS_LEGACY_ALIASES.items():
                    if legacy in race_condition:
                        race_class_info = modern
                        break
            if race_class_info is None:
                logger.warning("unknown race_class definition appeared:%s", race_id)
            # 向きを取得
            if (around_info is None) and (
                ("障害" in (race_class_info or "") or race_condition) or dart
            ):
                around_info = "直線"

            for key, value in Master.RACE_CONDITION_DICT.items():
                if key in race_condition:
                    race_flags[value] = 1
                    race_condition = race_condition.replace(key, "").strip()
                else:
                    race_flags[value] = 0

        if "歳以上" in race_condition:
            age = _re_first_int(race_condition.split("歳以上")[0]) + "+"

        else:
            age = _re_first_int(race_condition.split("歳")[0])
        if race_condition is not None:
            # ageの処理を修正
            if age is not None and age != "":
                if "+" in age:
                    race_condition = race_condition.replace(age[:-1], "").replace("歳以上", "").strip()
                else:
                    race_condition = race_condition.replace(age, "").replace("歳", "").strip()

            if sex_info is not None and sex_info != "":
                race_condition = race_condition.replace(sex_info, "").strip()
            if race_class_info is not None and race_class_info != "":
                race_condition = race_condition.replace(race_class_info, "").strip()
            if race_condition is not None and race_condition != "":
                race_condition = race_condition.replace("()", "").strip()
                race_condition = race_condition.replace("[]", "").strip()
            if race_condition is not None and race_condition != "":
                race_condition = race_condition.strip()

        # DataFrame作成歳
        df = pd.DataFrame(
            {
                "race_id": [race_id],
                "place_id": [place_id],
                "place": [place_name],
                "days": [race_day_count],  # 開催日数を追加
                "times": [race_round_count],  # 開催回数を追加
                "date": [race_date],
                "time": [start_time],
                "race_type": [race_type],
                "around": [around_info],
                "course_len": [race_distance],
                "weather": [weather],
                "ground_state1": [ground_state1],
                "ground_state2": [ground_state2],
                "age": [age],
                "sex": [sex_info],
                "race_class": [race_class_info],
                "race_condition": [race_condition],
                **race_flags,
            }
        )
    df["race_id"].astype(int)
    df.set_index("race_id", inplace=True)
    return df


r"""
def create_tmp_race_info(target_bin_file_path):
    with open(target_bin_file_path, "rb") as f:
        html = f.read()
        soup = BeautifulSoup(html, "lxml")

        # 天候、レースの種類、コースの長さ、馬場の状態、日付、回り、レースクラスをスクレイピング
        data_intro = soup.find("div", attrs={"class": "data_intro"})
        if data_intro is None:
            raise ValueError(f"data_intro not found (cancelled race?): {target_bin_file_path}")
        text1 = data_intro.find_all("p")[0].text
        text2 = data_intro.find_all("p")[1].text
        logger.debug("text1:%s", text1)
        logger.debug("text2:%s", text2)

        # テキスト情報を解析してDataFrameに変換
        race_distance = _re_first_int(text1.split("/")[0])
        weather = text1.split("/")[1].split(":")[1].strip()

        if weather in Master.WEATHER_LIST:
            pass
        else:
            logger.warning("unknown weather definition appeared")

        race_type = text1.split("/")[2].split(":")[0].strip()
        # 向きを取得

        around_info = None
        for around in Master.AROUND_LIST:
            if around in text1.split(" ")[0]:
                around_info = around
        if around_info is None:
            logger.warning("unknown around definition appeared")

        ground_state1 = text1.split("/")[2].split(":")[1].strip()
        if ground_state1 in Master.GROUND_STATE_LIST:
            pass
        else:
            logger.warning("unknown GROUND_STATE definition appeared")

        start_time = text1.split("/")[-1].split(":")[1:3]
        start_time = ":".join(start_time).strip()
        race_date = text2.split(" ")[0]
        race_name = text2.split(" ")[1]

        # 開催日数と開催回数を取得
        race_day_count = _re_first_int(race_name.split("日目")[0])  # 開催日数
        race_round_count = _re_first_int(race_name.split("回")[0])  # 開催回数

        # 開催場所を取得
        place_id = None
        for key, value in Master.PLACE_DICT.items():
            if key in race_name:
                place_id = value
                place_name = key
        # 馬齢を取得
        race_condition = text2.split(" ")[2]
        age = _re_first_int(race_condition.split("歳")[0])
        # 性別を取得
        sex_info = None
        for sex in Master.SEX_LIST:
            if sex in race_condition:
                sex_info = sex
        if sex_info is None:
            logger.warning("unknown sex definition appeared")
        # レースクラスを取得
        race_class_info = None
        for race_class in Master.RACE_CLASS_LIST:
            if race_class in race_condition:
                race_class_info = race_class
        if race_class_info is None:
            logger.warning("unknown race_class definition appeared")
        # 不要な部分を削除
        # レース条件から年齢、性別、レースクラスを削除
        race_condition = (
            race_condition.replace(age, "")
            .replace("歳", "")
            .replace(sex_info, "")
            .replace(race_class_info or "", "")
            .strip()
        )

        # DataFrame作成歳
        df = pd.DataFrame(
            {
                "レース名": [race_name],
                "レース場id": [place_id],
                "レース場名": [place_name],
                "開催日数": [race_day_count],  # 開催日数を追加
                "開催回数": [race_round_count],  # 開催回数を追加
                "レース開催日": [race_date],
                "発走時刻": [start_time],
                "レース種類": [race_type],
                "向き": [around_info],
                "レース距離": [race_distance],
                "天候": [weather],
                "馬場状態1": [ground_state1],
                "馬場状態2": [ground_state2],
                "馬齢": [age],
                "性別": [sex_info],
                "レースクラス": [race_class_info],
                "レース条件": [race_condition],
            }
        )

    return df



        info = re.findall(r"\w+", texts)
        length = len(info)

        # インデックスをrace_idにする
        race_id = re.findall(r"\d+", os.path.basename(target_bin_file_path))[0]
        df.index = [race_id] * length
        df["id"] = range(1, length + 1)
        df["info"] = info
        df["race_id"] = race_id
        df["id"] = df["id"].astype(int)

    return df


        # 天候、レースの種類、コースの長さ、馬場の状態、日付、回り、レースクラスをスクレイピング
        texts = (
            soup.find("div", attrs={"class": "data_intro"}).find_all("p")[0].text
            + soup.find("div", attrs={"class": "data_intro"}).find_all("p")[1].text
        )

        info = re.findall(r"\w+", texts)
        logger.debug("info:%s", info)
        df = pd.DataFrame()
        race_id = re.findall(r"\d+", os.path.basename(target_bin_file_path))[0]

        # 障害レースフラグを初期化
        hurdle_race_flg = False
        for text in info:
            if text in ["芝", "ダート", "障害"]:
                df["race_type"] = [text]
            # もし、textが任意の文字列＋3桁か4桁の数字+ "m"　（例えば、1200m ）の様に表現されている場合に、
            # その数字部分の文字を抽出し、整数化の上、df["course_len"]に格納する処理をここに入れたい
            # 正規表現パターン
            pattern = r"([0-9]{3})m|([0-9]{4})m"
            # 正規表現に一致する部分を抽出
            matches = re.findall(pattern, text)
            if matches:
                for match in matches:
                    # キャプチャグループから数字部分を取得
                    extracted_number = match[0] if match[0] else match[1]
                df["course_len"] = [int(extracted_number)]

            if "右" in text:
                df["around"] = [Master.AROUND_RIGHT]
            if "左" in text:
                df["around"] = [Master.AROUND_LEFT]
            if "直線" in text:
                df["around"] = [Master.AROUND_STRAIGHT]
            if "障害" in text:
                # AROUND_LIST[3] は範囲外: 既存挙動保持のため位置参照を残す。
                df["around"] = [Master.AROUND_LIST[3]]
                hurdle_race_flg = True

            if text in Master.GROUND_STATE_LIST:
                df["ground_state"] = [text]
            if text in Master.WEATHER_LIST:
                df["weather"] = [text]
            if "年" in text:
                df["date"] = [text]

            if "新馬" in text:
                df["race_class"] = [Master.RACE_CLASS_SHINBA]
            if "未勝利" in text:
                df["race_class"] = [Master.RACE_CLASS_MISHORI]
            if ("1勝クラス" in text) or ("500万下" in text):
                df["race_class"] = [Master.RACE_CLASS_1SHO]
            if ("2勝クラス" in text) or ("1000万下" in text):
                df["race_class"] = [Master.RACE_CLASS_2SHO]
            if ("3勝クラス" in text) or ("1600万下" in text):
                df["race_class"] = [Master.RACE_CLASS_3SHO]
            if "オープン" in text:
                df["race_class"] = [Master.RACE_CLASS_OPEN]
            if hurdle_race_flg:
                # 障害は race_class を上書きしない（障害判定は race_type 側で表現）。
                hurdle_race_flg = False

        # グレードレース情報の取得（grade アイコン → 対応グレードへ正しくマップ）
        grade_text = soup.find("div", attrs={"class": "data_intro"}).find_all("h1")[0].text
        if "G3" in grade_text:
            df["race_class"] = [Master.RACE_CLASS_G3] * len(df)
        elif "G2" in grade_text:
            df["race_class"] = [Master.RACE_CLASS_G2] * len(df)
        elif "G1" in grade_text:
            df["race_class"] = [Master.RACE_CLASS_G1] * len(df)

        df["race_id"] = race_id

    return df

"""
