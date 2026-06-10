"""ライブ予想用の出馬表・レース一覧スクレイパー（§予想セクション）。

main.ipynb の「予想」セクション（前日予想・当日予想）が呼び出す 3 関数を提供する:

- ``scrape_race_id_race_time_list(kaisai_date)``: 開催日のレース一覧から
  race_id と発走時刻のペアを取得する。
- ``create_active_race_id_list()``: 本日開催で馬体重が発表済み（=発走間近）の
  レースだけを返す。
- ``scrape_shutuba_table(race_id, date_str, filepath)``: 出馬表ページを取得し、
  ``ShutubaTableProcessor`` が前処理できる形式の DataFrame を pickle 保存する。

netkeiba の JS 描画ページに対応するため ``PlaywrightScraper.fetch_sync`` で
HTML を取得し、BeautifulSoup(lxml) でパースする。
"""

from __future__ import annotations

import datetime
import logging
import re

import pandas as pd
from bs4 import BeautifulSoup

from src.constants._master import Master
from src.preparing._scraper import PlaywrightScraper

logger = logging.getLogger(__name__)

NaN = float("nan")

# race_list_sub.html はレンダリング済みのレース一覧（result/shutuba アンカー＋発走時刻）を返す
_RACE_LIST_SUB_URL = "https://race.netkeiba.com/top/race_list_sub.html?kaisai_date={kaisai_date}"
_SHUTUBA_URL = "https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"

# href から race_id を抜く（result.html / shutuba.html いずれの形式も許容）
_RACE_ID_RE = re.compile(r"race_id=(\d{12})")
# HH:MM 形式の発走時刻
_TIME_RE = re.compile(r"(\d{1,2}:\d{2})")


def _parse_race_id_time_from_html(html: str, expected_year: str | None = None):
    """race_list_sub の HTML から (race_id_list, race_time_list) を抽出する。

    各レース項目（``li.RaceList_DataItem`` 相当）ごとに race_id と発走時刻を対にする。
    race_id の重複は除去し、race_id と time の並びを揃えたまま返す。
    """
    soup = BeautifulSoup(html, "lxml")
    race_id_list: list[str] = []
    race_time_list: list[str] = []
    seen: set[str] = set()

    # レース項目単位でループ（race_id と発走時刻を同じ要素から取り出して整合させる）。
    items = soup.select("li.RaceList_DataItem")
    if not items:
        # フォールバック: race_id を持つアンカーを直接走査
        items = soup.find_all("a", href=_RACE_ID_RE)

    for item in items:
        # race_id: 項目内の race_id を持つアンカー（無ければ item 自身）から取得
        anchor = item if item.name == "a" else item.find("a", href=_RACE_ID_RE)
        href = (anchor.get("href", "") if anchor is not None else "") or ""
        m = _RACE_ID_RE.search(href)
        if not m:
            # data 属性等にある場合も拾う
            m = _RACE_ID_RE.search(str(item))
        if not m:
            continue
        race_id = m.group(1)
        if expected_year is not None and race_id[:4] != expected_year:
            continue
        if race_id in seen:
            continue

        # 発走時刻: RaceList_Itemtime クラス優先、無ければ項目テキストから HH:MM を拾う
        race_time = ""
        time_el = item.find(class_=re.compile("RaceList_Itemtime")) if item.name != "a" else None
        if time_el is not None:
            tm = _TIME_RE.search(time_el.get_text(strip=True))
            if tm:
                race_time = tm.group(1)
        if not race_time:
            tm = _TIME_RE.search(item.get_text(" ", strip=True))
            if tm:
                race_time = tm.group(1)

        seen.add(race_id)
        race_id_list.append(race_id)
        race_time_list.append(race_time)

    return race_id_list, race_time_list


def scrape_race_id_race_time_list(kaisai_date: str):
    """開催日のレース一覧から race_id と発走時刻のリストを取得する。

    Parameters
    ----------
    kaisai_date : str
        開催日 'YYYYMMDD'（例 '20221001'）。

    Returns
    -------
    tuple[list[str], list[str]]
        (race_id_list, race_time_list)。それぞれ 12 桁 race_id と 'HH:MM' の
        並列リスト（並びは対応）。race_id は重複除去済み。
    """
    url = _RACE_LIST_SUB_URL.format(kaisai_date=kaisai_date)
    expected_year = str(kaisai_date)[:4]
    driver = PlaywrightScraper()
    try:
        html = driver.fetch_sync(url, wait_selector=".RaceList_DataList")
    except Exception as e:  # noqa: BLE001
        logger.error("レース一覧の取得に失敗: %s: %s", kaisai_date, e)
        return [], []

    try:
        race_id_list, race_time_list = _parse_race_id_time_from_html(html, expected_year)
    except Exception as e:  # noqa: BLE001
        logger.error("レース一覧のパースに失敗: %s: %s", kaisai_date, e)
        return [], []

    logger.info("%s: %d レースを取得", kaisai_date, len(race_id_list))
    return race_id_list, race_time_list


def _is_weight_published(html: str) -> bool:
    """出馬表 HTML の馬体重列に実値が入っているか（=発走間近か）を判定する。"""
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", class_=re.compile("Shutuba_Table"))
    if table is None:
        return False
    for td in table.find_all("td", class_=re.compile("Weight")):
        txt = td.get_text(strip=True)
        # '480(+2)' のように数値＋増減が入っていれば発表済み
        if re.search(r"\d", txt) and txt not in ("計不", "--", "-"):
            return True
    return False


def create_active_race_id_list():
    """本日開催で馬体重が発表済み（=発走間近）のレースの (race_id, time) を返す。

    本日の日付（``datetime.date.today``）を kaisai_date として race_list_sub から
    全レースを取得し、各レースの出馬表ページの馬体重列が発表済みのものだけに
    絞り込む。馬体重が未発表のレースは（前日予想ではなく当日予想の対象外として）除外する。

    判定のために各レースの出馬表ページを 1 件ずつ取得するため時間がかかる。
    出馬表が取得できない／判定に失敗したレースはスキップする。

    Returns
    -------
    tuple[list[str], list[str]]
        (race_id_list, race_time_list)。馬体重発表済みレースのみ。
    """
    today = datetime.date.today().strftime("%Y%m%d")
    all_race_ids, all_times = scrape_race_id_race_time_list(today)
    if not all_race_ids:
        return [], []

    active_ids: list[str] = []
    active_times: list[str] = []
    driver = PlaywrightScraper()
    driver.open_sync()
    try:
        for race_id, race_time in zip(all_race_ids, all_times):
            try:
                html = driver.fetch_sync(
                    _SHUTUBA_URL.format(race_id=race_id),
                    wait_selector=".Shutuba_Table",
                )
                if _is_weight_published(html):
                    active_ids.append(race_id)
                    active_times.append(race_time)
            except Exception as e:  # noqa: BLE001
                logger.warning("馬体重判定をスキップ: %s: %s", race_id, e)
                continue
    finally:
        driver.close_sync()

    logger.info("本日 %s: 馬体重発表済み %d レース", today, len(active_ids))
    return active_ids, active_times


def _extract_id(href: str, kind: str) -> object:
    """href から horse/jockey/trainer の ID を抜き出す。失敗時は NaN。

    netkeiba の href は ``/horse/2019104123`` 形式と
    ``/jockey/result/recent/05339/`` 形式の双方がある。末尾の英数字 ID を拾う。
    """
    if not href:
        return NaN
    # result/recent/<id> 形式（騎手・調教師）
    m = re.search(rf"/{kind}/(?:result/recent/)?(\w+)", href)
    return m.group(1) if m else NaN


def _parse_race_header(soup: BeautifulSoup, race_id: str):
    """出馬表ページのヘッダ（RaceData01/RaceData02/RaceName）から
    レースレベル情報を抽出する。

    Returns
    -------
    dict
        course_len(int), race_type, weather, ground_state, around, race_class。
    """
    info: dict[str, object] = {
        "course_len": NaN,
        "race_type": NaN,
        "weather": NaN,
        "ground_state": NaN,
        "around": NaN,
        "race_class": NaN,
    }

    data01 = soup.find(class_="RaceData01")
    text01 = data01.get_text(" ", strip=True) if data01 is not None else ""

    # 距離（例 '芝1600m'）
    m = re.search(r"(\d{3,4})\s*m", text01)
    if m:
        info["course_len"] = int(m.group(1))

    # レース種別（芝/ダ/障）→ 正準名
    if "障" in text01:
        info["race_type"] = Master.RACE_TYPE_HURDLE
    elif "ダ" in text01:
        info["race_type"] = Master.RACE_TYPE_DIRT
    elif "芝" in text01:
        info["race_type"] = Master.RACE_TYPE_TURF

    # 回り（右/左/直線）
    for around in Master.AROUND_LIST:
        if around in text01:
            info["around"] = around
            break
    if info["around"] is NaN and info["race_type"] == Master.RACE_TYPE_HURDLE:
        info["around"] = Master.AROUND_STRAIGHT

    # 天候（'天候:晴' 形式）
    m = re.search(r"天候\s*[:：]\s*(\S+)", text01)
    if m:
        w = m.group(1)
        for weather in Master.WEATHER_LIST:
            if weather in w:
                info["weather"] = weather
                break

    # 馬場状態（'馬場:良' / '芝:良' / 'ダート:良' 形式）
    m = re.search(r"(?:馬場|芝|ダート)\s*[:：]\s*(\S+)", text01)
    if m:
        g = m.group(1)
        for gs in Master.GROUND_STATE_LIST:
            if gs in g:
                info["ground_state"] = gs
                break

    # レースクラス: RaceName / RaceData02 のテキストから判定
    data02 = soup.find(class_="RaceData02")
    text02 = data02.get_text(" ", strip=True) if data02 is not None else ""
    name_el = soup.find(class_="RaceName")
    text_name = name_el.get_text(" ", strip=True) if name_el is not None else ""
    class_text = " ".join([text_name, text02])
    # グレードを優先的に判定
    if "G3" in class_text:
        info["race_class"] = Master.RACE_CLASS_G3
    elif "G2" in class_text:
        info["race_class"] = Master.RACE_CLASS_G2
    elif "G1" in class_text:
        info["race_class"] = Master.RACE_CLASS_G1
    else:
        for race_class in Master.RACE_CLASS_LIST:
            if race_class in class_text:
                info["race_class"] = race_class
                break
        # 旧表記の救済
        if info["race_class"] is NaN:
            if ("500万下" in class_text):
                info["race_class"] = Master.RACE_CLASS_1SHO
            elif ("1000万下" in class_text):
                info["race_class"] = Master.RACE_CLASS_2SHO
            elif ("1600万下" in class_text):
                info["race_class"] = Master.RACE_CLASS_3SHO

    return info


def scrape_shutuba_table(race_id: str, date_str: str, filepath: str) -> None:
    """出馬表ページを取得し、ShutubaTableProcessor 互換の DataFrame を pickle 保存する。

    Parameters
    ----------
    race_id : str
        12 桁 race_id。
    date_str : str
        レース開催日（例 '2022/10/01' / '2022/1/8'）。pd.to_datetime でパース可能な文字列。
    filepath : str
        保存先 pickle パス（例 'data/tmp/shutuba.pickle'）。

    Notes
    -----
    出力 DataFrame の index は race_id（全行同一）。列には ResultsProcessor が要求する
    馬単位列（枠番/馬番/斤量/単勝/性齢/馬体重/horse_id/jockey_id/trainer_id）と
    レースレベル列（date/course_len/around/race_type/weather/race_class/ground_state）を含む。
    馬体重が未発表（前日予想）の場合は空文字になることがあるが、呼び出し側で '0(0)' に
    上書きされるため問題ない。
    """
    url = _SHUTUBA_URL.format(race_id=race_id)
    driver = PlaywrightScraper()
    html = driver.fetch_sync(url, wait_selector=".Shutuba_Table")
    soup = BeautifulSoup(html, "lxml")

    table = soup.find("table", class_=re.compile("Shutuba_Table"))
    if table is None:
        raise ValueError(f"Shutuba_Table が見つかりません: {race_id}")

    header = _parse_race_header(soup, race_id)

    rows = []
    for tr in table.find_all("tr", class_=re.compile("HorseList")):
        try:
            tds = tr.find_all("td")
            if not tds:
                continue

            # 枠番（Waku）・馬番（Umaban）
            waku_td = tr.find("td", class_=re.compile("Waku"))
            umaban_td = tr.find("td", class_=re.compile("Umaban"))
            wakuban = waku_td.get_text(strip=True) if waku_td is not None else NaN
            umaban = umaban_td.get_text(strip=True) if umaban_td is not None else NaN

            # 馬名 / horse_id
            horse_a = tr.find("a", href=re.compile(r"/horse/"))
            horse_id = _extract_id(horse_a["href"], "horse") if horse_a is not None else NaN

            # 性齢（Barei）
            barei_td = tr.find("td", class_=re.compile("Barei"))
            sex_age = barei_td.get_text(strip=True) if barei_td is not None else NaN

            # 斤量（Txt_C のうち数値らしいもの）。Waku/Umaban/Barei も Txt_C のことが
            # あるため、それらの列を除外し、小数を含む値（例 57.0）を優先採用する。
            kinryo = NaN
            kinryo_fallback = NaN
            for td in tr.find_all("td", class_=re.compile("Txt_C")):
                cls = " ".join(td.get("class", []))
                if re.search(r"Waku|Umaban|Barei", cls):
                    continue
                txt = td.get_text(strip=True)
                if re.fullmatch(r"\d+\.\d+", txt):
                    kinryo = txt
                    break
                if kinryo_fallback is NaN and re.fullmatch(r"\d+(\.\d+)?", txt):
                    kinryo_fallback = txt
            if kinryo is NaN:
                kinryo = kinryo_fallback

            # 騎手 / jockey_id
            jockey_a = tr.find("a", href=re.compile(r"/jockey/"))
            jockey_id = _extract_id(jockey_a["href"], "jockey") if jockey_a is not None else NaN

            # 調教師 / trainer_id
            trainer_a = tr.find("a", href=re.compile(r"/trainer/"))
            trainer_id = _extract_id(trainer_a["href"], "trainer") if trainer_a is not None else NaN

            # 馬体重（Weight）。未発表時は空のまま。
            weight_td = tr.find("td", class_=re.compile("Weight"))
            weight = weight_td.get_text(strip=True) if weight_td is not None else ""
            if weight in ("計不", "--", "-"):
                weight = ""

            # 単勝オッズ（Txt_R Popular / Odds）
            tansho: object = NaN
            odds_td = tr.find("td", class_=re.compile("Odds|Popular"))
            if odds_td is not None:
                m = re.search(r"\d+(\.\d+)?", odds_td.get_text(strip=True))
                if m:
                    tansho = m.group(0)

            rows.append(
                {
                    "枠番": wakuban,
                    "馬番": umaban,
                    "斤量": kinryo,
                    "単勝": tansho,
                    "性齢": sex_age,
                    "馬体重": weight,
                    "horse_id": horse_id,
                    "jockey_id": jockey_id,
                    "trainer_id": trainer_id,
                    # レースレベル列（df_tmp.iat の位置参照に合わせた並び）
                    "date": date_str,
                    "course_len": header["course_len"],
                    "around": header["around"],
                    "race_type": header["race_type"],
                    "weather": header["weather"],
                    "race_class": header["race_class"],
                    "ground_state": header["ground_state"],
                }
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("出馬表の行パースをスキップ: %s: %s", race_id, e)
            continue

    if not rows:
        raise ValueError(f"出馬表の行が取得できませんでした: {race_id}")

    df = pd.DataFrame(rows)
    # index を race_id にする（全行同一）
    df.index = [race_id] * len(df)
    df.to_pickle(filepath)
    logger.info("出馬表を保存: %s (%d 頭) -> %s", race_id, len(df), filepath)
