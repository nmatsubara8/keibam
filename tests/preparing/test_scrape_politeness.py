"""取得ループ（modules.process_pkl_file）のポライトネス配線テスト。

race/horse/ped の bin 取得は単一 fetch 経路（PlaywrightScraper.fetch は 1 時間上限のみ）
のため、取得ループ側で 1 リクエストごとに polite_interval の間隔待機を挟む。
過去にこの間隔待機が未配線（delay/polite_interval がデッドコード）だった回帰を防ぐ。
"""

import pandas as pd

from src.preparing import _scrape_pages as modules


class _FakeLoader:
    """process_pkl_file が触る最小限の属性・メソッドだけ持つスタブ。"""

    def __init__(self, ids, tmp_dir):
        self._df = pd.DataFrame({"horse_id": ids})
        self.to_temp_location = str(tmp_dir)
        self.temp_save_file_name = "temp.csv"
        self.alias = "horse_html"
        self.batch_size = 50
        self.processing_id = None
        self.obtained_last_key = None
        self.target_data = None

    def load_file_pkl(self):
        return self._df

    def get_filetype(self):
        return "bin"  # horse/race/ped は bin

    def save_temp_file(self, alias):
        pass


def _patch_scraper(monkeypatch):
    """ネットワーク・ブラウザを使わないようスクレイパと取得をスタブ化する。"""
    import unittest.mock as mock

    from src.preparing import _scraper

    # process_pkl_file は _scraper.PlaywrightScraper を関数内で遅延 import するため、
    # 元モジュール側を差し替える。
    monkeypatch.setattr(_scraper, "PlaywrightScraper", lambda *a, **k: mock.Mock())
    # _fetch_with_retry は (取得データ, blocked) を返す契約。常に成功を返す。
    monkeypatch.setattr(
        modules, "_fetch_with_retry",
        lambda *a, **k: (pd.DataFrame({"x": [1]}), False),
    )


def test_polite_interval_applied_per_request(monkeypatch, tmp_path):
    _patch_scraper(monkeypatch)
    sleeps: list[float] = []
    monkeypatch.setattr(modules.time, "sleep", lambda s: sleeps.append(s))
    # 揺らぎ 0・基準 1.0 で待機を決定的にする（ちょうど 1.0 秒）。
    monkeypatch.setenv("KEIBA_SCRAPE_DELAY", "1.0")
    monkeypatch.setenv("KEIBA_SCRAPE_JITTER_MAX", "0")

    loader = _FakeLoader(["1", "2", "3"], tmp_path)
    modules.process_pkl_file(loader, process_function=lambda *a, **k: pd.DataFrame({"x": [1]}))

    # 3 リクエストすべてで間隔待機（>=1.0 秒）が入る
    assert len(sleeps) == 3
    assert all(s >= 1.0 for s in sleeps)


def test_delay_zero_disables_wait(monkeypatch, tmp_path):
    _patch_scraper(monkeypatch)
    sleeps: list[float] = []
    monkeypatch.setattr(modules.time, "sleep", lambda s: sleeps.append(s))
    # KEIBA_SCRAPE_DELAY=0 は明示無効化 → polite_interval は 0 を返し待機なし。
    monkeypatch.setenv("KEIBA_SCRAPE_DELAY", "0")

    loader = _FakeLoader(["1", "2"], tmp_path)
    modules.process_pkl_file(loader, process_function=lambda *a, **k: pd.DataFrame({"x": [1]}))

    assert sleeps == []
