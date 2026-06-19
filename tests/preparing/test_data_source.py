"""データソース抽象化（_data_source）のテスト。"""

import json
import os

import pandas as pd
import pytest

from src.preparing._data_source import JraVanFileDropSource
from src.preparing._data_source import NetkeibaDataSource
from src.preparing._data_source import available_data_sources
from src.preparing._data_source import create_data_source
from src.preparing._data_source import load_selected_source
from src.preparing._data_source import save_selected_source


# ---------------------------------------------------------------------------
# ファクトリ / レジストリ
# ---------------------------------------------------------------------------

def test_available_and_factory():
    assert set(available_data_sources()) == {"netkeiba", "jravan"}
    assert create_data_source("netkeiba").name == "netkeiba"
    assert create_data_source("jravan").name == "jravan"


def test_factory_unknown_raises():
    with pytest.raises(ValueError, match="未対応"):
        create_data_source("bloodhorse")


# ---------------------------------------------------------------------------
# NetkeibaDataSource は既存スクレイパに委譲する
# ---------------------------------------------------------------------------

def test_netkeiba_acquire_races_delegates(monkeypatch):
    calls = {}
    import src.preparing._get_rawdata as gr
    import src.preparing._scrape_html_race as shr

    monkeypatch.setattr(shr, "scrape_html_race",
                        lambda df, skip=True: calls.setdefault("scrape", list(df["race_id"])))
    monkeypatch.setattr(gr, "get_rawdata_results",
                        lambda skip=True, only_ids=None: calls.setdefault("results", only_ids))
    monkeypatch.setattr(gr, "get_rawdata_info",
                        lambda skip=True, only_ids=None: calls.setdefault("info", only_ids))
    monkeypatch.setattr(gr, "get_rawdata_return",
                        lambda skip=True, only_ids=None: calls.setdefault("ret", only_ids))

    NetkeibaDataSource().acquire_races(["202401010101", "202401010102"])
    assert calls["scrape"] == ["202401010101", "202401010102"]
    assert calls["results"] == ["202401010101", "202401010102"]
    assert calls["info"] == ["202401010101", "202401010102"]
    assert calls["ret"] == ["202401010101", "202401010102"]


def test_netkeiba_acquire_races_empty_noop(monkeypatch):
    import src.preparing._scrape_html_race as shr

    monkeypatch.setattr(shr, "scrape_html_race", lambda *a, **k: pytest.fail("空でも呼ばれた"))
    NetkeibaDataSource().acquire_races([])  # 例外が出なければ OK


def test_netkeiba_acquire_horses_delegates(monkeypatch):
    calls = {}
    import src.preparing._get_rawdata as gr
    import src.preparing._scrape_html_horse as shh
    import src.preparing._scrape_html_ped as shp

    monkeypatch.setattr(shh, "scrape_html_horse_with_master",
                        lambda ids, skip=True: calls.setdefault("horse", list(ids)))
    monkeypatch.setattr(shp, "scrape_html_ped",
                        lambda ids, skip=True: calls.setdefault("ped", list(ids)))
    monkeypatch.setattr(gr, "get_rawdata_horse_results",
                        lambda skip=True, only_ids=None: calls.setdefault("hr", only_ids))
    monkeypatch.setattr(gr, "get_rawdata_horse_info",
                        lambda skip=True, only_ids=None: calls.setdefault("hi", only_ids))
    monkeypatch.setattr(gr, "get_rawdata_peds",
                        lambda skip=True, only_ids=None: calls.setdefault("peds", only_ids))

    NetkeibaDataSource().acquire_horses(["h1", "h2"])
    assert calls["horse"] == ["h1", "h2"]
    assert calls["hr"] == ["h1", "h2"]
    assert calls["peds"] == ["h1", "h2"]


# ---------------------------------------------------------------------------
# JraVanFileDropSource はファイル受信
# ---------------------------------------------------------------------------

def _write_split_json(path, df):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(df.to_json(orient="split"))


def test_jravan_resolve_race_ids(tmp_path):
    src_dir = str(tmp_path)
    _write_split_json(os.path.join(src_dir, "results", "202401010101.json"), pd.DataFrame({"x": [1]}))
    _write_split_json(os.path.join(src_dir, "results", "202401010102.json"), pd.DataFrame({"x": [1]}))
    _write_split_json(os.path.join(src_dir, "results", "202312310101.json"), pd.DataFrame({"x": [1]}))
    src = JraVanFileDropSource(incoming_dir=src_dir)
    assert src.resolve_race_ids("20240101") == ["202401010101", "202401010102"]


def test_jravan_acquire_races_merges(tmp_path, monkeypatch):
    src_dir = str(tmp_path / "incoming")
    _write_split_json(
        os.path.join(src_dir, "results", "202401010101.json"),
        pd.DataFrame({"race_id": ["202401010101"], "着順": [1]}),
    )
    merged = {}
    import src.preparing._get_rawdata as gr
    monkeypatch.setattr(gr, "update_rawdata", lambda path, df: merged.setdefault(path, df))

    JraVanFileDropSource(incoming_dir=src_dir).acquire_races(["202401010101"])
    # results のみファイルがある → results 用 pkl パスへ merge される
    assert any("results.pkl" in p for p in merged)
    assert not any("race_info.pkl" in p for p in merged)  # ファイル無し → スキップ


def test_jravan_missing_dir_returns_empty(tmp_path):
    src = JraVanFileDropSource(incoming_dir=str(tmp_path / "nope"))
    assert src.resolve_race_ids("20240101") == []
    src.acquire_races(["r1"])  # 例外を出さない


# ---------------------------------------------------------------------------
# 選択ソースの永続化
# ---------------------------------------------------------------------------

def test_save_load_selected_source(tmp_path):
    path = os.path.join(tmp_path, "sel.json")
    assert load_selected_source(path) == "netkeiba"  # 未保存は既定
    save_selected_source("jravan", path)
    assert load_selected_source(path) == "jravan"
    assert json.load(open(path))["data_source"] == "jravan"


def test_save_selected_source_validates(tmp_path):
    with pytest.raises(ValueError):
        save_selected_source("unknown", os.path.join(tmp_path, "x.json"))
