"""JrdbStore の重複防止テスト。

① 既存データ内の重複排除: 主キー (race_id, umaban) + keep-last upsert。
② 新規ロード時の重複チェック: 処理済みファイル台帳（sha1）で未取込分だけ処理。
"""
from __future__ import annotations

import pandas as pd
import pytest

from src.jrdb._store import JrdbStore
from src.jrdb._store import file_sha1


@pytest.fixture()
def store(tmp_path):
    # tmp_path はテストごとに一意 → get_engine シングルトンも別 DB になり隔離される
    return JrdbStore(db_path=str(tmp_path / "jrdb_test.db"))


# ── 固定長レコード合成（parse を通す実ファイル用） ──
def _put(buf: bytearray, start1: int, s: str) -> None:
    b = s.encode("cp932")
    buf[start1 - 1: start1 - 1 + len(b)] = b


def _kyi_bytes(*, umaban: str = "01", idm: str = " 45.5") -> bytes:
    r = bytearray(b" " * 1024)
    _put(r, 1, "02152201")   # race_key → race_id 201502020201
    _put(r, 9, umaban)
    _put(r, 11, "13103588")  # 血統登録番号
    _put(r, 19, "テスト馬")
    _put(r, 55, idm)         # IDM（5 バイト）
    _put(r, 96, " 12.3")     # 基準オッズ
    return bytes(r) + b"\r\n"


# ======================================================================
# ① 既存データ内の重複排除（keep-last upsert）
# ======================================================================

def test_upsert_keep_last(store):
    """同一 (race_id, umaban) を二度 upsert → 1 行・後勝ち。"""
    store.upsert("KYI", pd.DataFrame({"race_id": ["R1"], "umaban": [1], "idm": [10.5]}))
    store.upsert("KYI", pd.DataFrame({"race_id": ["R1"], "umaban": [1], "idm": [20.5]}))
    out = store.read("KYI")
    assert len(out) == 1
    assert out.iloc[0]["race_id"] == "R1"
    assert out.iloc[0]["idm"] == "20.5"  # keep-last（TEXT 保存）


def test_upsert_dedup_within_frame(store):
    """同一 DataFrame 内に重複 PK があっても 1 行へ潰す（keep-last）。"""
    df = pd.DataFrame({"race_id": ["R1", "R1"], "umaban": [1, 1], "idm": [10.5, 20.5]})
    n = store.upsert("KYI", df)
    assert n == 1
    out = store.read("KYI")
    assert len(out) == 1 and out.iloc[0]["idm"] == "20.5"


def test_upsert_distinct_rows_kept(store):
    """異なる (race_id, umaban) は別行として保持される。"""
    df = pd.DataFrame({"race_id": ["R1", "R1", "R2"], "umaban": [1, 2, 1], "idm": [1.5, 2.5, 3.5]})
    store.upsert("KYI", df)
    assert len(store.read("KYI")) == 3


def test_upsert_drops_null_pk(store):
    """PK（race_id / umaban）が欠損の行は保存しない。"""
    df = pd.DataFrame({
        "race_id": ["R1", None, "R3"],
        "umaban": pd.array([1, 2, pd.NA], dtype="Int64"),
        "idm": [1.5, 2.5, 3.5],
    })
    n = store.upsert("KYI", df)
    assert n == 1  # R1 のみ（None race_id と NA umaban は落ちる）
    out = store.read("KYI")
    assert list(out["race_id"]) == ["R1"]


def test_upsert_empty_and_missing_pk(store):
    assert store.upsert("KYI", pd.DataFrame()) == 0
    with pytest.raises(ValueError, match="主キー列"):
        store.upsert("KYI", pd.DataFrame({"idm": [1.0]}))
    with pytest.raises(ValueError, match="record_type"):
        store.upsert("ZZZ", pd.DataFrame({"race_id": ["R1"], "umaban": [1]}))


def test_tables_are_separate_per_type(store):
    """KYI と SED は別テーブル（同種データでも衝突しない）。"""
    store.upsert("KYI", pd.DataFrame({"race_id": ["R1"], "umaban": [1], "idm": [9.5]}))
    store.upsert("SED", pd.DataFrame({"race_id": ["R1"], "umaban": [1], "chakujun": [3]}))
    assert len(store.read("KYI")) == 1
    assert len(store.read("SED")) == 1
    assert "chakujun" in store.read("SED").columns
    assert "chakujun" not in store.read("KYI").columns


# ======================================================================
# ② 新規ロード時の重複チェック（処理済みファイル台帳）
# ======================================================================

def test_ingest_skips_same_content(store, tmp_path):
    """同一内容のファイルを二度 ingest → 2 回目は skip・行は増えない。"""
    p = tmp_path / "KYI150712.txt"
    p.write_bytes(_kyi_bytes(umaban="01"))

    s1 = store.ingest_files({"KYI": [str(p)]})
    assert s1["KYI"]["files"] == 1 and s1["KYI"]["skipped"] == 0 and s1["KYI"]["rows"] == 1
    assert len(store.read("KYI")) == 1

    s2 = store.ingest_files({"KYI": [str(p)]})
    assert s2["KYI"]["files"] == 0 and s2["KYI"]["skipped"] == 1
    assert len(store.read("KYI")) == 1  # 重複せず


def test_ingest_reingests_on_content_change(store, tmp_path):
    """同名でも内容が変われば（sha1 変化）再取込し keep-last で上書きする。"""
    p = tmp_path / "KYI150712.txt"
    p.write_bytes(_kyi_bytes(umaban="01", idm=" 45.5"))
    store.ingest_files({"KYI": [str(p)]})
    assert store.read("KYI").iloc[0]["idm"] == "45.5"

    # 訂正版（同名・内容変更）
    p.write_bytes(_kyi_bytes(umaban="01", idm=" 60.5"))
    s = store.ingest_files({"KYI": [str(p)]})
    assert s["KYI"]["files"] == 1 and s["KYI"]["skipped"] == 0
    out = store.read("KYI")
    assert len(out) == 1 and out.iloc[0]["idm"] == "60.5"  # keep-last で訂正反映


def test_force_reingests_even_if_recorded(store, tmp_path):
    p = tmp_path / "KYI150712.txt"
    p.write_bytes(_kyi_bytes(umaban="01"))
    store.ingest_files({"KYI": [str(p)]})
    s = store.ingest_files({"KYI": [str(p)]}, force=True)
    assert s["KYI"]["files"] == 1 and s["KYI"]["skipped"] == 0


def test_is_ingested_and_ledger(store, tmp_path):
    p = tmp_path / "KYI150712.txt"
    p.write_bytes(_kyi_bytes(umaban="01"))
    sha1 = file_sha1(str(p))
    assert store.is_ingested(sha1) is False
    store.ingest_files({"KYI": [str(p)]})
    assert store.is_ingested(sha1) is True
    ledger = store.ingested_files()
    assert len(ledger) == 1
    assert ledger.iloc[0]["record_type"] == "KYI"
    assert ledger.iloc[0]["filename"] == "KYI150712.txt"


def test_cyb_ingest_roundtrip(store, tmp_path):
    """CYB（調教分析）が RECORD_TYPES に含まれ raw_jrdb_cyb に取り込まれる。"""
    r = bytearray(b" " * 94)   # 96 - CRLF
    _put(r, 1, "02152201")
    _put(r, 9, "03")
    _put(r, 30, "58 ")         # 追切指数
    _put(r, 86, "1")           # 調教評価
    p = tmp_path / "CYB080913.txt"
    p.write_bytes(bytes(r) + b"\r\n")
    s = store.ingest_files({"CYB": [str(p)]})
    assert s["CYB"]["files"] == 1 and s["CYB"]["rows"] == 1
    out = store.read("CYB")
    assert len(out) == 1
    assert out.iloc[0]["race_id"] == "201502020201" and out.iloc[0]["oikiri_idx"] == "58"


def test_ukc_master_keep_last(store):
    """UKC（馬基本）はマスタ PK=(ketto,)。同一 ketto は keep-last で 1 行。"""
    import pandas as pd
    store.upsert("UKC", pd.DataFrame({"ketto": ["13103588"], "massho_flag": [0]}))
    store.upsert("UKC", pd.DataFrame({"ketto": ["13103588"], "massho_flag": [1]}))  # 抹消に更新
    out = store.read("UKC")
    assert len(out) == 1 and out.iloc[0]["massho_flag"] == "1"
    assert "race_id" not in out.columns


def test_ksa_master_keep_last(store):
    """KSA（騎手）はマスタ PK=(kishu_code,)。週次更新の後着を keep-last で反映。"""
    import pandas as pd
    store.upsert("KSA", pd.DataFrame({"kishu_code": ["01234"], "honnen_leading": [10]}))
    store.upsert("KSA", pd.DataFrame({"kishu_code": ["01234"], "honnen_leading": [12]}))
    out = store.read("KSA")
    assert len(out) == 1 and out.iloc[0]["honnen_leading"] == "12"
    assert "race_id" not in out.columns and "umaban" not in out.columns


def test_csa_master_keep_last(store):
    """CSA（調教師）はマスタ PK=(chokyoshi_code,)。KSA と別テーブルで衝突しない。"""
    import pandas as pd
    store.upsert("CSA", pd.DataFrame({"chokyoshi_code": ["05678"], "honnen_leading": [5]}))
    store.upsert("KSA", pd.DataFrame({"kishu_code": ["05678"], "honnen_leading": [9]}))
    csa = store.read("CSA")
    ksa = store.read("KSA")
    assert len(csa) == 1 and csa.iloc[0]["chokyoshi_code"] == "05678"
    assert len(ksa) == 1 and ksa.iloc[0]["honnen_leading"] == "9"


def test_hjc_race_level_ingest(store, tmp_path):
    """HJC（払戻）はレース単位 PK=(race_id,) で raw_jrdb_hjc に取り込まれる。"""
    r = bytearray(b" " * 442)   # 444 - CRLF
    _put(r, 1, "02152201")      # race_key -> 201502020201
    _put(r, 9, "07")            # 単勝1 馬番
    _put(r, 11, "0000350")      # 単勝1 払戻
    p = tmp_path / "HJC150712.txt"
    p.write_bytes(bytes(r) + b"\r\n")
    s = store.ingest_files({"HJC": [str(p)]})
    assert s["HJC"]["files"] == 1 and s["HJC"]["rows"] == 1
    out = store.read("HJC")
    assert len(out) == 1
    assert out.iloc[0]["race_id"] == "201502020201"
    assert "umaban" not in out.columns          # レース単位（馬番キーなし）
    assert out.iloc[0]["tansho_pay1"] == "350"   # TEXT 保存


def test_hjc_keep_last_by_race(store, tmp_path):
    """同一 race_id の HJC を再取込 → 1 行（keep-last・レース単位 dedup）。"""
    import pandas as pd
    store.upsert("HJC", pd.DataFrame({"race_id": ["R1"], "tansho_pay1": [100]}))
    store.upsert("HJC", pd.DataFrame({"race_id": ["R1"], "tansho_pay1": [200]}))
    out = store.read("HJC")
    assert len(out) == 1 and out.iloc[0]["tansho_pay1"] == "200"


def test_ingest_skips_bad_record_length(store, tmp_path):
    """レコード長が仕様(128)と乖離する TYB は既定でスキップ（版差ガード）。"""
    p = tmp_path / "TYB100101.txt"
    # 118 バイト（正しくは 128）＝ 古いフォーマット版を模擬（content 有りで _records に残す）
    r = bytearray(b" " * 118)
    _put(r, 1, "02152201")
    _put(r, 9, "01")
    p.write_bytes(bytes(r) + b"\r\n")
    s = store.ingest_files({"TYB": [str(p)]})
    assert s["TYB"]["files"] == 0 and s["TYB"]["badlen"] == 1 and s["TYB"]["rows"] == 0
    assert len(store.read("TYB")) == 0


def test_allow_length_mismatch_forces_ingest(store, tmp_path):
    """--allow-length-mismatch 相当で版差ファイルも取り込む。"""
    r = bytearray(b" " * 118)
    _put(r, 1, "02152201")   # race_key
    _put(r, 9, "01")         # umaban
    p = tmp_path / "TYB100101.txt"
    p.write_bytes(bytes(r) + b"\r\n")
    s = store.ingest_files({"TYB": [str(p)]}, allow_length_mismatch=True)
    assert s["TYB"]["files"] == 1 and s["TYB"]["badlen"] == 0 and s["TYB"]["rows"] == 1


def test_persistence_across_store_instances(tmp_path):
    """別インスタンスでも台帳・データが残り、再取込されない（永続化）。"""
    db = str(tmp_path / "jrdb.db")
    p = tmp_path / "KYI150712.txt"
    p.write_bytes(_kyi_bytes(umaban="01"))

    JrdbStore(db_path=db).ingest_files({"KYI": [str(p)]})
    s = JrdbStore(db_path=db).ingest_files({"KYI": [str(p)]})  # 新インスタンス
    assert s["KYI"]["skipped"] == 1
    assert len(JrdbStore(db_path=db).read("KYI")) == 1
