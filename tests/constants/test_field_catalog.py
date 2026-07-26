"""フィールドカタログ（superset スキーマ定義）の構造テスト。"""

from src.constants import _field_catalog as fc


class TestStructuralIntegrity:
    def test_all_tables_present(self):
        for t in (
            "raw_results",
            "raw_race_info",
            "raw_horse_results",
            "raw_horse_info",
            "raw_peds",
            "raw_training",
            "raw_paddock",
            "raw_person_yearly",
        ):
            assert t in fc.CATALOG
            assert len(fc.columns(t)) > 0

    def test_no_duplicate_columns_per_table(self):
        for t in fc.all_tables():
            cols = fc.columns(t)
            assert len(cols) == len(set(cols)), f"duplicate columns in {t}: {cols}"

    def test_resolved_id_targets_exist(self):
        # 名寄せ先 ID 列は、同テーブルか他テーブルに実在する列であること
        all_cols = {c for t in fc.all_tables() for c in fc.columns(t)}
        for t in fc.all_tables():
            for name_col, id_col in fc.name_fields(t):
                assert name_col in fc.columns(t)
                assert id_col in all_cols, f"{t}.{name_col} -> 未定義の {id_col}"


class TestLeakSafety:
    def test_target_race_postrace_fields_marked_unsafe(self):
        # 当該レースの事後情報（着順/タイム/着差/上り/通過/賞金）は leak_safe=False
        unsafe = set(fc.columns("raw_results")) - set(fc.feature_safe_columns("raw_results"))
        for col in ("着順", "タイム", "着差", "上り", "通過", "賞金"):
            assert col in unsafe, f"{col} は事後情報なので leak_safe=False のはず"

    def test_horse_info_cumulative_marked_unsafe(self):
        unsafe = set(fc.columns("raw_horse_info")) - set(
            fc.feature_safe_columns("raw_horse_info")
        )
        for col in ("獲得賞金中央", "通算成績"):
            assert col in unsafe

    def test_static_horse_attrs_are_safe(self):
        safe = set(fc.feature_safe_columns("raw_horse_info"))
        for col in ("産地", "毛色", "母父", "父", "birthday"):
            assert col in safe

    def test_person_yearly_rates_unsafe_without_asof(self):
        # 当該年の率は集計途中 → as-of 結合前提で leak_safe=False
        unsafe = set(fc.columns("raw_person_yearly")) - set(
            fc.feature_safe_columns("raw_person_yearly")
        )
        for col in ("勝率", "連対率", "複勝率"):
            assert col in unsafe


class TestPremiumAndNew:
    def test_premium_columns_flagged(self):
        prem = set(fc.premium_columns("raw_results"))
        assert "タイム指数" in prem
        assert "厩舎コメント" in prem
        # 無料項目は含まれない
        assert "馬番" not in prem

    def test_new_columns_not_in_acquired(self):
        # 新規（未取得）列は acquired=False
        new_results = set(fc.new_columns("raw_results"))
        assert "通過" in new_results
        assert "上り" in new_results
        # 既存取得済みは new に含まれない
        assert "馬番" not in new_results
        assert "owner_id" not in new_results

    def test_training_is_all_new(self):
        # 調教テーブルは丸ごと新規
        assert set(fc.new_columns("raw_training")) == set(fc.columns("raw_training"))

    def test_existing_core_acquired(self):
        # 支配特徴は取得済みフラグ
        acquired = set(fc.columns("raw_results")) - set(fc.new_columns("raw_results"))
        for col in ("owner_id", "jockey_id", "trainer_id", "horse_id"):
            assert col in acquired


class TestNameFields:
    def test_horse_name_resolves_to_horse_id(self):
        nf = dict(fc.name_fields("raw_results"))
        assert nf.get("馬名") == "horse_id"
        assert nf.get("騎手") == "jockey_id"

    def test_training_workmate_resolution(self):
        nf = dict(fc.name_fields("raw_training"))
        assert nf.get("併入相手") == "併入相手_id"
