"""Phase 6-a: netkeiba 無料取得可否 調査スクリプトの解析ロジックのテスト。

実フェッチ（I/O）は行わず、純関数 analyze_* / training_verdict を合成 HTML で検証する。
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.probe_netkeiba_free import (
    analyze_shutuba_free_extras,
    analyze_training_page,
    training_verdict,
)

# ── 合成 HTML フィクスチャ ──────────────────────────────────

_FREE_TRAINING_HTML = """
<html><body>
<h1>追い切り</h1>
<table class="Oikiri_Table">
  <tr><th>調教日</th><th>コース</th><th>タイム</th></tr>
  <tr><td>栗東 坂路</td><td>ウッド</td><td>52.3</td></tr>
  <tr><td>美浦</td><td>坂路</td><td>13.4-12.8</td></tr>
  <tr><td>栗東</td><td>ウッド</td><td>67.9</td></tr>
</table>
<div>調教評価: A</div>
</body></html>
"""

_MEMBER_WALL_HTML = """
<html><body>
<div class="Icon_Member">この機能をご利用いただくにはログインが必要です。</div>
<p>プレミアムサービス（有料会員）にご登録ください。会員限定コンテンツです。</p>
</body></html>
"""

_EMPTY_NO_TRAINING_HTML = """
<html><body><h1>レース結果</h1><table><tr><td>着順</td></tr></table></body></html>
"""

_SHUTUBA_HTML = """
<html><body>
<table class="Shutuba_Table">
  <tr><th>脚質</th><th>馬体重</th></tr>
  <tr><td>逃げ</td><td>Weight: 480(+2)</td></tr>
</table>
<div class="PaceAnalysis">展開予想</div>
</body></html>
"""


class TestAnalyzeTrainingPage:
    def test_free_page_detected(self):
        r = analyze_training_page(_FREE_TRAINING_HTML)
        assert r["login_wall"] is False
        assert r["training_context"] is True
        assert r["lap_hits"] >= 1
        assert r["looks_free"] is True

    def test_member_wall_detected(self):
        r = analyze_training_page(_MEMBER_WALL_HTML)
        assert r["login_wall"] is True
        assert r["premium_hits"]
        assert r["looks_free"] is False

    def test_no_training_context(self):
        r = analyze_training_page(_EMPTY_NO_TRAINING_HTML)
        assert r["training_context"] is False
        assert r["looks_free"] is False

    def test_empty_html(self):
        r = analyze_training_page("")
        assert r["looks_free"] is False


class TestShutubaExtras:
    def test_leg_type_and_weight_detected(self):
        r = analyze_shutuba_free_extras(_SHUTUBA_HTML)
        assert "脚質" in r["leg_type_hits"]
        assert r["weight_hits"]
        assert r["pace_hits"]  # 展開予想


class TestTrainingVerdict:
    def test_proceed_when_any_free(self):
        v, _ = training_verdict([analyze_training_page(_FREE_TRAINING_HTML)])
        assert v == "PROCEED"

    def test_skip_when_all_walls(self):
        v, _ = training_verdict([analyze_training_page(_MEMBER_WALL_HTML)])
        assert v == "SKIP_TRAINING"

    def test_skip_when_no_training_context(self):
        v, _ = training_verdict([analyze_training_page(_EMPTY_NO_TRAINING_HTML)])
        assert v == "SKIP_TRAINING"

    def test_inconclusive_when_no_data_but_context(self):
        html = "<html><body>追い切り 坂路 美浦 栗東（データは会員のみ非表示）</body></html>"
        v, _ = training_verdict([analyze_training_page(html)])
        assert v == "INCONCLUSIVE"

    def test_empty_list(self):
        v, _ = training_verdict([])
        assert v == "INCONCLUSIVE"
