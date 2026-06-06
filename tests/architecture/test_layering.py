"""レイヤ単方向依存のアーキテクチャテスト。

src 配下の各レイヤが「自分より下位（または同位）」のレイヤしか import しないことを
静的に検証する。これにより、特定ロジックの修正影響が上位へ逆流する設計を機械的に防ぐ。

カバレッジ:
  - test_import_linter_contracts_pass : import-linter の全契約（4件）を CI で強制
  - test_no_upward_dependencies       : pytest 単独でもレイヤ逆依存を検出（高速・軽量）
  - test_constants_has_no_src_imports : constants の基盤純粋性をピンポイントで検証
  - test_policies_no_heavy_deps       : policies が training 以上に依存しないことを検証
  - test_app_helpers_no_toplevel_training_imports :
      app/ ヘルパが src.training / src.preparing / src.pipeline を
      モジュールトップレベルで import しないことを検証（lazy import を強制）
"""

import ast
import pathlib
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
APP = ROOT / "app"

# 数値が小さいほど下位レイヤ。上位は下位（<=）のみ import 可。
LAYER_RANK = {
    "constants": 0,
    "preprocessing": 1,
    "preparing": 2,
    "policies": 3,
    "training": 4,
    "portfolio": 5,
    "simulation": 6,
    "operation": 7,
    "pipeline": 8,
    "ui": 9,
}


# ---------------------------------------------------------------------------
# AST ユーティリティ
# ---------------------------------------------------------------------------

def _layer_of(module_parts: list[str]) -> str | None:
    if len(module_parts) >= 2 and module_parts[0] == "src" and module_parts[1] in LAYER_RANK:
        return module_parts[1]
    return None


def _iter_src_files():
    for path in SRC.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        parts = path.relative_to(SRC).parts
        if parts and parts[0] in LAYER_RANK:
            yield path, parts[0]


def _imported_src_layers(path: pathlib.Path) -> set[str]:
    """ファイル中のすべての `from src.X` / `import src.X` から X を返す。"""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError:
        return set()
    layers: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            layer = _layer_of(node.module.split("."))
            if layer:
                layers.add(layer)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                layer = _layer_of(alias.name.split("."))
                if layer:
                    layers.add(layer)
    return layers


def _toplevel_imported_src_layers(path: pathlib.Path) -> set[str]:
    """ファイルのトップレベル（関数・クラス外）の src.X import のみを返す。"""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError:
        return set()
    layers: set[str] = set()
    for node in ast.iter_child_nodes(tree):  # トップレベルのみ
        if isinstance(node, ast.ImportFrom) and node.module:
            layer = _layer_of(node.module.split("."))
            if layer:
                layers.add(layer)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                layer = _layer_of(alias.name.split("."))
                if layer:
                    layers.add(layer)
    return layers


# ---------------------------------------------------------------------------
# Test 1: import-linter 全契約パス（CI 強制）
# ---------------------------------------------------------------------------

def test_import_linter_contracts_pass():
    """lint-imports が 0 終了することで .importlinter 全契約を CI に強制する。

    この 1 テストで import-linter の 4 契約すべてをカバーする:
      - レイヤ単方向依存
      - constants 基盤純粋性
      - policies 独立性
      - simulation ↛ training
    """
    # lint-imports は importlinter パッケージの CLI エントリポイント
    lint_imports = pathlib.Path(sys.executable).parent / "lint-imports"
    cmd = [str(lint_imports)] if lint_imports.exists() else ["lint-imports"]
    result = subprocess.run(
        cmd,
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        "import-linter contracts failed:\n"
        + result.stdout
        + result.stderr
    )


# ---------------------------------------------------------------------------
# Test 2: 上位レイヤへの逆依存（AST 版 / import-linter 不要）
# ---------------------------------------------------------------------------

def test_no_upward_dependencies():
    """src 内の全ファイルで「自レイヤより上位レイヤへの import」がないことを検証する。

    import-linter の layers 契約と同等の検査を pytest 単独でも実行できるよう
    AST ベースで実装。CI での二重防御として機能する。
    """
    violations = []
    for path, own_layer in _iter_src_files():
        own_rank = LAYER_RANK[own_layer]
        for imported in _imported_src_layers(path):
            if LAYER_RANK[imported] > own_rank:
                violations.append(f"{path.relative_to(SRC)} ({own_layer}) -> {imported}")
    assert not violations, "上位レイヤへの逆方向依存を検出:\n" + "\n".join(violations)


# ---------------------------------------------------------------------------
# Test 3: constants 純粋性
# ---------------------------------------------------------------------------

def test_constants_has_no_src_imports():
    """src/constants/ は他の src レイヤを一切 import しない。

    constants に上位レイヤの依存が入ると全モジュールに影響が波及するため、
    このレイヤは標準ライブラリ・外部パッケージのみに依存する必要がある。
    """
    violations = []
    constants_dir = SRC / "constants"
    for path in constants_dir.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        imported = _imported_src_layers(path)
        if imported:
            violations.append(f"{path.relative_to(SRC)}: imports {imported}")
    assert not violations, "constants レイヤに src 依存を検出:\n" + "\n".join(violations)


# ---------------------------------------------------------------------------
# Test 4: policies 独立性
# ---------------------------------------------------------------------------

def test_policies_no_heavy_deps():
    """src/policies/ は training / simulation / portfolio / operation / pipeline に依存しない。

    policies は馬券・スコア戦略の純粋なプリミティブ。training 実装を知ると
    training を差し替えたときに policies にも変更が波及するため禁止する。
    """
    forbidden = {"training", "simulation", "portfolio", "operation", "pipeline"}
    violations = []
    policies_dir = SRC / "policies"
    for path in policies_dir.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        imported = _imported_src_layers(path) & forbidden
        if imported:
            violations.append(f"{path.relative_to(SRC)}: imports {imported}")
    assert not violations, "policies レイヤに禁止依存を検出:\n" + "\n".join(violations)


# ---------------------------------------------------------------------------
# Test 5: app ヘルパのトップレベル重依存禁止
# ---------------------------------------------------------------------------

def test_app_helpers_no_toplevel_training_imports():
    """app/ ヘルパモジュールは src.training / src.preparing / src.pipeline を
    モジュールトップレベルで import しない。

    これらの重い依存は関数内 lazy import にすることで:
      - Streamlit 起動時間を短縮する
      - テスト環境（torch/selenium 未インストール）でも app.* をインポートできる
      - 依存の可視性を上げ、どのコードパスで重い初期化が走るかを明示する

    app/pages/ は Streamlit が直接実行するスクリプトのため除外する。
    """
    heavy = {"training", "preparing", "pipeline"}
    violations = []

    # ヘルパモジュール（pages/ は除外）
    helper_files = [
        p for p in APP.rglob("*.py")
        if "__pycache__" not in p.parts and "pages" not in p.parts
    ]

    for path in helper_files:
        imported = _toplevel_imported_src_layers(path) & heavy
        if imported:
            violations.append(f"{path.relative_to(ROOT)}: top-level imports {imported}")

    assert not violations, (
        "app/ ヘルパに重依存のトップレベル import を検出（関数内 lazy import に変更してください）:\n"
        + "\n".join(violations)
    )
