"""レイヤ単方向依存のアーキテクチャテスト。

src 配下の各レイヤが「自分より下位（または同位）」のレイヤしか import しないことを
静的に検証する。これにより、特定ロジックの修正影響が上位へ逆流する設計を機械的に防ぐ。
import-linter が無い環境でも pytest だけで境界を強制できる。
"""

import ast
import pathlib

import pytest

SRC = pathlib.Path(__file__).resolve().parents[2] / "src"

# 数値が小さいほど下位レイヤ。上位は下位（<=）のみ import 可。
# policies（馬券/スコア戦略）は constants/harville のみに依存する低位の戦略プリミティブで、
# training の KeibaAI（オーケストレータ）がそれらを束ねるため policies < training とする。
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


def _layer_of(module_parts):
    # module_parts: ["src", "<layer>", ...]
    if len(module_parts) >= 2 and module_parts[0] == "src" and module_parts[1] in LAYER_RANK:
        return module_parts[1]
    return None


def _iter_src_files():
    for path in SRC.rglob("*.py"):
        parts = path.relative_to(SRC).parts
        if parts and parts[0] in LAYER_RANK:
            yield path, parts[0]


def _imported_layers(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    layers = set()
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


def test_no_upward_dependencies():
    violations = []
    for path, own_layer in _iter_src_files():
        own_rank = LAYER_RANK[own_layer]
        for imported in _imported_layers(path):
            if LAYER_RANK[imported] > own_rank:
                violations.append(f"{path.relative_to(SRC)} ({own_layer}) -> {imported}")
    assert not violations, "上位レイヤへの逆方向依存を検出:\n" + "\n".join(violations)
