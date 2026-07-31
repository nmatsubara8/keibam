"""JRDB TARGET 追加データのサンプルを機械的にプロファイルする（B フェーズ step3）。

data/jrdb_target_samples/ 以下の全ファイルを走査し、ZIP なら展開して各メンバーを、
そうでなければファイルそのものを検査する。判定するのは:

  - 文字コード（cp932/shift_jis/utf-8-sig/utf-8 の順で最初に decode できたもの）
  - 改行コード（CRLF / LF / CR）
  - 行数・行長の分布（固定長なら 1〜2 種に集中、CSV ならばらける）
  - 区切り文字の気配（カンマ / タブ）
  - 先頭数行のサンプル

結果は docs/jrdb_target_file_profiles.json（.gitignore 済み）に書き出す。
固定長か CSV か・CP932 か・日次と年次で同一形式か、をここまでで判定する。

使い方:
  python scripts/jrdb_target_profile.py
  python scripts/jrdb_target_profile.py data/jrdb_target_samples/gaikyu_comment
"""
from __future__ import annotations

import json
import sys
import zipfile
from collections import Counter
from pathlib import Path

ENCODINGS = ("cp932", "shift_jis", "utf-8-sig", "utf-8")


def detect_encoding(data: bytes) -> str | None:
    for encoding in ENCODINGS:
        try:
            data.decode(encoding)
            return encoding
        except UnicodeDecodeError:
            continue
    return None


def detect_newline(data: bytes) -> str | None:
    if b"\r\n" in data:
        return "CRLF"
    if b"\n" in data:
        return "LF"
    if b"\r" in data:
        return "CR"
    return None


def inspect_bytes(name: str, data: bytes) -> dict:
    encoding = detect_encoding(data)
    result: dict = {
        "name": name,
        "size_bytes": len(data),
        "encoding": encoding,
        "newline": detect_newline(data),
    }
    if encoding is None:
        result["note"] = "decode 不能（固定長バイナリ or 未知 encoding の可能性）"
        return result
    text = data.decode(encoding)
    lines = text.splitlines()
    length_hist = Counter(len(ln) for ln in lines)
    sample = lines[:5]
    result.update(
        {
            "line_count": len(lines),
            "distinct_line_lengths": len(length_hist),
            "line_lengths_top10": dict(length_hist.most_common(10)),
            "looks_fixed_width": len(length_hist) <= 2 and len(lines) > 5,
            "comma_per_line_top5": dict(
                Counter(ln.count(",") for ln in lines[:500]).most_common(5)
            ),
            "tab_per_line_top5": dict(
                Counter(ln.count("\t") for ln in lines[:500]).most_common(5)
            ),
            "first_lines": sample,
        }
    )
    return result


def inspect_file(path: Path) -> list[dict]:
    if zipfile.is_zipfile(path):
        results = []
        with zipfile.ZipFile(path) as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                results.append(inspect_bytes(info.filename, zf.read(info)))
        return results
    return [inspect_bytes(path.name, path.read_bytes())]


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/jrdb_target_samples")
    if not root.exists():
        print(f"見つかりません: {root}（サンプルを置いてから実行してください）")
        return 1

    reports = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.name not in (".gitkeep", "README.md"):
            reports.append({"archive": str(path), "members": inspect_file(path)})

    if not reports:
        print(f"サンプルがありません: {root}")
        return 1

    output = Path("docs/jrdb_target_file_profiles.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(reports, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"written: {output}  （{len(reports)} アーカイブ）")
    for rep in reports:
        for m in rep["members"]:
            fw = "固定長?" if m.get("looks_fixed_width") else "CSV/TSV?"
            print(
                f"  {rep['archive']} :: {m['name']}"
                f"  enc={m.get('encoding')} nl={m.get('newline')}"
                f" lines={m.get('line_count')} {fw}"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
