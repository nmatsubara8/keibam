#!/usr/bin/env bash
# build_lightgbm_gpu.sh — LightGBM を GPU 有効でビルドして現在の venv に入れる。
#
# 背景:
#  - CUDA ビルド(-DUSE_CUDA=ON)は nvcc が gcc<=13 を要求し gcc15 で不可だった。
#    → OpenCL ビルド(-DUSE_GPU=ON)は nvcc を使わず gcc で直接コンパイルするので gcc15 で通る。
#  - さらに Ubuntu 26.04 の Boost 1.90 は `system` コンポーネントを廃止（ヘッダオンリー化）した
#    ため、LightGBM 4.6 の find_package(Boost COMPONENTS filesystem system) が失敗する。
#    → ソースを clone し CMakeLists から `system` 要求を除去してからビルドする（system はリンク不要）。
#
# 使い方:
#   scripts/build_lightgbm_gpu.sh            # OpenCL(USE_GPU=ON) ビルド（既定・推奨）
#   LGB_BACKEND=cuda scripts/build_lightgbm_gpu.sh   # CUDA(USE_CUDA=ON) ※要 gcc-13 + CUDA Toolkit 12.8+
#     （RTX 50xx/Blackwell は sm_120 が 12.8 以上でのみ生成される。12.7 以下だと実行時に落ちる）
#   LGB_VERSION=4.6.0 scripts/build_lightgbm_gpu.sh  # clone するタグ（既定 4.6.0）
#
# ビルド後（コード側は配線済み）:
#   retrain ... --lgb-gpu gpu     # OpenCL ビルドを使う（device_type=gpu）
#   retrain ... --lgb-gpu cuda    # CUDA ビルドを使う（device_type=cuda）
#   単体確認:  python -c "import lightgbm as lgb, numpy as np; \
#     X=np.random.rand(4000,30); y=(X[:,0]>0.5).astype(int); \
#     lgb.train({'objective':'binary','device_type':'gpu','verbose':1}, lgb.Dataset(X,y), num_boost_round=20)"

set -euo pipefail
BACKEND="${LGB_BACKEND:-opencl}"
LGB_VERSION="${LGB_VERSION:-4.6.0}"

echo "[build_lightgbm_gpu] backend=${BACKEND}  version=v${LGB_VERSION}  gcc=$(gcc -dumpversion 2>/dev/null || echo '?')"

# --- バックエンド別の依存チェック & CMake フラグ -----------------------------
if [[ "${BACKEND}" == "opencl" ]]; then
    # OpenCL ICD ローダ + Boost。NVIDIA の OpenCL 実装はドライバ同梱（WSL2 は /usr/lib/wsl/lib）。
    _missing=()
    ls /usr/include/boost/version.hpp >/dev/null 2>&1 || _missing+=("libboost-all-dev")
    ldconfig -p 2>/dev/null | grep -q "libOpenCL.so" || _missing+=("ocl-icd-opencl-dev")
    command -v git >/dev/null 2>&1 || _missing+=("git")
    if [[ ${#_missing[@]} -gt 0 ]]; then
        echo "[build_lightgbm_gpu] 依存が不足しています。先に:"
        echo "    sudo apt-get install -y ${_missing[*]}"
        exit 1
    fi
    BUILD_FLAG="--gpu"
    PATCH_BOOST_SYSTEM=1   # Boost 1.90 で system 廃止 → CMakeLists から除去
else
    # CUDA: nvcc が gcc<=13 を要求。gcc-13 を CUDA ホストコンパイラに指定する。
    command -v git >/dev/null 2>&1 || { echo "git が必要です: sudo apt-get install -y git"; exit 1; }
    # nvcc（CUDA ツールキット）が必須。LightGBM の CUDA ビルドは nvcc でカーネルを生成する。
    if ! command -v nvcc >/dev/null 2>&1; then
        echo "[build_lightgbm_gpu] nvcc が見つかりません。CUDA Toolkit を導入してください:"
        echo "    sudo apt-get install -y cuda-toolkit-12-8   # 例。12.8 以上が必要（後述）"
        echo "    （nvidia-smi が動いても nvcc は別パッケージ。PATH に /usr/local/cuda/bin を通す）"
        exit 1
    fi
    # Blackwell(sm_120, RTX 50xx) の実行バイナリは CUDA Toolkit 12.8 以上でのみ生成される。
    # 12.7 以下だと build は通っても実行時に "no kernel image is available" で落ちる。
    _cuda_ver="$(nvcc --version 2>/dev/null | grep -oE 'release [0-9]+\.[0-9]+' | grep -oE '[0-9]+\.[0-9]+' | head -1)"
    _cuda_major="${_cuda_ver%%.*}"; _cuda_minor="${_cuda_ver##*.}"
    if [[ -n "${_cuda_ver}" ]] && { [[ "${_cuda_major}" -lt 12 ]] || { [[ "${_cuda_major}" -eq 12 ]] && [[ "${_cuda_minor}" -lt 8 ]]; }; }; then
        echo "[build_lightgbm_gpu] 警告: CUDA Toolkit ${_cuda_ver} を検出。RTX 50xx(Blackwell/sm_120) は"
        echo "    12.8 以上でないとカーネルが生成されず、実行時に 'no kernel image is available' になります。"
        echo "    12.8+ に更新してください（続行はしますが Blackwell では動きません）。"
    else
        echo "[build_lightgbm_gpu] CUDA Toolkit ${_cuda_ver:-?} を検出（12.8+ なら sm_120/Blackwell 対応）"
    fi
    if [[ -z "${CUDAHOSTCXX:-}" ]] && command -v g++-13 >/dev/null 2>&1; then
        export CUDAHOSTCXX=g++-13
        echo "[build_lightgbm_gpu] CUDAHOSTCXX=g++-13 を自動設定（未導入なら: sudo apt-get install -y gcc-13 g++-13）"
    elif [[ -z "${CUDAHOSTCXX:-}" ]]; then
        echo "[build_lightgbm_gpu] 警告: g++-13 が無く CUDAHOSTCXX 未設定。nvcc は gcc<=13 を要求するため"
        echo "    gcc-15 既定環境ではコンパイルに失敗します: sudo apt-get install -y gcc-13 g++-13"
    fi
    BUILD_FLAG="--cuda"
    PATCH_BOOST_SYSTEM=0
fi

# --- ソース取得 → パッチ → 公式 build-python.sh でビルド＆インストール --------
TMPD="$(mktemp -d)"
trap 'rm -rf "$TMPD"' EXIT
echo "[build_lightgbm_gpu] LightGBM v${LGB_VERSION} を clone します..."
git clone --recursive --depth 1 --branch "v${LGB_VERSION}" \
    https://github.com/microsoft/LightGBM "$TMPD/LightGBM"

if [[ "${PATCH_BOOST_SYSTEM}" == "1" ]]; then
    # (1) find_package(Boost ... COMPONENTS filesystem system ...) から system を除去
    #     （Boost 1.90 でヘッダオンリー化・リンク不要）。
    sed -i 's/filesystem system/filesystem/g' "$TMPD/LightGBM/CMakeLists.txt"
    echo "[build_lightgbm_gpu] CMakeLists の Boost 'system' 要求を除去しました"

    # (2) 同梱 boost::compute（メンテ停止）の sha1 ラッパーを Boost 1.90 の新 API に合わせる。
    #     boost::uuids::sha1::get_digest が unsigned int[5] → unsigned char[20] に変わったため、
    #     digest の型と 16 進整形を更新する（旧: 5語×8桁 → 新: 20バイト×2桁）。
    SHA1="$TMPD/LightGBM/external_libs/compute/include/boost/compute/detail/sha1.hpp"
    if [[ -f "$SHA1" ]]; then
        python3 - "$SHA1" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read()
s = s.replace("unsigned int digest[5];", "unsigned char digest[20];")
s = s.replace("for(int i = 0; i < 5; i++)", "for(int i = 0; i < 20; i++)")
s = s.replace("std::setw(8) << digest[i]",
              "std::setw(2) << static_cast<unsigned int>(digest[i])")
open(p, "w").write(s)
PY
        echo "[build_lightgbm_gpu] boost::compute sha1.hpp を Boost 1.90 新 API に合わせてパッチしました"
    fi
fi

echo "[build_lightgbm_gpu] build-python.sh install ${BUILD_FLAG} を実行します（数分）..."
( cd "$TMPD/LightGBM" && sh ./build-python.sh install "${BUILD_FLAG}" )

echo "[build_lightgbm_gpu] 完了。lightgbm=$(python -c 'import lightgbm; print(lightgbm.__version__)')"
echo "[build_lightgbm_gpu] 動作確認は冒頭コメントの python -c ワンライナーで（verbose=1 で GPU 使用を確認）"
