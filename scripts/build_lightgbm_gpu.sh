#!/usr/bin/env bash
# build_lightgbm_gpu.sh — LightGBM を GPU 有効でビルドして現在の venv に入れる。
#
# 背景: 前回失敗したのは CUDA ビルド（-DUSE_CUDA=ON）で、nvcc が gcc<=13 を要求するため
# gcc 15.2（Ubuntu 26.04）で不可だった。**OpenCL ビルド（-DUSE_GPU=ON）は nvcc を使わず
# gcc で直接コンパイルする**ので gcc 15 でも通る。まずは OpenCL 経路を推奨。
#
# 使い方:
#   scripts/build_lightgbm_gpu.sh            # OpenCL(USE_GPU=ON) ビルド（既定・推奨）
#   LGB_BACKEND=cuda scripts/build_lightgbm_gpu.sh   # CUDA(USE_CUDA=ON) ※要 gcc-13
#
# ビルド後の使い方（コード側は配線済み）:
#   retrain ... --lgb-gpu gpu     # OpenCL ビルドを使う（device_type=gpu）
#   retrain ... --lgb-gpu cuda    # CUDA ビルドを使う（device_type=cuda）
#   単体確認:  python -c "import lightgbm as lgb, numpy as np; \
#     X=np.random.rand(2000,20); y=(X[:,0]>0.5).astype(int); \
#     lgb.train({'objective':'binary','device_type':'gpu','verbose':1}, lgb.Dataset(X,y), num_boost_round=10)"

set -euo pipefail
BACKEND="${LGB_BACKEND:-opencl}"

echo "[build_lightgbm_gpu] backend=${BACKEND}  gcc=$(gcc -dumpversion 2>/dev/null || echo '?')"

if [[ "${BACKEND}" == "opencl" ]]; then
    # --- OpenCL 依存（nvcc 不要・gcc15 可）---------------------------------
    # Ubuntu/WSL2: OpenCL ICD ローダ + Boost。NVIDIA の OpenCL 実装はドライバ同梱
    # （WSL2 は /usr/lib/wsl/lib に libnvidia-opencl）。未導入なら:
    #   sudo apt-get install -y ocl-icd-opencl-dev libboost-dev libboost-system-dev \
    #        libboost-filesystem-dev cmake build-essential
    echo "[build_lightgbm_gpu] OpenCL(USE_GPU=ON) でソースビルドします（nvcc 不要）"
    # 依存プリフライト（長いビルド前に不足を検知）: Boost（boost::compute）と OpenCL ICD ローダ。
    _missing=()
    if ! ls /usr/include/boost/version.hpp >/dev/null 2>&1; then
        _missing+=("libboost-dev" "libboost-system-dev" "libboost-filesystem-dev")
    fi
    if ! ldconfig -p 2>/dev/null | grep -q "libOpenCL.so"; then
        _missing+=("ocl-icd-opencl-dev")
    fi
    if [[ ${#_missing[@]} -gt 0 ]]; then
        echo "[build_lightgbm_gpu] 依存が不足しています。先にインストールしてください:"
        echo "    sudo apt-get install -y ${_missing[*]}"
        exit 1
    fi
    CMAKE_DEF="cmake.define.USE_GPU=ON"
else
    # --- CUDA 依存（nvcc が gcc<=13 を要求）--------------------------------
    # gcc-15 では nvcc が失敗するため、gcc-13 を入れて CUDA ホストコンパイラに指定する:
    #   sudo apt-get install -y gcc-13 g++-13
    #   export CUDAHOSTCXX=g++-13  CC=gcc-13 CXX=g++-13
    echo "[build_lightgbm_gpu] CUDA(USE_CUDA=ON) でソースビルドします（要 gcc-13 / CUDAHOSTCXX）"
    if [[ -z "${CUDAHOSTCXX:-}" ]] && command -v g++-13 >/dev/null 2>&1; then
        export CUDAHOSTCXX=g++-13
        echo "[build_lightgbm_gpu] CUDAHOSTCXX=g++-13 を自動設定"
    fi
    CMAKE_DEF="cmake.define.USE_CUDA=ON"
fi

# 既存 CPU wheel を確実に置き換えるため no-binary でソースから再ビルドする。
pip install --no-binary lightgbm --force-reinstall --no-cache-dir \
    lightgbm --config-settings="${CMAKE_DEF}"

echo "[build_lightgbm_gpu] 完了。lightgbm=$(python -c 'import lightgbm; print(lightgbm.__version__)')"
echo "[build_lightgbm_gpu] 動作確認は上記コメントの python -c ワンライナーで（GPU が使われるか verbose=1 で確認）"
