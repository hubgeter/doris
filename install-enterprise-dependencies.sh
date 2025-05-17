#!/usr/bin/env bash
SELECTDB_JARS="selectdb-enterprise-2.1-zero-trust-jars-20250318-version"
TMP_DIR="/tmp/${SELECTDB_JARS}"

ZIP_PATH="/tmp/${SELECTDB_JARS}.zip"
if [[ ! -f "${ZIP_PATH}" ]]; then
    wget -O "${ZIP_PATH}" "https://qa-build.oss-cn-beijing.aliyuncs.com/zero-trust/${SELECTDB_JARS}.zip"
fi

rm -rf "${TMP_DIR}"
unzip -q "${ZIP_PATH}" -d "/tmp"
bash "${TMP_DIR}/install.sh"

rm -rf "${ZIP_PATH}" "${TMP_DIR}"