#!/bin/bash
export release_version=2.0

dir=$(dirname ${0})
cd ${dir}/../..

echo "KAP Release Version: ${release_version}"
sh build/script/package.sh