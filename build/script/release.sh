#!/bin/bash
export release_version=2.0

dir=$(dirname ${0})
cd ${dir}/../..

sh build/script/package.sh