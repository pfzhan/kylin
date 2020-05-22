#!/bin/bash

python -V 2>&1 || quit "ERROR: Detect python version failed. Please have python 2.7.7 or above installed."

version=`python -V 2>&1`
version_num="$(echo ${version} | cut -d ' ' -f2)"
version_first_num="$(echo ${version_num} | cut -d '.' -f1)"
version_second_num="$(echo ${version_num} | cut -d '.' -f2)"
version_third_num="$(echo ${version_num} | cut -d '.' -f3)"

if [[ "$version_first_num" -lt "2" ]] || [[ "$version_second_num" -lt "7" ]] || [[ "$version_third_num" -lt "7" ]]; then
    echo "Error: python 2.7.7 or above is required for query history migration "
    exit 0
fi

# install required packages
python -m pip install influxdb
python -m pip install mysql-connector-python
python -m pip install psycopg2-binary
python -m pip install tzlocal

python query_history_upgrade.py $@