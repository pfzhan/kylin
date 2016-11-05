#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

a=`lsb_release -a`                      || quit "ERROR: Command 'lsb_release -a' does not work. Please run on Linux OS."
[[ $a == *Mac* ]]                       && quit "ERROR: Mac is not supported."
[[ $a == *Ubuntu* ]]                    && echo "WARN: Ubuntu is not officially supported. Use at your own risk."

command -v hadoop  >/dev/null 2>&1      || quit "ERROR: Command 'hadoop' is not accessible. Please check permissions."
command -v hdfs    >/dev/null 2>&1      || quit "ERROR: Command 'hdfs' is not accessible. Please check permissions."
command -v yarn    >/dev/null 2>&1      || quit "ERROR: Command 'yarn' is not accessible. Please check permissions."
command -v hive    >/dev/null 2>&1      || quit "ERROR: Command 'hive' is not accessible. Please check permissions."
command -v hbase   >/dev/null 2>&1      || quit "ERROR: Command 'hbase' is not accessible. Please check permissions."
