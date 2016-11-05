#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking OS commands..."

a=`lsb_release -a`                      || quit "ERROR: Command 'lsb_release -a' does not work. Please run on Linux OS."
[[ $a == *Mac* ]]                       && quit "ERROR: Mac is not supported."
[[ $a == *Ubuntu* ]]                    && echo "WARN: Ubuntu is not officially supported. Use at your own risk."

command -v hadoop                       || quit "ERROR: Command 'hadoop' is not accessible. Please check Hadoop client setup."
command -v hdfs                         || quit "ERROR: Command 'hdfs' is not accessible. Please check Hadoop client setup."
command -v yarn                         || quit "ERROR: Command 'yarn' is not accessible. Please check Hadoop client setup."
command -v hive                         || quit "ERROR: Command 'hive' is not accessible. Please check Hadoop client setup."
command -v hbase                        || quit "ERROR: Command 'hbase' is not accessible. Please check Hadoop client setup."
