#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking OS commands..."

command -v lsb_release                  || echo "${CHECKENV_REPORT_PFX}WARN: Command lsb_release is not accessible. Please run on Linux OS."
a=`lsb_release -a`                      || echo "${CHECKENV_REPORT_PFX}WARN: Command 'lsb_release -a' does not work. Please run on Linux OS."
[[ $a == *Mac* ]]                       && echo "${CHECKENV_REPORT_PFX}WARN: Mac is not officially supported. Use at your own risk."
[[ $a == *Ubuntu* ]]                    && echo "${CHECKENV_REPORT_PFX}WARN: Ubuntu is not officially supported. Use at your own risk."

command -v hadoop                       || quit "ERROR: Command 'hadoop' is not accessible. Please check Hadoop client setup."
command -v hdfs                         || quit "ERROR: Command 'hdfs' is not accessible. Please check Hadoop client setup."
command -v yarn                         || quit "ERROR: Command 'yarn' is not accessible. Please check Hadoop client setup."
command -v hive                         || quit "ERROR: Command 'hive' is not accessible. Please check Hadoop client setup."
command -v hbase                        || quit "ERROR: Command 'hbase' is not accessible. Please check Hadoop client setup."
