#!/bin/bash
# Kyligence Inc. License
#title=Checking Java Version

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking Java version..."

version=`$JAVA -version 2>&1` || quit "ERROR: Detect java version failed. Please set JAVA_HOME."

version=$( echo $version | awk -F '"' '/version/ {print $2}' )
echo "Java Version: $version"
second_version="$(echo ${version} | cut -d '.' -f2)"

if [[ "$second_version" -lt "8" ]]; then
    quit "ERROR: The current Java version is not suitable for Kyligence Enterprise"
fi
