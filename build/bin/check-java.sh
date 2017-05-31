#!/bin/bash
# Kyligence Inc. License
#title=Checking Java Version

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

echo "Checking Java version..."

if type -p java; then
    _java=java
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then     
    _java="$JAVA_HOME/bin/java"
else
    quit "ERROR: Failed to find Java, please set JAVA_HOME"
fi

if [[ "$_java" ]]; then
    version=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    echo "Java Version: $version"
    if [[ "$version" < "1.7" ]]; then         
        quit "ERROR: The current Java version is not suitable for KAP"
    fi
fi
