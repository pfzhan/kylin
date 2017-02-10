#!/usr/bin/env bash

# check if we shipped hadoop/hbase/hive jars
HADOOP_PREFIX=("org.apache.hadoop." "org.apache.hbase." "org.apache.hive.")
CHK_DIRS=("lib" "tool" "tomcat/WEB-INF/lib" "tomcat/WEB-INF/classes")

for prefix in $HADOOP_PREFIX; do
    for dir in $CHK_DIRS; do
        echo "Check if we shipped class with prefix \"$prefix\" in \"$dir\"".
        if [ ! -d "$dir" ]; then
            echo "Dir not exists"
            continue
        fi

        grep -R $KYLIN_HOME/$prefix $dir
        if [ "$?" = "0" ]; then
            echo "========================================"
            echo "We should not ship classes/jars of hadoop/hbase/hive, but found one."
            echo "Please check the console output above."
            echo "========================================"
            exit 1
        fi
    done
done

echo "========================================"
echo "Confirmed: We actually did not shipped any hadoop/hbase/hive jars and classes."

