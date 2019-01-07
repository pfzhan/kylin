# Write SSB data into Hive
export HADOOP_STREAMING_JAR=${KYLIN_HOME}/ssb-kylin/lib/hadoop-streaming.jar

echo "Preparing SSB data..."

# set scale factor to 0.01 to generate 60,000 rows of data
${KYLIN_HOME}/ssb-kylin/bin/run.sh --scale 0.01

if [ $? -eq 0 ]; then
    echo "SSB data has been prepared!"
else
    echo "Failed to prepare SSB data."
fi
