#!/bin/bash
# Kyligence Inc. License

#prepare kap in FunsionInsight C70
if [ -z "$BIGDATA_CLIENT_HOME" ]
then
    return
fi

#check kylin home
if [ -z "$KYLIN_HOME" ]
then
    echo 'Please make sure KYLIN_HOME has been set'
    exit 1
else
    echo "KYLIN_HOME is set to ${KYLIN_HOME}"
fi

## Avoid replacing jars for spark twice
BYPASS=$KYLIN_HOME/spark/jars/replace-jars-bypass
if [[ ! -f ${BYPASS} ]]
then
    rm -rf $KYLIN_HOME/spark/jars/htrace*.jar
    cp -rf $BIGDATA_CLIENT_HOME/HBase/hbase/lib/htrace-core-*-incubating.jar $KYLIN_HOME/spark/jars/

    spark_old_jars=`find $KYLIN_HOME/spark/jars/ -name hadoop-*2.6.*.jar`
    for x in ${spark_old_jars[*]}
    do
        rm -rf $x
    done

    hadoop_jars_path=`find $BIGDATA_CLIENT_HOME/HBase/hbase -name hadoop*.jar | grep hadoop`
    for x in ${hadoop_jars_path[*]}
    do
        cp -rf $x $KYLIN_HOME/spark/jars/
    done
fi


distcp_jar_path=`find $BIGDATA_CLIENT_HOME | grep hadoop-distcp`
for x in ${distcp_jar_path[*]}
do
    cp -rf $x $KYLIN_HOME/ext
done


echo 'Finish prepare deploy package.'