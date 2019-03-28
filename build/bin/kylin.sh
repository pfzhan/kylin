#!/bin/bash
# Kyligence Inc. License

alias cd='cd -P'
dir=$(dirname ${0})
cd "${dir}"
version=`cat ../VERSION | awk '{print $2}'`

# setup verbose
verbose=${verbose:-""}
while getopts ":v" opt; do
    case $opt in
        v)
            echo "Turn on verbose mode." >&2
            export verbose=true
            shift 1
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            ;;
    esac
done

function exportEnv {
    export KYLIN_HOME=`cd ../; pwd`
    export KYLIN_HADOOP_CONF=${KYLIN_HOME}/hadoop_conf
    export SPARK_HOME=${KYLIN_HOME}/spark

    verbose "KYLIN_HOME is:${KYLIN_HOME}"
    verbose "KYLIN_HADOOP_CONF is:${KYLIN_HADOOP_CONF}"
    verbose "SPARK_HOME is:${SPARK_HOME}"
}

function quit {
        echo "$@"
        if [[ -n "${QUIT_MESSAGE_LOG}" ]]; then
            echo `setColor 31 "$@"` >> ${QUIT_MESSAGE_LOG}
        fi
        if [ $# == 2 ]
        then
            exit $2
        else
            exit 1
        fi
    }

function verbose {
    if [[ -n "$verbose" ]]; then
        echo "$@"
    fi
}

function fetchHadoopConf() {
    export FI_ENV_PLATFORM=

    ## FusionInsight platform C60.
    if [ -n "$BIGDATA_HOME" ]
    then
        FI_ENV_PLATFORM=$BIGDATA_HOME
    fi

    ## FusionInsight platform C70.
    if [ -n "$BIGDATA_CLIENT_HOME" ]
    then
        FI_ENV_PLATFORM=$BIGDATA_CLIENT_HOME
    fi

    if [ -n "$FI_ENV_PLATFORM" ]
    then
        # FI platform
        cp -rf $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/core-site.xml ${KYLIN_HADOOP_CONF}
        cp -rf $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/hdfs-site.xml ${KYLIN_HADOOP_CONF}
        cp -rf $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/yarn-site.xml ${KYLIN_HADOOP_CONF}
        cp -rf $FI_ENV_PLATFORM/Hive/config/hive-site.xml ${KYLIN_HADOOP_CONF}
        cp -rf $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/mapred-site.xml ${KYLIN_HADOOP_CONF}

        # Spark need hive-site.xml in FI
        cp -rf $FI_ENV_PLATFORM/Hive/config/hive-site.xml ${SPARK_HOME}/conf

        # don't find topology.map in FI
        cp -rf $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/topology.py ${KYLIN_HADOOP_CONF}
        cp -rf $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/ssl-client.xml ${KYLIN_HADOOP_CONF}
        cp -rf $FI_ENV_PLATFORM/HDFS/hadoop/etc/hadoop/hadoop-env.sh ${KYLIN_HADOOP_CONF}
    else
        # CDH/HDP platform
        cp -rf /etc/hadoop/conf/core-site.xml ${KYLIN_HADOOP_CONF}
        cp -rf /etc/hadoop/conf/hdfs-site.xml ${KYLIN_HADOOP_CONF}
        cp -rf /etc/hadoop/conf/yarn-site.xml ${KYLIN_HADOOP_CONF}
        cp -rf /etc/hive/conf/hive-site.xml ${KYLIN_HADOOP_CONF}
        cp -rf /etc/hadoop/conf/mapred-site.xml ${KYLIN_HADOOP_CONF}

        cp -rf /etc/hadoop/conf/topology.py ${KYLIN_HADOOP_CONF}
        cp -rf /etc/hadoop/conf/topology.map ${KYLIN_HADOOP_CONF}
        cp -rf /etc/hadoop/conf/ssl-client.xml ${KYLIN_HADOOP_CONF}
        cp -rf /etc/hadoop/conf/hadoop-env.sh ${KYLIN_HADOOP_CONF}
    fi
    if [ -d ${KYLIN_HOME}/hadoop_conf_override ]
    then
        cp -rf ${KYLIN_HOME}/hadoop_conf_override/hive-site.xml ${KYLIN_HADOOP_CONF}
    fi
}

function runTool() {
    exportEnv
    fetchHadoopConf
    source ${KYLIN_HOME}/bin/replace-jars-under-spark.sh

    #retrive $KYLIN_EXTRA_START_OPTS
    if [ -f "${KYLIN_HOME}/conf/setenv.sh" ]; then
        source ${KYLIN_HOME}/conf/setenv.sh
        export KYLIN_EXTRA_START_OPTS=`echo ${KYLIN_JVM_SETTINGS}|sed  "s/-XX:+PrintFlagsFinal//g"`
    fi
    if [ "$SKIP_KERB" != "1" ]; then
        source ${KYLIN_HOME}/bin/init-kerberos.sh
        initKerberosIfNeeded
    fi
    java ${KYLIN_EXTRA_START_OPTS} -Dlog4j.configuration=file:${KYLIN_HOME}/conf/kylin-tools-log4j.properties -Dkylin.hadoop.conf.dir=${KYLIN_HADOOP_CONF} -Dhdp.version=current -cp "${KYLIN_HOME}/tool/kap-tool-$version.jar:${SPARK_HOME}/jars/*" $@
}

# start command
if [[ "$1" == io.kyligence.* ]]
then
    runTool "$@"
elif [ "$1" == "start" ]
then
    exportEnv

    if [ -f "../pid" ]
    then
        PID=`cat ../pid`
	if ps -p $PID > /dev/null
        then
          quit "Kylin is running, stop it first, PID is $PID"
        fi
    fi

    cd ${KYLIN_HOME}/server

    mkdir -p ${KYLIN_HOME}/logs
    mkdir -p ${KYLIN_HOME}/hadoop_conf
    fetchHadoopConf
    source ${KYLIN_HOME}/bin/init-kerberos.sh
    initKerberosIfNeeded
    source ${KYLIN_HOME}/bin/replace-jars-under-spark.sh

    port=7070
    used=`netstat -tpln | grep "\<$port\>" | awk '{print $7}' | sed "s/\// /g"`
    if [ ! -z "$used" ]; then
        echo "<$used> already listen on $port"
        exit -1
    fi

    #retrive $KYLIN_EXTRA_START_OPTS
    if [ -f "${KYLIN_HOME}/conf/setenv.sh" ]; then
        source ${KYLIN_HOME}/conf/setenv.sh
        export KYLIN_EXTRA_START_OPTS=`echo ${KYLIN_JVM_SETTINGS}|sed  "s/-XX:+PrintFlagsFinal//g"`
    fi

    java ${KYLIN_EXTRA_START_OPTS} -Dlogging.path=${KYLIN_HOME}/logs -Dlogging.config=file:${KYLIN_HOME}/conf/kylin-server-log4j.properties -Dkylin.hadoop.conf.dir=${KYLIN_HADOOP_CONF} -Dhdp.version=current -Dserver.port=$port -Dloader.path="${KYLIN_HADOOP_CONF},${KYLIN_HOME}/server/jars,${SPARK_HOME}/jars"  -jar newten.jar PROD >> ../logs/kylin.out 2>&1 & echo $! > ../pid &

    echo "Kylin is starting, PID:`cat ../pid`. Please checkout http://`hostname`:$port/kylin/index.html"

# stop command
elif [ "$1" == "stop" ]
then

    if [ -f "../pid" ]
    then
        PID=`cat ../pid`
        if ps -p $PID > /dev/null
        then
           echo "Stopping Kylin: $PID"
           kill $PID
           for i in {1..10}
           do
              sleep 3
              if ps -p $PID -f | grep kylin > /dev/null
              then
                 if [ "$i" == "10" ]
                 then
                    echo "Killing Kylin: $PID"
                    kill -9 $PID
                 fi
                 continue
              fi
              break
           done
           rm ../pid
           exit 0
        else
           quit "Kylin is not running"
        fi

    else
        quit "Kylin is not running"
    fi

else
    quit "Usage: 'kylin.sh [-v] start' or 'kylin.sh [-v] stop'"
fi