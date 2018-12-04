#!/bin/bash
# Kyligence Inc. License


dir=$(dirname ${0})
cd "${dir}"

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

# start command
if [ "$1" == "start" ]
then

    export KYLIN_HOME=../
    export SPARK_HOME=../spark

    if [ -f "../pid" ]
    then
        PID=`cat ../pid`
	if ps -p $PID > /dev/null
        then
          quit "Kylin is running, stop it first, PID is $PID"
        fi
    fi

    cd ../server

    mkdir -p ../logs
    mkdir -p ../hadoop_conf

    cp -rf /etc/hadoop/conf/* ../hadoop_conf
    cp -rf /etc/hive/conf/hive-site.xml ../hadoop_conf

    port=7070
    java -Dkylin.hadoop.conf.dir=../hadoop_conf -Dhdp.version=current -Dserver.port=$port -Dloader.path=../hadoop_conf  -jar newten.jar PROD >> ../logs/kylin.out 2>&1 & echo $! > ../pid &

    echo "Kylin is starting. Please checkout http://localhost:$port/kylin/index.html"



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
           quit "Kylin is not running" 0
        fi

    else
        quit "Kylin is not running" 0
    fi

else
    quit "Usage: 'newten.sh [-v] start' or 'newten.sh [-v] stop'"
fi