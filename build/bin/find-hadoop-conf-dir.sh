#!/bin/bash
# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

if [[ "$kylin_hadoop_conf_dir" == "" ]]
then

    verbose Retrieving hadoop config dir...
    
    if [[ $CI_MODE == 'true' ]]; then
        override_hadoop_conf_dir=`${KYLIN_HOME}/build/bin/get-properties.sh kylin.env.hadoop-conf-dir`
    else
        override_hadoop_conf_dir=`${KYLIN_HOME}/bin/get-properties.sh kylin.env.hadoop-conf-dir`
    fi
    
    if [ -n "$override_hadoop_conf_dir" ]; then
        verbose "$override_hadoop_conf_dir is override as the kylin_hadoop_conf_dir"
        export kylin_hadoop_conf_dir=$override_hadoop_conf_dir
        return
    fi
    
    hbase_classpath=`hbase classpath`
    
    arr=(`echo $hbase_classpath | cut -d ":" -f 1- | sed 's/:/ /g'`)
    kylin_hadoop_conf_dir=
    
    for data in ${arr[@]}
    do
        result=`echo $data | grep -v -E ".*jar"`
        if [ $result ]
        then
            valid_conf_dir=true
            
            if [ ! -f $result/yarn-site.xml ]
            then
                verbose "$result is not valid hadoop dir conf because yarn-site.xml is missing"
                valid_conf_dir=false
                continue
            fi
            
            if [ ! -f $result/mapred-site.xml ]
            then
                verbose "$result is not valid hadoop dir conf because mapred-site.xml is missing"
                valid_conf_dir=false
                continue
            fi
            
            if [ ! -f $result/hdfs-site.xml ]
            then
                verbose "$result is not valid hadoop dir conf because hdfs-site.xml is missing"
                valid_conf_dir=false
                continue
            fi
            
            if [ ! -f $result/core-site.xml ]
            then
                verbose "$result is not valid hadoop dir conf because core-site.xml is missing"
                valid_conf_dir=false
                continue
            fi
            
            verbose "$result is chosen as the kylin_hadoop_conf_dir"
            export kylin_hadoop_conf_dir=$result
            return
        fi
    done

fi

