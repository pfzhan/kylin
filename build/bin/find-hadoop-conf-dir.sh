#!/bin/bash
# Kyligence Inc. License

# source me

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

if [[ "$kylin_hadoop_conf_dir" == "" ]]
then

### Retrieve and export the system properties that involved by hadoop, it doesn't support CI_MODE now.

    if [[ $CI_MODE != 'true' ]]; then
        verbose Retrieving hadoop java opts...
        old_hadoop_cp=${HADOOP_CLASSPATH}
        export HADOOP_CLASSPATH=`ls ${KYLIN_HOME}/tool/kap-tool-*.jar`
        extra_system_props=`$JAVA -cp ${HADOOP_CLASSPATH} io.kyligence.kap.tool.DumpHadoopSystemProps 'HADOOP_CONF_DIR HADOOP_CLASSPATH PATH CLASSPATH java.runtime.name java.vm.name'`  || quit "Failed to run io.kyligence.kap.tool.DumpHadoopSystemProps"
        export HADOOP_CLASSPATH=${old_hadoop_cp}
        verbose "The extra system properties that involved by Hadoop are: `cat ${extra_system_props}`"
        source ${extra_system_props}
        verbose "kylin_hadoop_opts is ${kylin_hadoop_opts}"
        java_lib_path=`echo ${kylin_hadoop_opts} | sed -E -n "s/(.*)-Djava.library.path=([^[:space:]]*)(.*)/\2/"p`
        export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${java_lib_path}
        verbose "LD_LIBRARY_PATH is ${LD_LIBRARY_PATH}"
    fi

    verbose Retrieving hadoop config dir...
    
    if [[ $CI_MODE == 'true' ]]; then
        override_hadoop_conf_dir=`${KYLIN_HOME}/build/bin/get-properties.sh kylin.env.hadoop-conf-dir`
    else
        override_hadoop_conf_dir=`${KYLIN_HOME}/bin/get-properties.sh kylin.env.hadoop-conf-dir`
    fi
    
    if [ -n "$override_hadoop_conf_dir" ]; then
        verbose "kylin_hadoop_conf_dir is override as $override_hadoop_conf_dir"
        export kylin_hadoop_conf_dir=$override_hadoop_conf_dir
        return
    fi
    
    hadoop_classpath=`hadoop classpath`
    
    arr=(`echo $hadoop_classpath | cut -d ":" -f 1- | sed 's/:/ /g'`)
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

# FusionInsight's hadoop conf dir doesn't contains this file
#            if [ ! -f $result/mapred-site.xml ]
#            then
#                verbose "$result is not valid hadoop dir conf because mapred-site.xml is missing"
#                valid_conf_dir=false
#                continue
#            fi
            
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
            
            verbose "kylin_hadoop_conf_dir is $result"
            export kylin_hadoop_conf_dir=$result
            return
        fi
    done
fi

