#!/bin/bash
# Kyligence Inc. License

function verbose() {
    (echo `date '+%F %H:%M:%S'` $@ | tee -a $diag_log_file)
}

function help() {
    echo "Example usage:"
    echo "  diagnosis.sh -full [<START_TIMESTAMP> <END_TIMESTAMP>] [-destDir <DESTINATION_DIR>]"
    echo "  diagnosis.sh -job <JOB_ID> [-destDir <DESTINATION_DIR>]"
    exit 1
}

if [ -z $KYLIN_HOME ];then
    export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi

if [[ $# -lt 1 ]]; then
    help
fi

# create temp folder
diag_tmp_dir=$(cd "/tmp" && cd "$(mktemp -d "diag.tmp.XXXXX")" && pwd)
mkdir $diag_tmp_dir/logs
mkdir $diag_tmp_dir/spark_logs
mkdir $diag_tmp_dir/metadata
mkdir $diag_tmp_dir/conf
mkdir $diag_tmp_dir/hadoop_conf

diag_log_file=$diag_tmp_dir/diag.log
exec 2>>$diag_log_file

case $1 in
    "-full")
        if [[ $# -eq 5 ]]; then
            start_time=$[$2 / 1000]
            end_time=$[$3 / 1000]
            diag_pkg_home=$5
        elif [[ $2 == "-destDir" ]]; then
            start_time=`date +%s -d "-1 day"`
            end_time=`date +%s`
            diag_pkg_home=$3
        elif [[ $# -eq 3 ]]; then
            start_time=$[$2 / 1000]
            end_time=$[$3 / 1000]
        else
            start_time=`date +%s -d "-1 day"`
            end_time=`date +%s`
        fi
        extract_log_args="-startTime $[$start_time * 1000] -endTime $[$end_time * 1000]"
        verbose "Start build full diagnosis package"
        ;;
    "-job")
        job_id=$2
        if [[ $3 == "-destDir" ]]; then
            diag_pkg_home=$4
        fi
        extract_log_args="-job $job_id"
        verbose "Start build job diagnosis package"
        ;;
    *) help ;;
esac

# extract kylin.log
verbose "Extract kylin.log..."
log_path=$KYLIN_HOME/logs
log_files=$(ls -t $log_path | grep "kylin.log")
for log_file in $log_files; do
    last_modified=$(date -r $log_path/$log_file "+%s")
    if [[ $1 == "-full" ]] && [[ $last_modified -lt $start_time ]]; then
        break
    fi

    $KYLIN_HOME/bin/log-extract-tool.sh $log_path/$log_file $extract_log_args 2>>${diag_log_file} 1>>$diag_tmp_dir/logs/$log_file
    if [[ $? -eq 0 ]]; then
        verbose "=> extract [$log_file] log content successful"
    fi
done

# extract spark logs
## get hdfs dir
verbose "Extract spark logs..."
hdfs_working_dir=`$KYLIN_HOME/bin/get-properties.sh kylin.env.hdfs-working-dir`
metadata_url=`$KYLIN_HOME/bin/get-properties.sh kylin.metadata.url`
spark_logs_path=${hdfs_working_dir}/${metadata_url}

## check whether it contain '@' mark,if it exists,extract the content before it
mark=`echo ${spark_logs_path} | grep "@"`
if [ ${#mark} -ne 0 ]
then
    spark_logs_path=`echo ${spark_logs_path} | awk -F'@' '{print $1}'`
fi

if [[ $1 == "-full" ]]; then
    if [[ -f $KYLIN_HOME/appid ]]; then
        start_date=`date -d "@$start_time" "+%F"`
        end_date=`date -d "@$end_time" "+%F"`
        appid=`cat $KYLIN_HOME/appid`
        sparder_logs_path=$spark_logs_path/sparder_logs
        for sparder_log_file in $(hadoop fs -ls -d "$sparder_logs_path/*/*" | awk '{print $8}'); do
            log_appid=$(echo $sparder_log_file | awk -F "/" '{print $NF}')
            log_date=$(echo $sparder_log_file | awk -F "/" '{print $(NF-1)}')
            if [[ $log_appid == $appid ]] && [[ $log_date > $start_date || $log_date == $start_date ]] && [[ $log_date < $end_date || $log_date == $end_date ]]; then
                if [[ ! -d ${diag_tmp_dir}/spark_logs/$log_date ]]; then
                    mkdir ${diag_tmp_dir}/spark_logs/$log_date
                fi
                hadoop fs -copyToLocal $sparder_log_file ${diag_tmp_dir}/spark_logs/$log_date/
                if [[ $? == 0 ]]; then
                    verbose "=> extract [$sparder_log_file] spark log successful"
                fi
            fi
        done
    else
        verbose "=> can not found appid"
    fi

elif [[ $1 == "-job" ]]; then
    spark_job_logs_path=$spark_logs_path/*/spark_logs
    for job_log_file in $(hadoop fs -ls -d "$spark_job_logs_path/*/*" | awk '{print $8}'); do
        log_job_id=$(echo $job_log_file | awk -F "/" '{print $NF}')
        if [[ $log_job_id == $job_id ]]; then
            if [[ ! -d ${diag_tmp_dir}/spark_logs/job ]]; then
                mkdir ${diag_tmp_dir}/spark_logs/job
            fi
            hadoop fs -copyToLocal $job_log_file ${diag_tmp_dir}/spark_logs/job/
            if [[ $? == 0 ]]; then
                verbose "=> extract [$job_log_file] spark log successful"
            fi
            break;
        fi
    done
fi

# dump metadata
verbose "Dump metadata..."
bash ${KYLIN_HOME}/bin/metastore.sh backup ${diag_tmp_dir} 2>>${diag_log_file}
if [[ $? == 0 ]]; then
    mv -f ${diag_tmp_dir}/*_backup/* ${diag_tmp_dir}/metadata/ && rm -rf ${diag_tmp_dir}/*_backup
else
    verbose "=> backup metadata failed"
fi

# copy conf
verbose "Copy kylin conf"
cp -rf ${KYLIN_HOME}/conf/* ${diag_tmp_dir}/conf/

# copy hadoop conf
verbose "Copy hadoop conf"
cp -rf ${KYLIN_HOME}/hadoop_conf/* ${diag_tmp_dir}/hadoop_conf/

# package
if [[ -z $diag_pkg_home ]]; then
    diag_pkg_home="${KYLIN_HOME}/diagnosis_package"
fi
if [[ ! -d $diag_pkg_home ]]; then
    mkdir $diag_pkg_home
fi
diag_pkg_home=$(cd -P $diag_pkg_home && pwd -P)
diag_package="diag_$(date '+%Y_%m_%d_%H_%M_%S')"
verbose "Packaging, build diagnosis package in [${diag_pkg_home}/${diag_package}.tar.gz]"
(cd ${diag_tmp_dir} && mkdir $diag_package && cp -rf "metadata" "logs" "spark_logs" "conf" "hadoop_conf" "diag.log" $diag_package \
    && tar -zcf "${diag_package}.tar.gz" $diag_package \
    && cp "${diag_package}.tar.gz" "${diag_pkg_home}/${diag_package}.tar.gz")

verbose "Build diagnosis package finished."
# delete tmp dir
if [[ -d $diag_tmp_dir ]]; then
    rm -rf $diag_tmp_dir
fi


