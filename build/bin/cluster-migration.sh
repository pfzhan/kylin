#!/bin/bash

# Kyligence Inc. License

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

## ${dir} assigned to $KYLIN_HOME/bin in header.sh
source ${dir}/find-hadoop-conf-dir.sh

if [ -z "${kylin_hadoop_conf_dir}" ]; then
    hadoop_conf_param=
else
    hadoop_conf_param="--config ${kylin_hadoop_conf_dir}"
fi

current_working_dir=`$KYLIN_HOME/bin/get-properties.sh kylin.env.hdfs-working-dir`
meta_backup_dir="meta_for_migrate"
full_metadata_dir="${KYLIN_HOME}/meta_backups/${meta_backup_dir}"

if [ $# -eq 0 ]; then
  echo "Usage : cluster-migration.sh backup"
  echo "Usage : cluster-migration.sh backup-cube cubeName metadataOnly"
  echo "Usage : cluster-migration.sh restore hdfs://namenode/kylin_working_dir"
  echo "Usage : cluster-migration.sh restore-cube project cubeName hdfs://namenode/kylin_working_dir"
  exit 0
fi

if [ "$1" == "backup" ]
then
    mkdir -p ${KYLIN_HOME}/meta_backups
    echo "Ready to backup KAP metadata to local dir: ${full_metadata_dir}"
    mkdir -p ${full_metadata_dir}

    rm -rf ${full_metadata_dir}
    hadoop ${hadoop_conf_param} fs -rm -r ${current_working_dir}/${meta_backup_dir}
    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool download ${full_metadata_dir}
    [[ $? == 0 ]] || quit "Failed to dump KAP metadata."
    echo "KAP metadata is dumped to file: ${full_metadata_dir}."
    hadoop ${hadoop_conf_param} fs -put -f ${full_metadata_dir} ${current_working_dir} || quit "Failed to put ${full_metadata_dir} to ${current_working_dir}"
    echo "KAP metadata is put to HDFS and ready to distcp to remote Hadoop Cluster."
fi

if [ "$1" == "backup-cube" ]
then
    ${dir}/kylin.sh io.kyligence.kap.tool.release.KapCubeMigrationCLI backup "$2" "$3"
fi

if [ "$1" == "restore-cube" ]
then
    ${dir}/kylin.sh io.kyligence.kap.tool.release.KapCubeMigrationCLI restore "$2" "$3" "$4" "$5"
fi

if [ "$1" == "restore" ]
then
    dist_working_dir="$2"
    echo "Distcp KAP data from remote Hadoop Cluster."
    rm -rf ${full_metadata_dir}
    hadoop ${hadoop_conf_param} fs -rm -r ${current_working_dir}/${meta_backup_dir}
    hadoop ${hadoop_conf_param} distcp ${dist_working_dir}/* hdfs:///${current_working_dir}
    mkdir -p ${KYLIN_HOME}/meta_backups
    hadoop ${hadoop_conf_param} fs -get ${current_working_dir}/${meta_backup_dir} ${full_metadata_dir} || quit "ERROR: failed to get ${hdfs_full_file} from HDFS"
    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool upload ${full_metadata_dir}
    rm -rf ${full_metadata_dir}
    hadoop ${hadoop_conf_param} fs -rm -r ${current_working_dir}/${meta_backup_dir}
fi