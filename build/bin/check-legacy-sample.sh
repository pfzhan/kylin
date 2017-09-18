#!/bin/bash
# Kyligence Inc. License
#title=Checking Legacy Sample Cubes

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh

mkdir -p ${KYLIN_HOME}/meta_backups
_file="${KYLIN_HOME}/meta_backups/meta_legacy"

rm -rf ${_file}

echo "Starting backup to ${_file}"
mkdir -p ${_file}
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool fetch $_file cube_desc
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool fetch $_file cube_instance
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool fetch $_file raw_table_desc
${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool fetch $_file raw_table_instance
echo "metadata store backed up to ${_file}"

cd ${KYLIN_HOME}/meta_backups/meta_legacy/cube_desc
sample_cube_name=`ls | grep kylin_sales_cube.json`
if [ -n "$sample_cube_name" ] && [ "$sample_cube_name"="kylin_sales_cube.json" -a -z `awk '/version/{print}' $sample_cube_name` ]; then
	cd ${KYLIN_HOME}/meta_backups/meta_legacy
	rm -f ${KYLIN_HOME}/meta_backups/meta_legacy/cube*/kylin_sales_cube.json
	rm -f ${KYLIN_HOME}/meta_backups/meta_legacy/raw_table*/kylin_sales_cube.json
	${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool remove cube_desc
	${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool remove cube_instance
	${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool remove raw_table_desc
	${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.common.persistence.ResourceTool remove raw_table_instance
	bash ${KYLIN_HOME}/bin/metastore.sh restore ${KYLIN_HOME}/meta_backups/meta_legacy
fi

rm -rf ${_file}