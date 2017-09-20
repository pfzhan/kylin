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
echo "metadata store backed up to ${_file}"

cd ${KYLIN_HOME}/meta_backups/meta_legacy/cube_desc
sample_cube_name=`ls | grep kylin_sales_cube.json`
if [ -n "$sample_cube_name" ] && [ "$sample_cube_name"="kylin_sales_cube.json" -a -z `awk '/version/{print}' $sample_cube_name` ]; then
	${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.release.KapCubeRemovalCLI kylin_sales_cube
fi

rm -rf ${_file}
