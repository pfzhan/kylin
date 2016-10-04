#!/bin/bash

# ============================================================================

kapbase=master
kylinbase=yang21

# ============================================================================

set -o pipefail  # trace ERR through pipes
set -o errtrace  # trace ERR through 'time command' and other functions
function error() {
	SCRIPT="$0"           # script name
	LASTLINE="$1"         # line of error occurrence
	LASTERR="$2"          # error code
	echo "ERROR exit from ${SCRIPT} : line ${LASTLINE} with exit code ${LASTERR}"
	exit 1
}
trap 'error ${LINENO} ${?}' ERR

# ============================================================================

# get patches
git fetch origin
git checkout origin/$kapbase-hbase1.x
git format-patch -1
git checkout origin/$kapbase-cdh5.7
git format-patch -1
cd kylin
git fetch apache
git checkout apache/$kylinbase-hbase1.x
git format-patch -1
git checkout apache/$kylinbase-cdh5.7
git format-patch -1
cd ..

# switch to base branches
git checkout origin/$kapbase
git checkout -b tmp
git reset origin/$kapbase --hard
cd kylin
git checkout apache/$kylinbase
git checkout -b tmp
git reset apache/$kylinbase --hard
cd ..

# apply hbase patch
cd kylin
git am -3 --ignore-whitespace 0001-KYLIN-1528-Create-a-branch-for-v1.5-with-HBase-1.x-A.patch
cd ..
git am -3 --ignore-whitespace 0001-Support-HBase-1.x.patch
mvn clean compile -DskipTests
git add kylin
git commit --amend --no-edit
git push origin tmp:$kapbase-hbase1.x -f
cd kylin
git push apache tmp:$kylinbase-hbase1.x -f
cd ..
rm 0001-Support-HBase-1.x.patch
rm kylin/0001-KYLIN-1528-Create-a-branch-for-v1.5-with-HBase-1.x-A.patch

# apply cdh patch
cd kylin
git am -3 --ignore-whitespace 0001-KYLIN-1672-support-kylin-on-cdh-5.7.patch
cd ..
git am -3 --ignore-whitespace 0001-Support-CDH-5.7.patch
mvn clean compile -DskipTests
git add kylin
git commit --amend --no-edit
git push origin tmp:$kapbase-cdh5.7 -f
cd kylin
git push apache tmp:$kylinbase-cdh5.7 -f
cd ..
rm 0001-Support-CDH-5.7.patch
rm kylin/0001-KYLIN-1672-support-kylin-on-cdh-5.7.patch

# clean up
git checkout $kapbase
git reset origin/$kapbase --hard
git branch -D tmp
cd kylin
git checkout $kylinbase
git reset apache/$kylinbase --hard
git branch -D tmp
cd ..
