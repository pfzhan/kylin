#!/bin/bash

# ============================================================================

kapbase=master
kylinbase=master
kap_remote=origin
kylin_remote=apache
push_remote=true

# ============================================================================

while [[ $# -ge 1 ]]
do
    key="$1"
    case $key in
        -l|--local)
            push_remote=false
            echo "turn off remote push"
            ;;
        -a|--kap_remote)
            kap_remote="$2"
            echo "kap remote name: ${kap_remote}"
            shift
            ;;
        -k|--kylin_remote)
            kylin_remote="$2"
            echo "kylin remote name: ${kylin_remote}"
            shift
            ;;
        *)
            ;;

    esac
    shift
done

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
git fetch ${kap_remote}
git checkout ${kap_remote}/$kapbase-hbase0.98
git format-patch -1
cd kylin
git fetch ${kylin_remote}
git checkout ${kylin_remote}/$kylinbase-hbase0.98
git format-patch -1
cd ..

# switch to base branches
git checkout ${kap_remote}/$kapbase
git checkout -b tmp
git reset ${kap_remote}/$kapbase --hard
cd kylin
git checkout ${kylin_remote}/$kylinbase
git checkout -b tmp
git reset ${kylin_remote}/$kylinbase --hard
cd ..

# apply hbase patch
cd kylin
git am -3 --ignore-whitespace 0001-KYLIN-2307-Create-a-branch-for-master-with-HBase-0.9.patch
cd ..
if git am -3 --ignore-whitespace 0001-Support-HBase-0.98.patch; then
    echo git am 0001-Support-HBase-0.98.patch was successful
else
    git add kylin && git am --continue
fi
mvn clean compile -DskipTests
git add kylin
git commit --amend --no-edit
if [[ "${push_remote}" == "true" ]]; then
    git push ${kap_remote} tmp:$kapbase-hbase0.98 -f
fi
cd kylin
if [[ "${push_remote}" == "true" ]]; then
    git push ${kylin_remote} tmp:$kylinbase-hbase0.98 -f
fi
cd ..
rm 0001-Support-HBase-0.98.patch
rm kylin/0001-KYLIN-2307-Create-a-branch-for-master-with-HBase-0.9.patch

# clean up
git checkout $kapbase
git reset ${kap_remote}/$kapbase --hard
git branch -D tmp
cd kylin
git checkout $kylinbase
git reset ${kylin_remote}/$kylinbase --hard
git branch -D tmp
cd ..
