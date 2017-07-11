#!/bin/bash

# ============================================================================

kapbase=kap-2.4.x
kap_remote=origin
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

git fetch ${kap_remote}
git checkout ${kap_remote}/$kapbase-hbase0.98
git format-patch -1

git checkout ${kap_remote}/$kapbase
git checkout -b tmp
git reset ${kap_remote}/$kapbase --hard

git am -3 --ignore-whitespace 0001-Support-HBase-0.98.patch
mvn clean install -DskipTests
if [[ "${push_remote}" == "true" ]]; then
    git push ${kap_remote} tmp:$kapbase-hbase0.98 -f
fi
rm 0001-Support-HBase-0.98.patch

# clean up
git checkout $kapbase-hbase0.98
git reset ${kap_remote}/$kapbase-hbase0.98 --hard

git checkout $kapbase
git reset ${kap_remote}/$kapbase --hard

git branch -D tmp
