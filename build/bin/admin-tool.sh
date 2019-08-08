#!/bin/bash
# Kyligence Inc. License

RESTORE='\033[0m'
YELLOW='\033[00;33m'

if [[ -z $KYLIN_HOME ]];then
    export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi

function help {
    echo "Usage: 'admin-tool.sh admin-password-reset'"
    return 1
}

function printAdminPasswordResetResult() {
    error=$1

    if [[ $error == 0 ]]; then
        echo -e "${YELLOW}Reset the ADMIN password successfully.${RESTORE}"
    else
        echo -e "${YELLOW}Reset the ADMIN password failed, for more details please refer to \"\$KYLIN_HOME/logs/shell.stderr\".${RESTORE}"
    fi
}

ret=0
if [[ "$1" == "admin-password-reset" ]]; then
    ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.security.KapPasswordResetCLI
    ret=$?
    printAdminPasswordResetResult ${ret}
else
    help
    ret=$?
fi

exit ${ret}
