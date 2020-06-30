#!/bin/bash

##
## Copyright (C) 2020 Kyligence Inc. All rights reserved.
##
## http://kyligence.io
##
## This software is the confidential and proprietary information of
## Kyligence Inc. ("Confidential Information"). You shall not disclose
## such Confidential Information and shall use it only in accordance
## with the terms of the license agreement you entered into with
## Kyligence Inc.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
## "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
## LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
## A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
## OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
## SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
## LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
## DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
## THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
## (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
## OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
##

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

    if [[ $error != 0 ]]; then
        echo -e "${YELLOW}Reset the ADMIN password failed, for more details please refer to \"\$KYLIN_HOME/logs/shell.stderr\".${RESTORE}"
    fi
}

ret=0
if [[ "$1" == "admin-password-reset" ]]; then
    read -p "You are resetting the password of [ADMIN], please enter [Y/N] to continue.: " input
    if [[ ${input} != [Yy] ]]; then
        echo "Reset password of [ADMIN] failed."
        exit 1
    fi

    ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.tool.security.KapPasswordResetCLI
    ret=$?
    printAdminPasswordResetResult ${ret}
else
    help
    ret=$?
fi

exit ${ret}
