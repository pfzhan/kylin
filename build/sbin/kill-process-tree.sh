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

function help_func {
    echo "Usage: kill-process-tree.sh <PID>(process id)"
    echo "       kill-process-tree.sh 12345"
    exit 1
}

function isRunning() {
    [[ -n "$(ps -p $1 -o pid=)" ]]
}

function killTree() {
    local parent=$1 child
    for child in $(ps ax -o ppid= -o pid= | awk "\$1==$parent {print \$2}"); do
        killTree ${child}
    done
    echo "begin to kill pid: ${parent}"
    kill ${parent}
    if isRunning ${parent}; then
        sleep 5
        if isRunning ${parent}; then
            echo "begin to kill -9 pid: ${parent}"
            kill -9 ${parent}
        fi
    fi
}

# Check parameters count.
if [[ $# -ne 1 ]]; then
    help_func
fi

# Check whether it contains non-digit characters.
# Remove all digit characters and check for length.
# If there's length it's not a number.
if [[ -n ${1//[0-9]/} ]]; then
    help_func
fi

killTree $@