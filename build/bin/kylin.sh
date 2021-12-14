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

function help() {
    echo "Usage: kylin.sh <COMMAND>"
    echo
    echo "Commands:"
    echo "  -D                    -D[ skipCheck ]"
    echo "                        skip some check when bootstrap, eg. bash kylin.sh -DskipCheck start"
    echo "  -C                    -C[ true | false ], default true, use the local properties or not"
    echo "  start                 start ke"
    echo "  restart               restart ke"
    echo "  stop                  stop ke"
    echo "  io.kyligence.*        run tool"
    echo "  interactive Enter for bootstrap"
}

function parseSkipCheckArgs() {
  case $1 in
    "skipCheck")
      skipValue=1
      ;;
    *)
      echo "Invalid option -D: -$1" >&2
      exit 1
      ;;
  esac
  echo ${skipValue}
}

function checkArguments() {
   local local_properties="false"
   #enabled when check env by pass
   if [[ -f ${KYLIN_HOME}/bin/check-env-bypass ]]; then
     local_properties="true"
   fi
   case $1 in
       "start" | "restart")
             export _KYLIN_GET_PROPERTIES_FROM_LOCAL=${_KYLIN_GET_PROPERTIES_FROM_LOCAL:-"${local_properties}"}
             exportPropertiesToFile
             ${KYLIN_HOME}/sbin/rotate-logs.sh "$@"
             export KYLIN_SKIP_ROTATE_LOG=1
             ;;
       "stop" )
             export _KYLIN_GET_PROPERTIES_FROM_LOCAL=${_KYLIN_GET_PROPERTIES_FROM_LOCAL:-"${local_properties}"}
             ;;
       "spawn" )
             export _KYLIN_GET_PROPERTIES_FROM_LOCAL=${_KYLIN_GET_PROPERTIES_FROM_LOCAL:-"${local_properties}"}
             ${KYLIN_HOME}/sbin/rotate-logs.sh "$@"
             export KYLIN_SKIP_ROTATE_LOG=1
             ;;
       *)
             ;;
   esac
}

function main() {
  # parsed arguments
  while getopts "vD:C:h" opt; do
    case ${opt} in
      v)
        export verbose=true
        ;;
      D)
        skipArgs=`parseSkipCheckArgs "$OPTARG"` || exit 1
        export KYLIN_SKIP_CHECK=${skipArgs}
        ;;
      C)
        export _KYLIN_GET_PROPERTIES_FROM_LOCAL="$OPTARG";;
      h)
        help
        exit 0;;
      *)
        echo "Invalid option: -$OPTARG" && exit 1
    esac
  done
  shift $((OPTIND-1))

  # init
  source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/../sbin/header.sh "$@"
  mkdir -p ${KYLIN_HOME}/logs
  ERR_LOG=${KYLIN_HOME}/logs/shell.stderr
  OUT_LOG=${KYLIN_HOME}/logs/shell.stdout

  # check action arguments
  checkArguments "$@"

  echo "-----------------------  log start  -----------------------" >>${ERR_LOG}
  echo "-----------------------  log start  -----------------------" >>${OUT_LOG}
  bash -x ${KYLIN_HOME}/sbin/bootstrap.sh "$@" 2>>${ERR_LOG}  | tee -a ${OUT_LOG}
  ret=${PIPESTATUS[0]}
  echo "-----------------------  log end  -------------------------" >>${ERR_LOG}
  echo "-----------------------  log end  -------------------------" >>${OUT_LOG}
  exit ${ret}
}

main "$@"

