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

if [ -z $KYLIN_HOME ];then
    export KYLIN_HOME=$(cd -P -- "$(dirname -- "$0")"/../ && pwd -P)
fi

if [ -z ${kylin_hadoop_conf_dir} ]; then
    export kylin_hadoop_conf_dir=$KYLIN_HOME/hadoop_conf
fi
_KYLIN_CACHED_CONFIG_FILE=${_KYLIN_CACHED_CONFIG_FILE:-"${KYLIN_HOME}/conf/._kylin_properties_"}
GET_PROPERTIES_FROM_LOCAL=${_KYLIN_GET_PROPERTIES_FROM_LOCAL:-""}

function verboseLog() {
    echo `date '+%F %H:%M:%S'` "$@" >>${KYLIN_HOME}/logs/shell.stderr
}

function quit() {
    verboseLog "$@"
    exit 1
}

function help() {
    echo "Usage: get-properties <COMMAND>"
    echo
    echo "Commands:"
    echo "  -c        get properties from cache(local disk)"
    echo "  -r        get properties in real time, this options support backwards config"
    echo "  -e        [file_path]only export properties to cache(local disk)"
    echo "  -h        print help"
    echo "  interactive Enter to get kylin properties"
}

function runTool() {

    if [ -z ${kylin_hadoop_conf_dir} ]; then
        export kylin_hadoop_conf_dir=$KYLIN_HOME/hadoop_conf
    fi

    local KYLIN_KERBEROS_OPTS=""
    if [ -f ${KYLIN_HOME}/conf/krb5.conf ];then
        KYLIN_KERBEROS_OPTS="-Djava.security.krb5.conf=${KYLIN_HOME}/conf/krb5.conf"
    fi

    local SPARK_HOME=$KYLIN_HOME/spark

    local kylin_tools_log4j=""
    if [[ -f ${KYLIN_HOME}/conf/kylin-tools-log4j.xml ]]; then
        kylin_tools_log4j="file:${KYLIN_HOME}/conf/kylin-tools-log4j.xml"
        else
        kylin_tools_log4j="file:${KYLIN_HOME}/tool/conf/kylin-tools-log4j.xml"
    fi

    mkdir -p ${KYLIN_HOME}/logs
    local result=`java ${KYLIN_KERBEROS_OPTS} -Dlog4j.configurationFile=${kylin_tools_log4j} -Dkylin.hadoop.conf.dir=${kylin_hadoop_conf_dir} -Dhdp.version=current -cp "${kylin_hadoop_conf_dir}:${KYLIN_HOME}/lib/ext/*:${KYLIN_HOME}/server/jars/*:${SPARK_HOME}/jars/*" "$@" 2>>${KYLIN_HOME}/logs/shell.stderr`

    echo "$result"
}


function getPropertiesFromLocal() {
  if [ $# != 1 ]
  then
      echo 'invalid input from local'
      exit 1
  fi

 if [[ "${GET_PROPERTIES_FROM_LOCAL}" == "true" && -e ${_KYLIN_CACHED_CONFIG_FILE} ]]; then
   local inputP=$1
   if [[ ${inputP} = kap.* ]]; then
     quit "local properties dosen't support kap.*"
   elif [[ ${inputP} = *. ]]; then
     grep -F "${inputP}" ${_KYLIN_CACHED_CONFIG_FILE} | sed -e "s/${inputP}//g"
   else
     grep -F "${inputP}=" ${_KYLIN_CACHED_CONFIG_FILE} | cut -d "=" -f 2-
     verboseLog "getProperties ${inputP} from local success"
   fi
 else
   if [[ "${GET_PROPERTIES_FROM_LOCAL}" != "true" ]]; then
     verboseLog "Please turn _KYLIN_GET_PROPERTIES_FROM_LOCAL:${GET_PROPERTIES_FROM_LOCAL} to true."
   else
     verboseLog "Cannot find local cache file:${_KYLIN_CACHED_CONFIG_FILE}!"
   fi
   return 1
 fi

}

function getPropertiesFromKE() {
  if [ $# != 1 ]
  then
      if [[ $# -lt 2 || $2 != 'DEC' ]]
          then
              echo 'invalid input'
              exit 1
      fi
  fi
  runTool io.kyligence.kap.tool.KylinConfigCLI "$@"
}

#1.export properties to tmp file _KYLIN_CACHED_CONFIG_FILE".tmp"
#2.rename .tmp file
function exportPropertiesToLocal() {
  if [[ "${GET_PROPERTIES_FROM_LOCAL}" != "true" ]]; then
    quit "cannot export properties to local when _KYLIN_GET_PROPERTIES_FROM_LOCAL=false"
  fi

  local exportFile=${1:-${_KYLIN_CACHED_CONFIG_FILE}}

  local config_tmp_file="${exportFile}.tmp"
  if [[ -e ${config_tmp_file} ]]; then
      verboseLog "tmp file already exist, try to remove it. path:${config_tmp_file}"
      rm -f "${config_tmp_file}"
  fi

  local result=`runTool io.kyligence.kap.tool.KylinConfigExporterCLI "${config_tmp_file}"`
  verboseLog "${result:-"export success"}"

  if [[ $? -ne 0 || ! -e ${config_tmp_file} ]]; then
    quit "export properties failed"
  else
    mv "${config_tmp_file}" "${exportFile}"
    if [[ $? -ne 0 || ! -e ${exportFile} ]]; then
      quit "mv properties failed"
    fi
  fi
}

function getProperties() {
  if [[ "${GET_PROPERTIES_FROM_LOCAL}" == "true" ]]; then
      getPropertiesFromLocal "$@"
      if [[ $? -ne 0 ]]; then
         verboseLog "get from cache failed, try to get in real time again" "$@"
         GET_PROPERTIES_FROM_LOCAL=""
         getPropertiesFromKE "$@"
      fi
  else
      getPropertiesFromKE "$@"
  fi

}

function main() {
  if [[ $# == 0 ]]; then
    help
    exit 0
  fi

  while getopts "c:r:eh" opt; do
    case ${opt} in
      c)
        shift
        GET_PROPERTIES_FROM_LOCAL="true"
        getPropertiesFromLocal "$@" || echo ""
        exit $?;;
      r)
        shift
        GET_PROPERTIES_FROM_LOCAL=""
        getProperties "$@"
        exit $?;;
      e)
        shift
        exportPropertiesToLocal "$@"
        exit $?;;
      h)
        help
        exit $?;;
      *)
        verboseLog "Invalid option: -$OPTARG"
        exit 1;;
    esac
  done

  getProperties "$@"
}

main "$@"