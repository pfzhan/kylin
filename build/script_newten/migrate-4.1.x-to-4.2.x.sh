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
  echo "Usage: migrate-4.1.x-to-4.2.x.sh -e
it will
1. backup current metadata
2. upgrade user group metadata
3. check project mode
4. modify session table
5. create layout candidate table
6. upgrade recommendation metadata
7. update and add dimensions according to current models
8. create epoch table
9. rollback metadata to metastore, if the upgrade process failed
"
  exit 0
}

function step() {
  echo "$@"
}

function info() {
  echo -e "\033[32m$@\033[0m"
}

function warn() {
  echo -e "\033[33m$@\033[0m"
}

function error() {
  echo -e "\033[31m$@\033[0m"
}

function fail() {
  exit 1
}

function migrate() {
  if [[ -f ${KYLIN_HOME}/pid ]]; then
    PID=$(cat "${KYLIN_HOME}"/pid)
    if ps -p "$PID" >/dev/null; then
      error "Please stop the Kyligence Enterprise during the upgrade process."
      exit 1
    fi
  fi

  step "start to backup current metastore"
  if [[ ! -d "${KYLIN_HOME}"/meta_backups/ ]]; then
    mkdir -p "${KYLIN_HOME}"/meta_backups/
  fi

  if [[ ! -d "${KYLIN_HOME}"/logs/ ]];then
    mkdir -p "${KYLIN_HOME}"/logs/
  fi

  "${KYLIN_HOME}"/bin/metastore.sh backup "${KYLIN_HOME}"/meta_backups/ &> ${KYLIN_HOME}/logs/shell.stderr
  if [[ $? != 0 ]]; then
    error "backup current metastore failed, for more details please refer to ${KYLIN_HOME}/logs/shell.stderr."
    fail
  fi
  metadata_backup_dir=$(ls -t ${KYLIN_HOME}/meta_backups/ | grep backup | head -1)

  info "backup current metastore in ${KYLIN_HOME}/meta_backups/${metadata_backup_dir} "

  operator_metadata_backup_dir="${KYLIN_HOME}"/meta_backups/"${metadata_backup_dir}"_operator

  cp -a "${KYLIN_HOME}"/meta_backups/"${metadata_backup_dir}" "${operator_metadata_backup_dir}"

  step 'start to check user group metadata.'
  "${KYLIN_HOME}"/bin/kylin.sh io.kyligence.kap.tool.upgrade.UpdateUserGroupCLI -d "${operator_metadata_backup_dir}" $1
  if [[ $? != 0 ]]; then
    error "user group metadata upgrade failed, for more details please refer to ${KYLIN_HOME}/logs/shell.stderr."
    fail
  fi

  step 'start to check project mode.'
  "${KYLIN_HOME}"/bin/kylin.sh io.kyligence.kap.tool.upgrade.CheckProjectModeCLI -d "${operator_metadata_backup_dir}" $1
  if [[ $? != 0 ]]; then
    error "projects mode upgrade failed, for more details please refer to ${KYLIN_HOME}/logs/shell.stderr."
    fail
  fi

  step 'start to check session and session_ATTRIBUTES table.'
  "${KYLIN_HOME}"/bin/kylin.sh io.kyligence.kap.tool.upgrade.UpdateSessionTableCLI --truncate $1
  if [[ $? != 0 ]]; then
    error "session and session_ATTRIBUTES table updated failed, for more details please refer to ${KYLIN_HOME}/logs/shell.stderr."
    fail
  fi

  step 'start to check layout candidate table.'
  "${KYLIN_HOME}"/bin/kylin.sh io.kyligence.kap.tool.upgrade.CreateTableLayoutCandidateCLI $1
  if [[ $? != 0 ]]; then
    error "layout candidate table upgrade failed, for more details please refer to ${KYLIN_HOME}/logs/shell.stderr."
    fail
  fi

  step 'start to check recommendation metadata.'
  "${KYLIN_HOME}"/bin/kylin.sh io.kyligence.kap.tool.upgrade.DeleteFavoriteQueryCLI -d "${operator_metadata_backup_dir}" $1
  if [[ $? != 0 ]]; then
    error "recommendation metadata upgrade failed, for more details please refer to ${KYLIN_HOME}/logs/shell.stderr"
    fail
  fi

  step "start to check model dimensions."
  "${KYLIN_HOME}"/bin/kylin.sh io.kyligence.kap.tool.upgrade.UpdateModelCLI -d "${operator_metadata_backup_dir}" $1
  if [[ $? != 0 ]]; then
    error "model dimensions upgrade failed, for more details please refer to ${KYLIN_HOME}/logs/shell.stderr."
    fail
  fi

  step "start to check epoch table."
  "${KYLIN_HOME}"/bin/kylin.sh io.kyligence.kap.tool.upgrade.CreateTableEpochCLI $1
  if [[ $? != 0 ]]; then
    error "epoch table upgrade failed, for more details please refer to ${KYLIN_HOME}/logs/shell.stderr."
    fail
  fi

  if [[ "-e" == "$1" ]];then
    step "start to restore the new metadata."
    "${KYLIN_HOME}"/bin/metastore.sh restore "${operator_metadata_backup_dir}" --after-truncate &> ${KYLIN_HOME}/logs/shell.stderr
    if [[ $? != 0 ]]; then
      error "restore failed. please contact technical support for help."
      fail
    fi
    info "restore successfully. upgrade process finished."
  else
    info "Upgrade check finished, please add -e to execute the script. For more details, please refer to ${KYLIN_HOME}/logs/shell.stderr."
  fi

}

exec="false"
while getopts ":eh" opt; do
  case ${opt} in
    e)
        exec="true"
      ;;
    h)
        help
      ;;
    \?)
      echo "Invalid option: -$OPTARG"
        help
      ;;
  esac
done

# have to under bin or sbin directory.
source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/../sbin/header.sh
if [[ "true" == "${exec}" ]];then
    migrate -e
else
    migrate
fi
