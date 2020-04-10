#!/bin/bash
# Kyligence Inc. License

function help() {
  echo "Usage: migrate-4.0.x-to-4.1.x.sh"
  echo "it will"
  echo "1. backup current metadata"
  echo "2. modify audit log table"
  echo "3. set project default database, replace project override properties"
  echo "4. migrate job metadata"
  echo "5. rename duplicate case insensitive project name, username"
  echo "6. restore modified metadata to datbase, if not success will rollback to origin metadata"
  exit 0
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
  error "...................................................[FAIL]"
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

  info "backup current metastore"
  if [[ ! -d "${KYLIN_HOME}"/meta_backups/ ]]; then
    mkdir "${KYLIN_HOME}"/meta_backups/
  fi
  "${KYLIN_HOME}"/bin/metastore.sh backup "${KYLIN_HOME}"/meta_backups/
  if [ $? != 0 ]; then
    fail
  fi
  metadata_backup_dir=$(ls -t ${KYLIN_HOME}/meta_backups/ | grep backup | head -1)

  info "backup current metastore in ${KYLIN_HOME}/meta_backups/${metadata_backup_dir} [DONE]"

  operator_metadata_backup_dir="${KYLIN_HOME}"/meta_backups/"${metadata_backup_dir}"_operator

  cp -a "${KYLIN_HOME}"/meta_backups/"${metadata_backup_dir}" "${operator_metadata_backup_dir}"

  info 'modofy audit log table'
  "${KYLIN_HOME}"/bin/kylin.sh io.kyligence.kap.tool.upgrade.AddInstanceColumnCLI
  if [ $? != 0 ]; then
    fail
  fi
  info 'modofy audit log table [DONE]'

  info 'set project default database and replace project override properties'
  "${KYLIN_HOME}"/bin/kylin.sh io.kyligence.kap.tool.upgrade.UpdateProjectCLI -dir "${operator_metadata_backup_dir}"
  if [ $? != 0 ]; then
    fail
  fi
  info 'set project default database and replace project override properties [DONE]'

  info "migrate job metastore"
  "${KYLIN_HOME}"/bin/kylin.sh io.kyligence.kap.tool.upgrade.MigrateJobTool -dir "${operator_metadata_backup_dir}"
  if [ $? != 0 ]; then
    fail
  fi
  info "migrate job metastore [DONE]"

  info "rename duplicate project name"
  "${KYLIN_HOME}"/bin/kylin.sh io.kyligence.kap.tool.upgrade.RenameProjectResourceTool --collect-only false -dir "${operator_metadata_backup_dir}"

  if [ $? != 0 ]; then
    fail
  fi
  info "rename duplicate project name [DONE]"

  info "rename duplicate username"
  "${KYLIN_HOME}"/bin/kylin.sh io.kyligence.kap.tool.upgrade.RenameUserResourceTool --collect-only false -dir "${operator_metadata_backup_dir}"

  if [ $? != 0 ]; then
    fail
  fi
  info "rename duplicate username [DONE]"

  info "restore modified metadata"
  "${KYLIN_HOME}"/bin/metastore.sh restore "${operator_metadata_backup_dir}"
  if [ $? != 0 ]; then
    error "restore modified metadata failed, restore to origin metadata"
    "${KYLIN_HOME}"/bin/metastore.sh restore "${metadata_backup_dir}"
    fail
  fi
  info "restore modified metadata [DONE]"

  info "all sucess!"
}

if [ $# != 0 ]; then
  help
fi

source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/../sbin/header.sh $@

migrate
