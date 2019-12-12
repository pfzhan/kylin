#!/bin/bash
# Kyligence Inc. License

function help() {
    echo "Usage: upgrade.sh <OLD_KYLIN_HOME> [--silent]"
    echo
    echo "<OLD_KYLIN_HOME>    Specify the old version of the Kyligence Enterprise"
    echo "                    installation directory."
    echo
    echo "--silent            Optional, don't enter interactive mode, automatically complete the upgrade."
    exit 1
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

function logging() {
    case $1 in
        "info") shift; info $@ ;;
        "warn") shift; warn $@ ;;
        "error") shift; error $@ ;;
        *) echo -e $@ ;;
    esac

    (echo -e `date '+%F %H:%M:%S'` $@ >> $upgrade_log)
}

function fail() {
    error "...................................................[FAIL]"
    error "Upgrade Kyligence Enterprise failed."
    exit 1
}

function prompt() {
    if [[ $silent -eq 0 ]]; then
        return 0
    fi

    read -p "$@ (y/n) > " answer
    if [[ -z $answer ]] || [[ $answer == "y" ]]; then
        return 0
    else
        return 1
    fi
}

function upgrade() {
    if [[ -f ${old_kylin_home}/pid ]]; then
        PID=`cat ${old_kylin_home}/pid`
        if ps -p $PID > /dev/null; then
          error "Please stop the Kyligence Enterprise during the upgrade process."
          exit 1
        fi
    fi

    origin_version=$(awk '{print $NF}' ${old_kylin_home}/VERSION)
    target_version=$(awk '{print $NF}' ${new_kylin_home}/VERSION)
    echo
    logging "warn" "Upgrade Kyligence Enterprise from ${origin_version} to ${target_version}"
    warn "Old KYLIN_HOME is ${old_kylin_home}, log is at ${upgrade_log}"
    echo

    # copy LICENSE
    logging "Copy LICENSE"
    if [[ -f ${old_kylin_home}/LICENSE ]]; then
        if prompt "'${old_kylin_home}/LICENSE' -> '${new_kylin_home}/'"; then
            \cp -vf ${old_kylin_home}/LICENSE ${new_kylin_home}/ >> $upgrade_log || fail
        fi
    fi
    info "...................................................[DONE]"

    # copy kylin conf
    # exclude 'profile*' directory
    logging "Copy Kylin Conf"
    for conf_file in $(ls $old_kylin_home/conf); do
        if prompt "'${old_kylin_home}/conf/${conf_file}' -> '${new_kylin_home}/conf/'"; then
            if [[ -d ${old_kylin_home}/conf/${conf_file} ]]; then
                # silent copy directory
                \cp -rfv ${old_kylin_home}/conf/${conf_file} ${new_kylin_home}/conf/ >> $upgrade_log || fail
            else
                # need to delete the symbolic link first
                \cp -vf --remove-destination ${old_kylin_home}/conf/${conf_file} ${new_kylin_home}/conf/ >> $upgrade_log || fail
            fi

        fi
    done
    info "...................................................[DONE]"

    # copy ext jars
    logging "Copy Ext Jars"
    for jar_file in $(ls $old_kylin_home/lib/ext); do
        if prompt "'${old_kylin_home}/lib/ext/${jar_file}' -> '${new_kylin_home}/lib/ext/'"; then
            \cp -vf ${old_kylin_home}/lib/ext/${jar_file} ${new_kylin_home}/lib/ext/ >> $upgrade_log || fail
        fi
    done
    info "...................................................[DONE]"

    # copy the customize directory under old kylin home
    # such as hadoop_conf
    logging "Copy Customize Directory"
    OLDIFS=$IFS
    IFS=$'\n'
    for diff_log in $(diff -qr $old_kylin_home $new_kylin_home); do
        if [[ $diff_log =~ (^Only in ${old_kylin_home}: )(.*) ]]; then
            diff_file=${BASH_REMATCH[2]}
            if [[ $diff_file == "meta_backups" || $diff_file == "appid" || $diff_file == "work" ]]; then
                continue
            fi
            if prompt "'${old_kylin_home}/${diff_file}' -> '${new_kylin_home}/'"; then
                cp -rfv ${old_kylin_home}/${diff_file} ${new_kylin_home}/ >> $upgrade_log || fail
            fi
        fi
    done
    IFS=$OLDIFS
    info "...................................................[DONE]"

    # sed -nE 's/^([#\t ]*)(kylin\..*|kap\..*)/\2/p' kylin.properties | awk '{kv[substr($0,0,index($0,"=")-1)]=substr($0,index($0,"=")+1)} END{print kv["kylin.metadata.url"]}'
    logging "Checking Kylin Conf"
python <<PY
from __future__ import print_function
import os
import sys
try:
    import commands as cmd
except ImportError:
    import subprocess as cmd

def printer(msg, *outs):
    for o in outs: print(msg, file=o)

def getProp(prop_file):
    if not os.path.exists(prop_file):
        return dict()

    output = cmd.getoutput("sed -nE 's/^([#\\\\t ]*)(kylin\\..*|kap\\..*)/\\\\2/p' %s" % prop_file)
    prop = dict()
    for x in output.split('\n'):
        prop[x[0: x.index('=')]] = x[x.index('=') + 1:]
    return prop

with open('${upgrade_log}', 'a+') as upgrade_log:
    origin_prop = getProp('${new_kylin_home}/conf/kylin.properties')
    prod_prop = dict(getProp('${new_kylin_home}/conf/kylin.properties'), **getProp('${new_kylin_home}/conf/kylin.properties.override'))
    diffs = set(prod_prop.items()) - set(origin_prop.items())

    def logging(msg):
        printer(msg, sys.stdout, upgrade_log)

    for diff in diffs:
        logging(diff)
PY
    info "...................................................[DONE]"

    logging "Install"
    if prompt "'${new_kylin_home}' -> '${old_kylin_home}'"; then
        install_dir=$(dirname $old_kylin_home)
        home_name=$(basename $old_kylin_home)

        # backup
        now=`date '+%Y%m%d%H%M'`
        cd $install_dir && tar -zcvf ${home_name}_${now}.tar.gz ${home_name} >> $upgrade_log || fail

        # install
        rm -rfv ${old_kylin_home} >> $upgrade_log || fail
        mv -vf ${new_kylin_home} ${old_kylin_home} >> $upgrade_log || fail
        info "...................................................[DONE]"
    else
        warn "...................................................[SKIP]"
    fi

    info "Upgrade finished!"

}


new_kylin_home=$(cd `dirname -- $0` && cd ../ && pwd -P)
silent=1
while [[ $# != 0 ]]; do
    if [[ $1 == "--silent" ]]; then
        silent=0
    else
        old_kylin_home=$(cd $1 && pwd)
    fi
    shift
done

if [[ -z $old_kylin_home ]] || [[ ! -d $old_kylin_home ]]; then
    help
fi

if [[ $old_kylin_home == $new_kylin_home ]]; then
    error "Please specify the old version of the Kyligence Enterprise installation directory."
    help
fi

mkdir -p ${new_kylin_home}/logs
upgrade_log=${new_kylin_home}/logs/upgrade-$(date '+%Y_%m_%d_%H_%M_%S').log

set -o errexit
set -o pipefail
upgrade

