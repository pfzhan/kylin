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

KAP_KERBEROS_ENABLED=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.enabled`

function is_kap_kerberos_enabled(){
  if [[ "${KAP_KERBEROS_ENABLED}" == "true" ]];then
    echo 1
    return 1
  else
    echo 0
    return 0
  fi
}

function exportKRB5() {
    KAP_KERBEROS_CACHE=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.cache`
    export KRB5CCNAME=${KYLIN_HOME}"/conf/"${KAP_KERBEROS_CACHE}

    KAP_KERBEROS_KRB5=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.krb5-conf`
    if [ ! -n "$KAP_KERBEROS_KRB5" ]; then
        quit "kylin.kerberos.krb5-conf cannot be set to empty in kylin.properties"
    fi

    export KRB5_CONFIG=${KYLIN_HOME}"/conf/"${KAP_KERBEROS_KRB5}
    echo "KRB5_CONFIG is set to ${KRB5_CONFIG}"
}

function prepareKerberosOpts() {
    export KYLIN_KERBEROS_OPTS=""
    if [[ $(is_kap_kerberos_enabled) == 1 ]];then
      KYLIN_KERBEROS_OPTS="-Djava.security.krb5.conf=${KYLIN_HOME}/conf/krb5.conf"
    fi
}

function initKerberos() {
    KAP_KERBEROS_PRINCIPAL=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.principal`
    KAP_KERBEROS_KEYTAB=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.keytab`
    KAP_KERBEROS_KEYTAB_PATH=${KYLIN_HOME}"/conf/"${KAP_KERBEROS_KEYTAB}

    if [ ! -e ${KRB5_CONFIG} ]; then
        quit "${KRB5_CONFIG} file doesn't exist"
    fi

    echo "Kerberos is enabled, init..."
    kinit -kt $KAP_KERBEROS_KEYTAB_PATH $KAP_KERBEROS_PRINCIPAL
}

function prepareJaasConf() {
    if [ -f ${KYLIN_HOME}/conf/jaas.conf ]; then
        return
    fi

    cat > ${KYLIN_HOME}/conf/jaas.conf <<EOL
Client{
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=false
    useTicketCache=true
    debug=false;
};

EOL
}

function prepareZKPrincipal() {
    params=`env | grep "HADOOP_OPTS"`
    splitParams=(${params//'-D'/ })
    for param in ${splitParams[@]}
    do
        if [[ "$param" == zookeeper* ]];then
            infos=(${param//'zookeeper.server.principal='/ })
            envZKPrincipal=${infos[0]}
            zkPrincipal=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.zookeeper-server-principal`
            if [ $zkPrincipal != $envZKPrincipal ]
            then
                sed -i '/kap.kerberos.zookeeper.server.principal/d' ${KYLIN_CONFIG_FILE}
                sed -i '/kylin.kerberos.zookeeper-server-principal/d' ${KYLIN_CONFIG_FILE}
                sed -i '$a\kylin.kerberos.zookeeper-server-principal='$envZKPrincipal'' ${KYLIN_CONFIG_FILE}
            fi
        fi
    done
}

function prepareFIKerberosInfoIfNeeded() {
    prepareJaasConf
    KERBEROS_PALTFORM=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.platform`
    if [[ "${KERBEROS_PALTFORM}" == "FI" ]]
    then
        prepareZKPrincipal
    fi
}

function initKerberosIfNeeded(){
    if [[ -n $SKIP_KERB ]]; then
        return
    fi

    export SKIP_KERB=1

    if [[ $(is_kap_kerberos_enabled) == 1 ]]
    then
        if [[ -z "$(command -v klist)" ]]
        then
             quit "Kerberos command not found! Please check configuration of Kerberos in kylin.properties or check Kerberos installation."
        fi

        exportKRB5

        if ! klist -s
        then
            initKerberos
        else
           echo "Kerberos ticket is valid, skip init."
        fi

        prepareFIKerberosInfoIfNeeded

        # check if kerberos init success
        if ! klist -s
        then
            quit "Kerberos ticket is not valid, please run 'kinit -kt <keytab_path> <principal>' manually or set configuration of Kerberos in kylin.properties."
        fi
    fi
}