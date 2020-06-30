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

function exportKRB5CCNAME() {
    KAP_KERBEROS_CACHE=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.cache`
    export KRB5CCNAME=${KYLIN_HOME}"/conf/"${KAP_KERBEROS_CACHE}
}

function prepareKerberosOpts() {
    export KYLIN_KERBEROS_OPTS=""
    KAP_KERBEROS_ENABLED=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.enabled`
    if [[ "${KAP_KERBEROS_ENABLED}" == "true" ]];then
      KYLIN_KERBEROS_OPTS="-Djava.security.krb5.conf=${KYLIN_HOME}/conf/krb5.conf"
    fi
}

function initKerberos() {
    KAP_KERBEROS_PRINCIPAL=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.principal`
    KAP_KERBEROS_KEYTAB=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.keytab`
    KAP_KERBEROS_KEYTAB_PATH=${KYLIN_HOME}"/conf/"${KAP_KERBEROS_KEYTAB}
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
            envZKPrinciple=${infos[0]}
            zkPrinciple=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.zookeeper-server-principal`
            if [ $zkPrinciple != $envZKPrinciple ]
            then
                sed -i '/kap.kerberos.zookeeper.server.principal/d' ${KYLIN_CONFIG_FILE}
                sed -i '/kylin.kerberos.zookeeper-server-principal/d' ${KYLIN_CONFIG_FILE}
                sed -i '$a\kylin.kerberos.zookeeper-server-principal='$envZKPrinciple'' ${KYLIN_CONFIG_FILE}
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

    KAP_KERBEROS_ENABLED=`$KYLIN_HOME/bin/get-properties.sh kylin.kerberos.enabled`
    if [[ "${KAP_KERBEROS_ENABLED}" == "true" ]]
    then
        if [[ -z "$(command -v klist)" ]]
        then
             quit "Kerberos command not found! Please check configuration of Kerberos in kylin.properties or check Kerberos installation."
        fi

        exportKRB5CCNAME

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