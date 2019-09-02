#!/bin/bash
# Kyligence Inc. License

function exportKRB5CCNAME() {
    KAP_KERBEROS_CACHE=`$KYLIN_HOME/bin/get-properties.sh kap.kerberos.cache`
    export KRB5CCNAME=${KYLIN_HOME}"/conf/"${KAP_KERBEROS_CACHE}
}

function initKerberos() {
    KAP_KERBEROS_PRINCIPAL=`$KYLIN_HOME/bin/get-properties.sh kap.kerberos.principal`
    KAP_KERBEROS_KEYTAB=`$KYLIN_HOME/bin/get-properties.sh kap.kerberos.keytab`
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

function prepareZKPrinciple() {
    params=`env | grep "HADOOP_OPTS"`
    splitParams=(${params//'-D'/ })
    for param in ${splitParams[@]}
    do
        if [[ "$param" == zookeeper* ]];then
            infos=(${param//'zookeeper.server.principal='/ })
            envZKPrinciple=${infos[0]}
            zkPrinciple=`$KYLIN_HOME/bin/get-properties.sh kap.kerberos.zookeeper.server.principal`
            if [ $zkPrinciple != $envZKPrinciple ]
            then
                sed -i '/kap.kerberos.zookeeper.server.principal/d' ${KYLIN_CONFIG_FILE}
                sed -i '$a\kap.kerberos.zookeeper.server.principal='$envZKPrinciple'' ${KYLIN_CONFIG_FILE}
            fi
        fi
    done
}

function prepareFIKerberosInfoIfNeeded() {
    prepareJaasConf
    KERBEROS_PALTFORM=`$KYLIN_HOME/bin/get-properties.sh kap.kerberos.platform`
    if [[ "${KERBEROS_PALTFORM}" == "FI" ]]
    then
        prepareZKPrinciple
    fi
}

function initKerberosIfNeeded(){
    if [[ -n $SKIP_KERB ]]; then
        return
    fi

    export SKIP_KERB=1

    KAP_KERBEROS_ENABLED=`$KYLIN_HOME/bin/get-properties.sh kap.kerberos.enabled`
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