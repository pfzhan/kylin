#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script/functions.sh
exportProjectVersions

#Make sure commands exist in environment
checkCommandExits proguard
checkCommandExits mvn

BUILD_LIB_DIR=build/lib

# keep all rest classes in *.xml
keepParam=`grep -hro --include="*[^pom].xml" "io\.kyligence\.kap\.rest\.[^\"\<]*" . | sort -u | awk '{print "-keep class " $0 " {*;}"}'`' '
# keep all class name in double quote
keepParam+=`grep -hro --include="*.java" "\"io\.kyligence\.kap\.[^\"\\]*" . | cut -c 2- | sort -u | awk '{print "-keep class " $0 " {*;}"}'`' '
# keep classes in kylin.properties
keepParam+=`grep -hro --include="kylin.properties" "io\.kyligence\.kap\.[^\"\\]*" . | sort -u | awk '{print "-keep class " $0 " {*;}"}'`' '
# keep classes in *.sh
keepParam+=`grep -hro --include="*.sh" "io\.kyligence\.kap\.[^\*\.]*\.[^ ]*" . | sort -u | awk '{print "-keep class " $0 " {*;}"}'`' '

if [ -z $java_home ]; then
	java_home_mess=`mvn -version | grep "Java home"`
	java_home=`cut -d ':' -f 2- <<< "$java_home_mess"`
fi

# directory tmp for output
if [ ! -f tmp ]; then
	mkdir tmp
fi

# $1 - dir for get class path
# $2 - bin location dir
# $3 - bin orignal name
function obfuscate {
	cd $1
	mvn dependency:build-classpath -Dmdep.outputFile=cp.txt
	cp=`cat cp.txt`
	rm cp.txt
	cd ../../

	# cover both jar and war
	if [ -f $2/$3.jar ];then
		origin_bin=$2/$3.jar
		obf_bin=$2/$3-obf.jar
		otherParam='-applymapping server_mapping.txt'
	else
		origin_bin=$2/$3.war
		obf_bin=$2/$3-obf.war
		otherParam='-printmapping server_mapping.txt'
	fi
	
	# make proguard config
	cat build/script/obfuscate.pro > tmp.pro
	echo -injars ${origin_bin} \(!META-INF/*.SF,!META-INF/*.DSA,!META-INF/*.RSA\)  >> tmp.pro
	echo -outjars ${obf_bin} \(!META-INF/*.SF,!META-INF/*.DSA,!META-INF/*.RSA\)    >> tmp.pro
	echo -libraryjars $cp                                                          >> tmp.pro
	echo -libraryjars $java_home/lib/rt.jar                                        >> tmp.pro
	echo -libraryjars $java_home/lib/jce.jar                                       >> tmp.pro
	echo -libraryjars $java_home/lib/jsse.jar                                      >> tmp.pro
	echo -libraryjars $java_home/lib/ext/sunjce_provider.jar                       >> tmp.pro
	echo $keepParam $otherParam                                                    >> tmp.pro
	
	proguard @tmp.pro  || { exit 1; }
	rm tmp.pro
	
	if [ -f $2/$3.jar ];then
		mv $obf_bin tmp/$3.jar
	else
		mv $obf_bin tmp/$3.war
	fi
}

# obfuscate server war
obfuscate extensions/server/ extensions/server/target kap-server-${kap_version}

# obfuscate job(assembly) jar
obfuscate extensions/assembly/ $BUILD_LIB_DIR kylin-job-kap-${kap_version}

# obfuscate coprocessor jar
obfuscate extensions/storage-hbase/ $BUILD_LIB_DIR kylin-coprocessor-kap-${kap_version}

rm server_mapping.txt
#echo "keep param " $keepParam
