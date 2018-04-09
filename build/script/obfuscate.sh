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
keepParam=$(grep -hro --include="*.xml" --exclude={pom.xml,workspace.xml,checkstyle-\*.xml} "io\.kyligence\.kap\.rest\.[^\"\<]*" . | sort -u | awk '{print "-keep class " $0 " {*;}"}')' '
# keep all class name in double quote
keepParam+=$(grep -hro --include="*.java" "\"io\.kyligence\.kap\.[^\"\\]*" . | cut -c 2- | sort -u | awk '{print "-keep class " $0 " {*;}"}')' '
# keep classes in kylin.properties
keepParam+=$(grep -hro --include="kylin.properties" "io\.kyligence\.kap\.[^\"\\]*" . | sort -u | awk '{print "-keep class " $0 " {*;}"}')' '
# keep classes in kylin-defaults*.properties
keepParam+=$(grep -hro --include="kylin-defaults*.properties" "io\.kyligence\.kap\.[^\"\\#]*" . | sort -u | awk '{print "-keep class " $0 " {*;}"}')' '
# keep classes in *.sh
keepParam+=$(grep -hro --include="*.sh" "io\.kyligence\.kap\.[^\*\.]*\.[^ \`\"]*" . | sort -u | awk '{print "-keep class " $0 " {*;}"}')' '

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
# $3 - 0 for -printmapping, other for -applymapping
# $4 - 0 for delete input jars, other for keep
# $5 - output jar name, without .jar
# .. - input jar names
function obfuscate {
	cd $1
	MVN_OPTS="-Dmdep.outputFile=cp.txt"
	if [ "$MVN_PROFILE" != "" ]; then
	    MVN_OPTS="-P $MVN_PROFILE $MVN_OPTS"
    fi
	mvn dependency:build-classpath $MVN_OPTS
	cp=`cat cp.txt`
	rm cp.txt
	cd -

	# cover both jar and war
	location_dir=$2
	output_jar=$location_dir/$5.jar

	if [ "$3" -eq "0" ];then
		otherParam='-printmapping server_mapping.txt'
	else
		otherParam='-applymapping server_mapping.txt'
	fi

    keep_input=$4
	shift 5

	# make proguard config
	cat build/script/obfuscate.pro > tmp.pro
	for input_jar in $@; do
	    echo -injars $location_dir/$input_jar \(!META-INF/*.SF,!META-INF/*.DSA,!META-INF/*.RSA\)  >> tmp.pro
	done

	echo -outjars $output_jar \(!META-INF/*.SF,!META-INF/*.DSA,!META-INF/*.RSA\)    >> tmp.pro
	echo -libraryjars $cp                                                           >> tmp.pro
	echo -libraryjars $java_home/lib/rt.jar                                         >> tmp.pro
	echo -libraryjars $java_home/lib/jce.jar                                        >> tmp.pro
	echo -libraryjars $java_home/lib/jsse.jar                                       >> tmp.pro
	echo -libraryjars $java_home/lib/ext/sunjce_provider.jar                        >> tmp.pro
	echo $keepParam $otherParam                                                     >> tmp.pro
	
	proguard @tmp.pro  || { exit 1; }

    if [ "$keep_input" -eq "0" ]; then
        for input_jar in $@; do
            rm $location_dir/$input_jar
        done
	fi
	rm tmp.pro
}

# extract server war
mkdir tmp_war
cd tmp_war
jar -xvf ../extensions/server/target/kap-server-${kap_version}.war
rm WEB-INF/lib/kap.jar
cd ..

# only obfuscate kap* jars
obfuscate extensions/server/ tmp_war/WEB-INF/lib 0 0 kap-one `cd tmp_war/WEB-INF/lib;ls kap-*.jar`

# combine kap* and kylin* jars to one jar
build/script/one-jar.sh tmp_war/WEB-INF/lib
cd tmp_war
jar cvf kap-server-${kap_version}.war *
mv kap-server-${kap_version}.war ../tmp/kylin.war
chmod 644 ../tmp/kylin.war
cd ..
rm -rf tmp_war

# obfuscate job(assembly) jar
obfuscate extensions/assembly/ $BUILD_LIB_DIR 1 1 kylin-job-kap-${release_version}-obf kylin-job-kap-${release_version}.jar
mv $BUILD_LIB_DIR/kylin-job-kap-${release_version}-obf.jar tmp/

## obfuscate coprocessor jar
#obfuscate extensions/storage-hbase/ $BUILD_LIB_DIR 1 1 kylin-coprocessor-kap-${release_version}-obf kylin-coprocessor-kap-${release_version}.jar
#mv $BUILD_LIB_DIR/kylin-coprocessor-kap-${release_version}-obf.jar tmp/

# obfuscate storage parquet jar
obfuscate extensions/storage-parquet/ $BUILD_LIB_DIR 1 1 kylin-storage-parquet-kap-${release_version}-obf kylin-storage-parquet-kap-${release_version}.jar
mv $BUILD_LIB_DIR/kylin-storage-parquet-kap-${release_version}-obf.jar tmp/

# obfuscate tool jar
obfuscate extensions/assembly/ $BUILD_LIB_DIR/../tool/ 1 1 kylin-tool-kap-${release_version}-obf kylin-tool-kap-${release_version}.jar
mv $BUILD_LIB_DIR/../tool/kylin-tool-kap-${release_version}-obf.jar tmp/

# compare whether rest api signature changed in obf stage
java -jar thirdparty/pathfinder/pathfinder-1.0-SNAPSHOT.jar tmp/kylin.war > tmp/war_obf.txt
java -jar thirdparty/pathfinder/pathfinder-1.0-SNAPSHOT.jar extensions/server/target/kap-server-${kap_version}.war > tmp/war_orig.txt

diff tmp/war_obf.txt tmp/war_orig.txt
if [ "$?" -ne "0" ]; then
    echo "Rest API changed in Obfuscation. Please have a check."
    exit 1
else
    echo "Obfuscation passed."
fi
