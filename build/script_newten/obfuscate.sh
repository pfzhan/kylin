#!/bin/bash

dir=$(dirname ${0})
cd ${dir}/../..

source build/script_newten/functions.sh
exportProjectVersions

#Make sure commands exist in environment
checkCommandExists proguard
checkCommandExists mvn

BUILD_LIB_DIR=build/lib

# keep all rest classes in *.xml
keepParam=$(grep -hro --include="*.xml" --exclude={pom.xml,workspace.xml,checkstyle-\*.xml} "io\.kyligence\.kap\.rest\.[^\"\<]*" src | sort -u | awk '{print "-keep class " $0 " {*;}"}')' '
# keep all class name in double quote
keepParam+=$(grep -hro --include="*.java" "\"io\.kyligence\.kap\.[^\"\\]*" src | cut -c 2- | sort -u | awk '{print "-keep class " $0 " {*;}"}')' '
# keep classes in kylin.properties
keepParam+=$(grep -hro --include="kylin.properties" "io\.kyligence\.kap\.[^\"\\]*" src | sort -u | awk '{print "-keep class " $0 " {*;}"}')' '
# keep classes in kylin-defaults*.properties
keepParam+=$(grep -hro --include="kylin-defaults*.properties" "io\.kyligence\.kap\.[^\"\\#]*" src | sort -u | awk '{print "-keep class " $0 " {*;}"}')' '
# keep classes in kylin-*-log4j.properties
keepParam+=$(grep -hro --include="kylin-*-log4j.properties" "io\.kyligence\.kap\.[^\"\\#]*" src | sort -u | awk '{print "-keep class " $0 " {*;}"}')' '
# keep classes in *.sh
keepParam+=$(grep -hro --include="*.sh" "io\.kyligence\.kap\.[^\*\.]*\.[^ \`\"]*" src | sort -u | awk '{print "-keep class " $0 " {*;}"}')' '

if [ -z $java_home ]; then
	java_home_mess=`java -XshowSettings:properties -version 2>&1 > /dev/null | grep "java.home"`
    	java_home=`cut -d '=' -f 2- <<< "$java_home_mess"`
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

	# cover both jar and jar
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
	cat build/script_newten/obfuscate.pro > tmp.pro
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

# extract server jar
ls src/server/target/jars/kap-*.jar > kap_jar.txt

# only obfuscate kap* jars
obfuscate src/server/ src/server/target/jars 0 0 kap-all `cd src/server/target/jars;ls kap-*.jar`

for f in `cat kap_jar.txt`; do
	rm -f $f
done
rm kap_jar.txt

# obfuscate job(assembly) jar
obfuscate src/assembly/ $BUILD_LIB_DIR 1 1 kap-assembly-${release_version}-job-obf newten-job.jar
mv $BUILD_LIB_DIR/kap-assembly-${release_version}-job-obf.jar tmp/

# obfuscate tool jar
obfuscate src/tool-assembly/ $BUILD_LIB_DIR/../tool/ 1 1 kap-tool-assembly-${release_version}-assembly-obf kap-tool-${release_version}.jar
mv $BUILD_LIB_DIR/../tool/kap-tool-assembly-${release_version}-assembly-obf.jar tmp/
