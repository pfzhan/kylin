-dontshrink
-dontoptimize
-dontnote
-dontwarn
-dontusemixedcaseclassnames
-ignorewarnings

-keep class io.kyligence.kap.common.obf.IKeep
-keep @org.aspectj.lang.annotation.Aspect public class * {*;}
-keep class * implements io.kyligence.kap.common.obf.IKeep {*;}
-keepclassmembers class * implements io.kyligence.kap.common.obf.IKeepClassMembers {*;}
-keepclassmembernames class * implements io.kyligence.kap.common.obf.IKeepClassMemberNames {*;}
-keepclasseswithmembers class * implements io.kyligence.kap.common.obf.IKeepClassWithMembers {*;}
-keepclasseswithmembernames class * implements io.kyligence.kap.common.obf.IKeepClassWithMemberNames {*;}
-keepnames class * implements io.kyligence.kap.common.obf.IKeepNames {*;}

-keepnames class * implements java.io.Serializable
-keepnames class * extends org.apache.kylin.measure.MeasureTypeFactory
-keepnames class * extends org.apache.kylin.engine.mr.common.AbstractHadoopJob
-keepnames class * extends org.apache.kylin.job.execution.AbstractExecutable
-keepnames class * implements org.springframework.security.web.AuthenticationEntryPoint
-keepnames class io.kyligence.kap.metadata.cube.cuboid.NQueryLayoutChooser
-keepnames class io.kyligence.kap.query.runtime.plan.ResultPlan
-keepnames class org.apache.spark.sql.kylin.external.LogEx
-keepnames class io.kyligence.kap.engine.spark.utils.LogEx

-keepclassmembers class * implements java.io.Serializable {
	static final long serialVersionUID;
	private static final java.io.ObjectStreamField[] serialPersistentFields;
	!static !transient *;
	private void writeObject(java.io.ObjectOutputStream);
	private void readObject(java.io.ObjectInputStream);
	public void writeExternal(java.io.ObjectOutputStream);
	public void readExternal(java.io.ObjectInputStream);
	java.lang.Object writeReplace();
	java.lang.Object readResolve();
}

-keepclassmembernames class io.kyligence.kap.** {
    static ** newInstance(org.apache.kylin.common.KylinConfig);
    static ** newInstance(org.apache.kylin.common.KylinConfig,java.lang.String);
}

-keepclassmembers class * {
     private static synthetic java.lang.Object $deserializeLambda$(java.lang.invoke.SerializedLambda);
}

-keepclassmembernames class * {
    private static synthetic *** lambda$*(...);
}

#Confuse the string constants corresponding to the class name.
-adaptclassstrings io.kyligence.kap.engine.spark.builder.DictionaryBuilderHelper
-adaptclassstrings io.kyligence.kap.engine.spark.builder.DFDictionaryBuilder

-keepclassmembers class io.kyligence.kap.rest.request.** {*;}
-keepclassmembers class io.kyligence.kap.rest.response.** {*;}

-keep class io.kyligence.kap.rest.service.OpenUserService {*;}
-keep class io.kyligence.kap.rest.service.OpenUserGroupService {*;}
-keep class io.kyligence.kap.rest.security.OpenAuthenticationProvider {*;}
-keep class io.kyligence.kap.metadata.user.ManagedUser {*;}
-keep class io.kyligence.kap.common.persistence.transaction.UnitOfWorkParams {*;}
-keep class io.kyligence.kap.common.persistence.transaction.UnitOfWorkParams$* {*;}
-keep class io.kyligence.kap.common.persistence.transaction.UnitOfWork {*;}
-keep class io.kyligence.kap.common.persistence.transaction.UnitOfWork$Callback {*;}
-keep class org.apache.kylin.common.persistence.ResourceStore {*;}
-keep class io.kyligence.kap.common.metrics.MetricsGroup {*;}
-keep class io.kyligence.kap.rest.monitor.SparkContextCanary {*;}

-keep class io.kyligence.kap.tool.shaded.** {*;}
-keep class io.kyligence.kap.job.shaded.** {*;}
-keep class io.kyligence.kap.storage.parquet.protocol.shaded.** {*;}

-keep class * extends org.apache.log4j.AppenderSkeleton {*;}
-keep class * extends org.apache.kylin.rest.service.BasicService {*;}
-keep class * extends io.kyligence.kap.rest.health.AbstractKylinHealthIndicator {*;}
-keep class * implements IGTCodeSystem {*;}
-keep class * extends AbstractApplication {*;}

-keep class !io.kyligence.** {*;}
-keep class io.kyligence.kap.rest.config.** {*;}
-keep class io.kyligence.kap.rest.controller.** {*;}
-keep class io.kyligence.kap.tool.** {*;}
-keep class io.kyligence.kap.query.util.** {*;}
-keep class io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl {*;}
-keep class io.kyligence.kap.shaded.** {*;}
-keep class io.kyligence.kap.ext.** {*;}
-keep class io.kyligence.api.** {*;}
-keep class io.kyligence.kap.secondstorage.** {*;}

-keep enum io.kyligence.kap.**,io.kyligence.kap.**$** {
    **[] $VALUES;
	public *;
}

-keepattributes Exceptions,Signature,Deprecated,SourceFile,LineNumberTable,*Annotation*,EnclosingMethod,InnerClasses
-keepdirectories
-keepparameternames

-renamesourcefileattribute SourceFile
-repackageclasses 'io.kyligence'
-allowaccessmodification
