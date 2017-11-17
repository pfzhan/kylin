-dontshrink
-dontoptimize
-dontnote
-dontusemixedcaseclassnames
-ignorewarnings

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
-keepclassmembers class io.kyligence.kap.rest.request.** {*;}
-keepclassmembers class io.kyligence.kap.rest.response.** {*;}
-keepclassmembers class * implements org.apache.kylin.gridtable.IGTCodeSystem {*;}
-keepclassmembers class io.kyligence.kap.job.SampleCubeJoinedFlatTable {*;}

-keep class io.kyligence.kap.modeling.smart.shaded.** {*;}
-keep class io.kyligence.kap.tool.shaded.** {*;}
-keep class io.kyligence.kap.job.shaded.** {*;}
-keep class io.kyligence.kap.storage.parquet.shaded.** {*;}
-keep class io.kyligence.kap.storage.parquet.protocol.shaded.** {*;}

-keep class * extends org.apache.log4j.AppenderSkeleton {*;}
-keep class * extends org.apache.kylin.rest.controller.BasicController {*;}
-keep class * extends org.apache.kylin.rest.service.BasicService {*;}
-keep class !io.kyligence.** {*;}
-keep class io.kyligence.kap.tool.** {*;}
-keep class io.kyligence.kap.query.util.** {*;}
-keep class io.kyligence.kap.storage.parquet.adhoc.udf.** {*;}
-keep class io.kyligence.kap.storage.parquet.cube.spark.rpc.SparkDriverClient {*;}
-keep class io.kyligence.kap.storage.parquet.adhoc.PushDownRunnerSparkImpl {*;}
-keep class * extends org.apache.kylin.common.util.AbstractApplication {*;}
-keep class io.kyligence.kap.storage.parquet.ParquetSpliceStorage {*;}
-keep class io.kyligence.kap.storage.parquet.ParquetStorage {*;}
-keep class io.kyligence.kap.storage.parquet.cube.spark.rpc.SparkExecutorPreAggFunction {*;}
-keep class io.kyligence.kap.storage.parquet.cube.spark.rpc.gtscanner.ParquetBytesGTScanner  {*;}
-keep class io.kyligence.kap.storage.parquet.cube.spark.rpc.gtscanner.ParquetBytesGTScanner4Cube  {*;}
-keep class io.kyligence.kap.storage.parquet.cube.spark.rpc.gtscanner.ParquetBytesGTScanner4Raw  {*;}
-keep class io.kyligence.kap.storage.parquet.format.ParquetRawTableFileInputFormat  {*;}
-keep class io.kyligence.kap.storage.parquet.format.ParquetSpliceTarballFileInputFormat {*;}
-keep class io.kyligence.kap.storage.parquet.format.ParquetTarballFileInputFormat  {*;}
-keep class io.kyligence.kap.storage.parquet.cube.spark.rpc.LongAccumulableParam  {*;}
-keep class io.kyligence.kap.storage.parquet.cube.spark.refactor.** {*;}
-keep class io.kyligence.kap.shaded.** {*;}
-keep class org.apache.spark.** {*;}
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
