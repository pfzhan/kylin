-dontshrink
-dontoptimize
-dontnote
-dontusemixedcaseclassnames
-ignorewarnings

-keepnames class * implements java.io.Serializable
-keepnames class * extends org.apache.kylin.measure.MeasureTypeFactory

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
-keepclassmembers class io.kyligence.kap.measure.percentile.PercentileAggFunc {*;}


-keep class !io.kyligence.** {*;}
-keep class io.kyligence.kap.query.udf.** {*;}
-keep class io.kyligence.kap.tool.** {*;}
-keep enum io.kyligence.kap.**,io.kyligence.kap.**$** {
    **[] $VALUES;
	public *;
} 

-keepattributes Exceptions,Signature,Deprecated,SourceFile,LineNumberTable,*Annotation*,EnclosingMethod
-keepdirectories
-keepparameternames

-renamesourcefileattribute SourceFile
-repackageclasses 'io.kyligence'
-allowaccessmodification
