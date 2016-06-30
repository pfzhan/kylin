-dontshrink
-dontoptimize
-dontnote
-ignorewarnings

-keepnames class * implements java.io.Serializable
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

-keep class !io.kyligence.** {*;}

-keep enum io.kyligence.kap.**,io.kyligence.kap.**$** { 
    **[] $VALUES;
	public *;
} 

-keep class io.kyligence.kap.rest.** {public *;}
-keepparameternames
-keepattributes Exceptions,Signature,Deprecated,LineNumberTable,*Annotation*,EnclosingMethod 
-keepdirectories 
