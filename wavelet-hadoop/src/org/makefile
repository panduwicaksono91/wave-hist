CLASSPATH = -classpath /home/username/hadoop-0.20.2-release/hadoop-0.20.2-core.jar:/home/username/hadoop-0.20.2-release/commons-logging-1.1.1.jar:/home/username/hadoop-0.20.2-release/sched.jar

all: HadoopWavelet
	jar -cvf /home/username/hadoop-0.20.2-release/wavelet.jar -C bin/ . 

gcs: gcs.java PCoefficient.java prng.java
	javac -d bin/ gcs.java PCoefficient.java prng.java

BCWaveletTree: BCWaveletTree.java PCoefficient.java
	javac -d bin/ BCWaveletTree.java PCoefficient.java

SequentialRecordInputFormat: SequentialRecordInputFormat.java SequentialRecordReader.java
	javac $(CLASSPATH) -d bin/ SequentialRecordInputFormat.java SequentialRecordReader.java

SkipTextInputFormat: SkipTextInputFormat.java SkipLineRecordReader.java
	javac $(CLASSPATH) -d bin/ SkipTextInputFormat.java SkipLineRecordReader.java

RandomRecordInputFormat: RandomRecordInputFormat.java RandomRecordReader.java
	javac $(CLASSPATH) -d bin/ RandomRecordInputFormat.java RandomRecordReader.java

HadoopWavelet: HadoopWavelet.java TPUTp1Coefficient.java IntDouble.java TPUTCoefficient.java UIntWritable.java RandomRecordInputFormat SkipTextInputFormat BCWaveletTree gcs UIntArray.java SequentialRecordInputFormat
	javac $(CLASSPATH):bin/ -d bin/ HadoopWavelet.java TPUTp1Coefficient.java IntDouble.java TPUTCoefficient.java UIntWritable.java UIntArray.java

clean:
	/bin/rm -rf bin/*
