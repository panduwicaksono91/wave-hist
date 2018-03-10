# Building Histograms on Large Datasets in Apache Flink
## Big Data Project - Issue 12
##### Team Member:
* Dieu Tran Thi Hong (dieutth)
* Pandu Wicaksono (panduwicaksono91)
* Shibo Cheng (ShiboC)
##### Team Advisor:
Alireza Rezaei Mahdiraji  (alirezarm)

## Deliverables:
* Mid-term presentation 
* [Source code](./wavelet)
* Final presentation
* Report


## Run the Experiment
***Note:*** It is assumed that we are at the wavelet source code folder where we run all the commands in CLI.
Maven, Flink, and Java need to be installed to run the expriments.

### 1. Dataset Format
A dataset contains a list of file, each file contains a list of integers, comma separated. Integers are drawn from a domain U.

A toy dataset with domain U=8 can be found at: *wavelet/src/main/resource/toydataset.txt*
### 2. Build jar file with maven
Execute:
> mvn clean package //not skipping test

or 
> mvn clean package -DskipTests //skipping test

The jar file **wavelet-0.0.1-SNAPSHOT.jar** will be created in folder ./target

### 3. Submit jar to Flink and run as a Flink job
 **wavelet-0.0.1-SNAPSHOT.jar** can be submitted to Flink to run.
 Each algorithm is executed with a specific setting. 
The table below provides a summary of parameters, following by the setting needed to run each algorithm.
 
 | Parameter | Meaning |
|--|--|
| /path/to/input/file | Absolute path of input file (described in section dataset) |
|numLevels|Number of levels in the wavelet tree. The formula is: numLevels = log(U)/log(2) where U is domain size| 
|k|Number of top coefficients to be kept|
|mapperOption|Possible value: 1 or 2. 1 to use the Mapper1 which use array, 2 to use Mapper2 which use map, to compute wavelet tree. Mapper1 tends to perform more stable, but could lead to OOM if there is not enough resource|
|epsilon|Parameter for approximate algorithms that affect sample probability (see report for detail)|
|numParalellism|Number of mappers. This should be equal to parallelism set up when submitting job to Flink.|
|/path/to/output/file|Absolute path to output file for sinking|

#### 3.1. **SendV**
 
**Entry Class:** main.java.calculation.exact.sendv.SendV

**Program Arguments:** /path/to/input/file numLevels k /path/to/output/file

In our experiment, for example, we run this job in the [cluster](ibm-power-1.dima.tu-berlin.de) with the following command:

> /share/flink/flink-1.3.2/bin/flink run -p 40 -c main.java.calculation.exact.sendv.SendV ./target/wavelet-0.0.1-SNAPSHOT.jar /share/tmp/dataset245.txt 29 30 /share/tmp/sendV_result.txt


#### 3.2. **SendCoef**

**Entry Class:** main.java.calculation.exact.sendcoef.SendCoef

**Program Arguments:** /path/to/input/file numLevels k mapperOption /path/to/output/file

In our experiment, for example, we run this job in the [cluster](ibm-power-1.dima.tu-berlin.de) with the following command (in this example, mapperOption = 2, k = 30):

> /share/flink/flink-1.3.2/bin/flink run -p 40 -c  main.java.calculation.exact.sendcoef.SendCoef ./target/wavelet-0.0.1-SNAPSHOT.jar /share/tmp/dataset245.txt 29 30 2 /share/tmp/sendCoef_result.txt


#### 3.3.  **BasicS**

**Entry Class:** main.java.calculation.appro.BasicSample

**Program Arguments:** /path/to/input/file k epsilon /path/to/output/file

In our experiment, for example, we run this job in the [cluster](ibm-power-1.dima.tu-berlin.de) with the following command (in this example, k = 30, epsilon = 0.0001):

> /share/flink/flink-1.3.2/bin/flink run -p 40 -c  main.java.calculation.appro.BasicSample ./target/wavelet-0.0.1-SNAPSHOT.jar /share/tmp/dataset245.txt 30 0.0001 /share/tmp/basicS_result.txt


#### 3.4. **ImprovedS**

**Entry Class:** main.java.calculation.appro.ImprovedSample

**Program Arguments:** /path/to/input/file k epsilon /path/to/output/file

In our experiment, for example, we run this job in the [cluster](ibm-power-1.dima.tu-berlin.de) with the following command (in this example, k = 30, epsilon = 0.0001):

> /share/flink/flink-1.3.2/bin/flink run -p 40 -c  main.java.calculation.appro.ImprovedSample ./target/wavelet-0.0.1-SNAPSHOT.jar /share/tmp/dataset245.txt 30 0.0001 /share/tmp/improvedS_result.txt

#### 3.5. **TwoLevelS**

**Entry Class:** main.java.calculation.appro.TwoLevelSample

**Program Arguments:** /path/to/input/file k numParallelism epsilon /path/to/output/file

In our experiment, for example, we run this job in the [cluster](ibm-power-1.dima.tu-berlin.de) with the following command (in this example, k = 30, epsilon = 0.0001):

> /share/flink/flink-1.3.2/bin/flink run -p 40 -c  main.java.calculation.appro.ImprovedSample ./target/wavelet-0.0.1-SNAPSHOT.jar /share/tmp/dataset245.txt 30 40 0.0001 /share/tmp/twoLevelS_result.txt


### 4. SSE Calculation
To compute SSE, execute the following command from wavelet source code folder:

> flink run -c main.java.generator.CalculateSSE ./target/wavelet-0.0.1-SNAPSHOT.jar /path/to/generated/freq/file /path/to/original/freq/file /path/to/output/file

This assumes you can run flink from wavelet source code folder. Otherwise, replace _flink_ with _path/to/your/flink_.

### 5. Frequency Reconstruction
To generate frequency back from top k coefficients, execute the following command from wavelet source code folder:

> java -cp ./target/wavelet-0.0.1-SNAPSHOT.jar  main.java.generator.ReproduceFrequency /path/to/topKfile /path/to/reconstructed/freqs/file
