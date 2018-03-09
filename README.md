Your topic title
Your group id
Your names and Github user names
Your mentor(s) name(s) and Github user names
A summary of your task (may be copied from the report)
Links to all your deliverables (please use relative paths):
Midterm presentation
Final presentation
Your report
Your sources, java doc, etc.

# Building Histograms on Large Datasets in Apache Flink
## Big Data Project - Issue 12
##### Team Member:
* Dieu Tran (dieutth)
* Pandu Wicaksono (panduwicaksono91)
* Shibo Cheng (XXX)
##### Team Advisor:
* Alireza

Deliverables:
* Mid-term presentation
* Source code
* Final presentation
* Report
* [link](test.txt)


# Run the Experiment
## 1. Dataset Format
A dataset contains a list of file, each file contains a list of integers, comma separated. Integers are drawn from a domain U.

***Note:*** A toy dataset with domain U=8 can be found at: *wavelet/src/main/resource/toydataset.txt*
## 2. Build jar file with maven
Navigate to wavelet source code, and execute:
> mvn clean package

The jar file **wavelet-0.0.1-SNAPSHOT.jar** will be created in folder ./target

## 3. Submit jar to Flink and run as a Flink job
 **wavelet-0.0.1-SNAPSHOT.jar** can be submitted to Flink to run.
 Each algorithm is executed with a specific setting. 
The following provides a summary of parameters, following by the setting needed to run each algorithm.
 
 | Parameter | Meaning |
|--|--|
| /path/to/input/file | Absolute path of input file (described in section dataset) |
|numLevels|Number of levels in the wavelet tree. The formula is: numLevels = log(U)/log(2) where U is domain size| 
|k|Number of top coefficients to be kept|
|mapperOption|Possible value: 1 or 2. 1 to use the Mapper1 which use array, 2 to use Mapper2 which use map, to compute wavelet tree. Mapper1 tends to perform more stable, but could lead to OOM if there is not enough resource|
|epsilon|Parameter for approximate algorithms that affect sample probability (see report for detail)|
|numParalellism|Number of mappers. This should be equal to parallelism set up when submitting job to Flink.|
|/path/to/output/file|Absolute path to output file for sinking|

 1. **SendV**
 
**Entry Class:** main.java.calculation.exact.sendv.SendV

**Program Arguments:** /path/to/input/file numLevels k /path/to/output/file

2. **SendCoef**

**Entry Class:** main.java.calculation.exact.sendcoef

**Program Arguments:** /path/to/input/file numLevels k mapperOption /path/to/output/file

3.  **BasicS**

**Entry Class:** main.java.calculation.appro.BasicSample

**Program Arguments:** /path/to/input/file k epsilon /path/to/output/file

4. **ImprovedS**

**Entry Class:** main.java.calculation.appro.ImprovedSample

**Program Arguments:** /path/to/input/file k epsilon /path/to/output/file

5. **TwoLevelS**

**Entry Class:** main.java.calculation.appro.TwoLevelSample

**Program Arguments:** /path/to/input/file k numParallelism epsilon /path/to/output/file
