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
####### Team Adviser:
* Alireza

change something

[link](test.txt)


# Run the Experiment
## 1. Dataset Format
A dataset contains a list of file, each file contains a list of integers, comma separated. Integers are drawn from a domain U.

***Note:*** A toy dataset with domain U=8 can be found at: *wavelet/src/main/resource/toydataset.txt*
## 2. Build jar file with maven
Navigate to wavelet source code, and execute:
> mvn clean package

The jar file **wavelet-0.0.1-SNAPSHOT.jar** will be created in folder ./target

## 3. Run jar as a Flink job
 **wavelet-0.0.1-SNAPSHOT.jar** can be submitted to Flink to run.
 There are 5 algorithms implemented. The following describe parameters that could be used to run the program smoothly.
 1. SendV
**Entry Class:** main.java.calculation.exact.sendv.SendV
**Program Arguments:** /path/to/input/file numLevels k /path/to/output/file
2. SendCoef
**Entry Class:** main.java.calculation.exact.sendcoef
**Program Arguments:** /path/to/input/file numLevels k mapperOption /path/to/output/file
3.  BasicS
**Entry Class:** main.java.calculation.appro.BasicSample
**Program Arguments:** /path/to/input/file numLevels k epsilon /path/to/output/file
4. ImprovedS
**Entry Class:** main.java.calculation.appro.ImprovedSample
**Program Arguments:** 
5. TwoLevelS
**Entry Class:** main.java.calculation.appro.TwoLevelSample
**Program Arguments:** 