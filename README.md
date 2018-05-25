# aws-bucket-test

Python scripts to inventory bucket depression directory then predict and evaluate results
D. Joachim, 5/23/18

>> Use AWSBucketDataSet.py to create an inventory .csv file (full_data_set.csv) from an AWS bucket. In addition a list only file (files_data_set.csv) is generated for testing. 

1) install the Sox utility
   You may need to change the path in the script pointing to the sox utilities. The default is
   /usr/local/bin/sox and /usr/local/bin/soxi
   
2) install (pip install) the following dependencies if not present:
  csv, sys, os, datetime, boto, json, math, sox, datetime, re, pandas, numpy, timeit

3) create an info file with the following content:

  line 1: AWS access key ID
  line 2: AWS secret access key
  line 3: AWS bucket [sondefoobucket]
  line 4: AWS dataset directory [djoachim/data/2017/amzjaguar]
  line 5: local directory for outputs

4) run the utility from the command line:
  python AWSBucketDataSet.py complete-path-to-info-file
  
  
It takes a few minutes to run (1-5 minutes per 350 files)
 
 
>> Use AWSBucketPredict.py to output a .csv file of predictions. The script reads the "files_data_set.csv" file from the output directory, performs the predictions, then produces a prediction file (predictions.csv) in the same directory.

1) perform steps 1-3 specified above for the AWSBucketDataSet.py script

2) run the AWSBucketPredict script
  python AWSBucketPredict.py
