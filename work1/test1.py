# coding: utf-8
import os
import sys
import re


if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/home/mao/spark'

# Create a variable for our root path
SPARK_HOME = os.environ['SPARK_HOME']

# Add the PySpark/py4j to the Python Path
sys.path.insert(0, os.path.join(SPARK_HOME, "python", "build"))
sys.path.insert(0, os.path.join(SPARK_HOME, "python"))

############# spark version #################
from pyspark import  SparkContext

sc = SparkContext( 'local[4]', 'pyspark')
textFile = sc.textFile("/home/mao/code/spark/test1/bbb", 1)
textFile.flatMap(lambda line: re.split(' |\n|\[|\]|\(|\)|/|#|\.|\+|\-', str.lower(line)))\
                .filter(lambda word: len(word) > 1)\
                .map(lambda word: (word, 1))\
                .reduceByKey(lambda a, b: a + b)\
                .sortBy(lambda x: x[1], ascending=False)\
                .collect()
sc.stop()

############ python version #################
# def countWord(textContent):
#     rst = {}
#     for line in textContent:
#         words = re.split(' |\n|\[|\]|\(|\)|/|#|\.|\+|\-', str.lower(line))
#         for word in words:
#             if len(word) <= 1:
#                 continue
#             if word in rst:
#                 rst[word] = rst[word] + 1
#             else:
#                 rst[word] = 1
#
#     return sorted(rst.items(), key = lambda item: item[1], reverse=True)
#
# with open('bbb') as f:
#     print(len(countWord(f)))
