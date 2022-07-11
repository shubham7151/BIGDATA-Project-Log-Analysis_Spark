#!/usr/bin/env python

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType
import json
#### returns spark session
def createSparkSession(masterPartition : int, appName : str ):
    return SparkSession.builder.master('local['+str(masterPartition)+']').appName(appName).getOrCreate()

#### readData and return DataFrame
def loadDataAndGetDataFrame(sessionObj,fileType:str,location:str,header:bool,schema:StructType=None):
    return sessionObj.read.format(fileType).load(location,header=header,schema=schema)


def readSchema():
        try:
            with open('./resource/schema.json','r') as report:
                return json.load(report)
        except Exception as e:
            print(e)
