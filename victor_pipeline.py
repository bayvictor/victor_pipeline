from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row, HiveContext, SparkSession 
import pyspark.sql.functions as func
from pyspark.sql.window import Window
import time as pytime
import os, sys
import re
#from pyspark import *
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql import functions as F

def get_df_with_appname2( app_name, input_file ):
 
    input_file = sys.argv[1]  # input file
    spark = SparkSession.builder.appName( app_name ).getOrCreate()

    # Read in csv files
    lineDF = spark.read.option("header", "true").option("inferSchema", True).csv(input_file)
    return lineDF

def sum_col(df, col):
  return df.select(F.sum(col)).collect()[0][0]

def avg_col(df, col): 
  ss = sum_col( df, col )
  total_items = df.count()
  ret = float(ss)/total_items  
  return(ret)

if __name__ == "__main__":

    # Check the number of arguments
    if len(sys.argv) != 2:
        print("Usage: couchdbApp <input file> ")
        exit(-1)
    # foreachPartition applies a UDF on the local data partition

    df = get_df_with_appname2( 'chosenName', sys.argv[1]  )
    print("    1. Please provide the data types for the columns in the file. Transform the data into its proper data types (if needed).")
    type_dict = df.dtypes
    print( type_dict )
    #print (" type of columns[0]")
    #print (type_dict[0])
    #kk = type(type_dict[0])
    #print (kk)

    #'education_university.degree'
    total_items = df.count()
    print("total_items read-in=("+str(total_items)+")")
    dur_sum = sum_col( df, "duration" ) 
    avg_dur = dur_sum/((float)(total_items))
    print( "    2. What is the average duration of the campaigns?" )
    d1 = df.agg({"duration":"avg"}).show()
    print ("home brew average_duration="+str(avg_dur))
    print("    3. Please group the data by age groups: <20, 20-30, 30-40, 40-50, 50-60, >60" )
    age_df_lt20 = df.filter(df["age"]< 20 )
    age_df_ge20_lt30 = df.filter(df["age"] >= 20 ).filter(df["age"] <  30)
    age_df_ge30_lt40 = df.filter(df["age"] >= 30 ).filter(df["age"] <  40)
    age_df_ge40_lt50 = df.filter(df["age"] >= 40 ).filter(df["age"] <  50)
    age_df_ge50_le60 = df.filter(df["age"] >= 50 ).filter(df["age"] <= 60)
    age_df_gt60 = df.filter(df["age"] > 60 )

    print("     4. How many people in the age group 50+ have university degrees? " )
    #aa = df.filter(df["age"]>=50 ).filter(df["education_university_degree"]>0).show()
    aa = df.filter(df["age"]>=50 ).filter(df["education_university_degree"]>0).count()
    print("we are going to print all for ag>=50:")
    print( "age>=50, unniversity degree people number=" + str(aa) )
    #res = df.groupBy('age').agg({"age":"avg"}).show()

    print ("    5. Create a table that has the summary statistics (count, mean, min, max, and standard deviation) for the 12 jobs listed. Sort from high to low mean.")        

    job_list = []
    job_list.append("job_admin_")
    job_list.append("job_blue-collar")
    job_list.append("job_entrepreneur")
    job_list.append("job_housemaid")
    job_list.append("job_management")
    job_list.append("job_retired")
    job_list.append("job_self-employed")
    job_list.append("job_services")
    job_list.append("job_student")
    job_list.append("job_technician")
    job_list.append("job_unemployed")
    job_list.append("job_unknown")

    for job in job_list:
      df.describe( job ).show(5)

    print (" 6. How many people in the age group 20-30 have loans? How many do not?")
    loann = age_df_ge20_lt30.filter(age_df_ge20_lt30["loan_yes"]>0).count()
    no_loan = age_df_ge20_lt30.count() - loann
    print("6.1. has_loan="+str(loann))
    print("6.2. no_loan="+str(no_loan))

    print ("     7. Create a new column that calculates the age squared. ")
    df.withColumn("age", df.age*df.age).show(5)

    print ("     8. Remove all records of age group 50-60.")
    print (" before emptying age_df_ge50_le60 , count()="+ str( age_df_ge50_le60.count() ) )
    age_df_ge50_le60 = age_df_ge50_le60.filter(df["age"]> 0)    # so equvalent to zero records 
    #empty_df = age_df_ge50_le60[0:0]
    print (" after emptying it , count()="+ str( age_df_ge50_le60.count() ) )

    """
    print("last df via.agg:avg average of age")
    res = df.agg({"age":"avg"}).show()
    print("home brew avg age=("+ str( avg_col(df, "age" )) + ")" )
    #df.show()
    #print( df )
    print (res)
    """
    print("     9. Build a pipeline to automate all of the above. (use this question to distinguish yourself. Remember than Experian is 100% AWS) " )





