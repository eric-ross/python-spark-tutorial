#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Configure the necessary Spark environment
import os
import sys

spark_home = os.environ.get('SPARK_HOME', None)
sys.path.insert(0, spark_home + "/python")

# Add the py4j to the path.
# You may need to change the version number to match your install -- currently using spark 2.4
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.7-src.zip'))

#os.environ['PYSPARK_SUBMIT_ARGS']="--jars /work/ericr/spark/sparkdev/postgresql.jar --executor-memory 40g --executor-cores 16 pyspark-shell"

# Initialize PySpark to predefine the SparkContext variable 'sc'
#execfile(os.path.join(spark_home, 'python/pyspark/shell.py'))


# In[2]:


from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import flatten, explode, col
import json


# In[3]:


spark = SparkSession.builder.appName('Basic').config("spark.executor.extraJavaOptions","--executor-memory 40G --executor-cores 40 --driver-class-path $SPARK_HOME/postgresql.jar").getOrCreate()


# In[4]:


dbconfig = {"url": "jdbc:postgresql://localhost/qxedb",
            "dbtable": "gateway_eventnotification",
            "user": "hermes",
            "password": "mysecret",
            "driver": "org.postgresql.Driver"}


# In[5]:


spark


# In[6]:


df = spark.read.jdbc(url="jdbc:postgresql://localhost/qxedb", 
                      table="gateway_eventnotification",
                      properties=dbconfig)


# In[7]:


sqlcontext = SQLContext(spark.sparkContext)


# In[8]:


df.printSchema()


# In[9]:


payload_df = sqlcontext.read.json(df.rdd.map(lambda r: r.payload))


# In[10]:


payload_df.printSchema()


# In[11]:


versions = payload_df.select("version")


# In[12]:


versions.show()


# In[13]:


x = payload_df.select(explode(payload_df.events).alias("event"))


# In[14]:


x.head(1)


# In[15]:


flx = x.filter("event.eventCategory == 'systemError'").select("event.eventDetail.eventCode")


# In[16]:


counts = flx.groupBy("eventCode").count().sort("count", ascending=False).collect()


# In[17]:


systemErrors = x.filter("event.eventCategory == 'systemError'")


# In[18]:


counts


# In[19]:


systemErrors.show(2,truncate=200)


# In[20]:


x.printSchema()


# In[32]:


y = x.select("event.eventCategory").groupBy("eventCategory").count().sort("count",ascending=False)


# In[33]:


y.show(200)


# In[23]:


for t in y.groupby(['eventCategory']).count().collect():
    print(t)


# In[24]:


fwall = x.select(["event.eventCategory","event.eventDetail.firmwareVersion","event.eventDetail.firmwareAssert.code"])


# In[25]:


fwasserts = fwall.filter("eventCategory == 'systemError'")


# In[27]:


fwasserts.printSchema()


# In[28]:


fwasserts.count()

