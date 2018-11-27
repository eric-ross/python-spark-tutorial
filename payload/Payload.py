from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pprint import pprint

if __name__ == "__main__":

    session = SparkSession.builder.appName("Payload").getOrCreate()

    dataFrameReader = session.read

    responses = dataFrameReader \
        .option("header", "true") \
        .option("inferSchema", value = True) \
        .json("payload/payload500.json")

    print("=== Print out schema ===")
    responses.printSchema()
    pprint(responses.columns)
    df = responses.select(explode('events'))
    df.show()
    #pprint(df.collect())
    #responses.show()

    
   

    session.stop()
