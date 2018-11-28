from pyspark.sql import SparkSession

if __name__ == "__main__":

    session = SparkSession.builder.appName("Payload").getOrCreate()

    dataFrameReader = session.read

    responses = dataFrameReader \
        .option("header", "true") \
        .option("inferSchema", value = True) \
        .csv("payload/events.csv")

    print("=== Print out schema ===")
    responses.printSchema()
    
   

    session.stop()
