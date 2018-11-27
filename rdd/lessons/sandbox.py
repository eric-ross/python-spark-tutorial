from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("sandbox").setMaster("local[3]")
    sc = SparkContext(conf = conf)

    inputIntegers = list(range(1,200))
    integerRdd = sc.parallelize(inputIntegers)
    print(integerRdd.first())
    
  

