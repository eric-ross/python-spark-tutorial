 val opts = Map(
  "url" -> "jdbc:postgresql://localhost/qxedb",
  "dbtable" -> "gateway_eventnotification",
  "user" -> "hermes",
  "password" -> "mysecret")

val df = spark.read.format("jdbc").options(opts).load

val myRdd = df.select("payload").rdd.map(row=>row.toString())
val jsonRdd = spark.sqlContext.read.json(myRdd)
