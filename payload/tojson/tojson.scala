 val opts = Map(
  "url" -> "jdbc:postgresql://localhost/qxedb",
  "dbtable" -> "gateway_eventnotification",
  "user" -> "hermes",
  "password" -> "mysecret")

val fdf = spark.read.format("jdbc").options(opts).load

val myRdd = fdf.select("payload").rdd.map(row=>row.toString())
val df = spark.sqlContext.read.json(myRdd)
