from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Belajar Spark SQL dengan python") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

    df = spark.read.json("examples/src/main/resources/driftMonsters.json")
    df.show()

    df.createOrReplaceTempView("driftMonsters")

    sqlDFAll = spark.sql("SELECT * FROM driftMonsters")

    sqlDFAll.show()

    sqlDFDriftS = spark.sql("SELECT NUM, KodeMobil, name, merek FROM driftMonsters WHERE driftrating='S'")
    sqlDFDriftC = spark.sql("SELECT NUM, KodeMobil, name, merek, weight FROM driftMonsters WHERE driftrating='C'")
    sqlDFLWeight = spark.sql("SELECT NUM, KodeMobil, name, merek, driftrating FROM driftMonsters WHERE weight<1200")
    sqlDFToyoda = spark.sql("SELECT NUM, KodeMobil, name, driftrating, weight FROM driftMonsters WHERE merek='Toyota'")
    sqlDFMazdaAND = spark.sql("SELECT NUM, KodeMobil, name, driftrating, weight FROM driftMonsters WHERE merek='Mazda' AND weight>1300")
    sqlDFDriftS.show()
    sqlDFDriftC.show()
    sqlDFLWeight.show()
    sqlDFToyoda.show()
    sqlDFMazdaAND.show()