from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()


(
    spark.read.csv("./data/list_of_numbers/sample.csv", header=True)
    .withColumn(
        "new_column", F.when(F.col("old_column") > 10, 10).otherwise(0)
    )
    .where("old_column > 8")
    .groupby("new_column")
    .count()
    .write.csv("updated_frequencies.csv", mode="overwrite")
)
