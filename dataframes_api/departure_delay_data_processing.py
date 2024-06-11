from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Departure_Delay").getOrCreate()
csv_path = "data/departuredelays.csv"

df = spark.read.csv(csv_path, inferSchema=True, header=True)
df.show()

df.createOrReplaceTempView("us_delay_flights_tbl")

df2 = spark.sql("""select * from us_delay_flights_tbl where delay > 50 """)
df2.show()