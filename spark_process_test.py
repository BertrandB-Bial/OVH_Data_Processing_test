from pyspark.sql import SparkSession
# from delta import configure_spark_with_delta_pip

jar_path = '/opt/spark/workdir/jar'
jars = [
    f'{jar_path}/hadoop-aws-3.3.2.jar',
    f'{jar_path}/aws-java-sdk-bundle-1.12.382.jar'
]

jars = ','.join(jars)

builder = SparkSession.builder \
    .appName('POC_OVH_Stago') \
    .config("spark.jars", jars) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.defaultFS", "s3a://poc-ovh-hps3-001/")

spark = builder.getOrCreate()
# spark = configure_spark_with_delta_pip(builder).getOrCreate()

read_path = 's3a://poc-ovh-hps3-001/dummy_data.csv'
df = spark.read.csv(read_path)

df.show()

write_path = 's3a://poc-ovh-hps3-001/dummy_data_delta'
df.write \
    .format('Delta') \
    .mode('overwrite') \
    .save(write_path)