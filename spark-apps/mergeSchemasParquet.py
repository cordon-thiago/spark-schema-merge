from pyspark.sql import SparkSession
import datetime
from modules.dataGenerator import DataGenerator
from modules.mergeSchema import MergeSchemaHelper, MergeSchemaParquet
from modules import helpers
from pyspark.sql.functions import col, when

# Spark session & context
spark = (SparkSession
         .builder
         .master("spark://spark:7077")
         .appName("merge-schema-parquet-driver")
         .getOrCreate())

# Set dynamic partitions to overwrite only the partition in DF
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

sc = spark.sparkContext

sc.setLogLevel("WARN")

##################################
# Generate test data
##################################
# Set up parameters
entity = "test_data_parquet"
data_path = "/home/spark-data/raw/{}".format(entity)
num_rows = 10

# Generate test data
data_gen = DataGenerator(spark, sc)
data_gen.gen_data_simple_schema(data_path, datetime.date(2020,1,1), num_rows)
data_gen.gen_data_add_nested_struct(data_path, datetime.date(2020,2,1), num_rows)
data_gen.gen_data_add_columns(data_path, datetime.date(2020,3,1), num_rows)
data_gen.gen_data_change_datatype_add_struct(data_path, datetime.date(2020,4,1), num_rows)
data_gen.gen_data_change_column_name(data_path, datetime.date(2020,5,1), num_rows)
data_gen.gen_data_remove_column(data_path, datetime.date(2020,6,1), num_rows)


##################################
# Run Merge Schema
##################################
# Set up parameters
ctrl_file = "/home/spark-data/raw/last_read_control/{}.json".format(entity)
merge_helper = MergeSchemaHelper()
last_read_ts = merge_helper.get_last_read_ts(ctrl_file)

# Run Merge Schema
merge_parquet = MergeSchemaParquet(spark, sc)

# Read partitions and merge schemas
if merge_helper.get_modified_partitions(data_path, last_read_ts) != []:
    df = spark.read.json(merge_parquet.merge_schemas_incremental(data_path, ctrl_file))
else:
    print("No new files to process")

# Check schema
print("\n")
print("Schema after merging files:")
df.printSchema()

##################################
# Save data with new schema
##################################
#data_path_dest = '/home/jovyan/spark-data/raw_schema_merged/{}'.format(entity)
data_path_dest = '/home/spark-data/raw_schema_merged/{}'.format(entity)
df.write.partitionBy('date').mode('overwrite').parquet(data_path_dest)

##################################
# Test data after schema merge
##################################
# Read merged data
df_merged = spark.read.option("mergeSchema", "true").parquet(data_path_dest)

# Flatten DF
df_merged_flat = df_merged.selectExpr(helpers.flatten(df_merged.schema))

# Count nulls by partition and column
print("\n")
print("Count nulls by partition and column:")
df_merged_flat.select(
    ["date"] + 
    [when(col(c).isNull(), 1).otherwise(0).alias(c) for c in df_merged_flat.columns if c != "date"]
).groupBy("date").sum().sort("date").show()

# Count by partition
print("\n")
print("Count by partition:")
df_merged_flat.select(
    col("date")
).groupBy("date").count().sort("date").show()