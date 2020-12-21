import sys
from pyspark.sql import SparkSession
from modules.mergeSchema import MergeSchemaHelper, MergeSchema
from modules import helpers
from pyspark.sql.functions import col, when

# Spark session & context
spark = (SparkSession
         .builder
         .master("spark://spark:7077")
         .appName("merge-schema-driver")
         .getOrCreate())

# Set dynamic partitions to overwrite only the partition in DF
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

sc = spark.sparkContext

sc.setLogLevel("WARN")

##################################
# Arguments
# sys.argv[1] = file_format
# sys.argv[2] = entity_base_name
# sys.argv[3] = mode (F = FULL, I = INCREMENTAL)
##################################

##################################
# Run Merge Schema
##################################
# Set up parameters
file_format = sys.argv[1]
entity = "{entity_base_name}_{file_format}".format(file_format=file_format, entity_base_name=sys.argv[2])
data_path = "/home/spark-data/raw/{}".format(entity)
ctrl_file = "/home/spark-data/raw/last_read_control/{}.json".format(entity)
mode = sys.argv[3]

merge_schema = MergeSchema(spark, sc)

# Read partitions and merge schemas
print("\n**********************************************************")
print("Merging Schemas...".format(file_format))
print("Entity:", entity)
print("Format:", file_format)
print("**********************************************************")
rdd_json = merge_schema.merge_schemas(data_path, file_format, mode, ctrl_file)
if rdd_json != None:
    df = spark.read.json(rdd_json)

    # Check schema
    print("\nSchema after merging files:")
    df.printSchema()

    ##################################
    # Save data with new schema
    ##################################
    data_path_dest = '/home/spark-data/raw_schema_merged/{}'.format(entity)
    print("\nSaving merged files in {}".format(data_path_dest))
    df.write.partitionBy('date').mode('overwrite').format(file_format).save(data_path_dest)

    ##################################
    # Test data after schema merge
    ##################################
    # Read merged data
    df_merged = spark.read.option("mergeSchema", "true").format(file_format).load(data_path_dest)

    # Flatten DF
    df_merged_flat = df_merged.selectExpr(helpers.flatten(df_merged.schema))

    # Count nulls by partition and column
    print("\nCount nulls by partition and column:")
    df_merged_flat.select(
        ["date"] + 
        [when(col(c).isNull(), 1).otherwise(0).alias(c) for c in df_merged_flat.columns if c != "date"]
    ).groupBy("date").sum().sort("date").show()

    # Count by partition
    print("\nCount by partition:")
    df_merged_flat.select(
        col("date")
    ).groupBy("date").count().sort("date").show()
else:
    print("No new files to process")
    

