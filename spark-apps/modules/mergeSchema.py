import datetime, os, time, json
from stat import S_ISREG, ST_CTIME, ST_MODE
from pyspark.sql.functions import lit
from pyspark.sql.types import StructField, StructType, StringType

class MergeSchemaHelper:
    
    def get_modified_partitions(self, dir_path, ts_filter):
        """
        Input:
        - dir_path: Entity path containing partitions
        - ts_filter: Timestamp to filter the partitions returned (filter by last modification)
        
        Output: List
        Returns a list of partition directories that were modified after the ts_filter provided as parameter.
        """
        
        # All directories inside the dir_path that matches partition pattern
        lst_dir = [d for d in os.listdir(dir_path) if d.find("=") != -1]
        
        if lst_dir != []:
            # Filter based on modification date
            return sorted([(dir, round(os.stat(dir_path + "/" + dir).st_mtime * 1000)) for dir in lst_dir if round(os.stat(dir_path + "/" + dir).st_mtime * 1000) > ts_filter])
        else:
            return []
        
    def get_last_read_ts(self, file_path):
        """
        Input:
        - file_path: Complete file path of the file used to control the partition read.
        
        Output: Integer
        Returns the timestamp of the last time that a partition was read.
        """
        
        if os.path.exists(file_path):
            # Read control file
            with open(file_path) as inputfile:
                ctrl_file = json.load(inputfile)
                last_modified_ts_ctrl = ctrl_file["last_read_ts"]
        else:
            last_modified_ts_ctrl = 0
            
        return last_modified_ts_ctrl

    def set_last_read_ts(self, file_path):
        """
        Input:
        - file_path: Complete file path of the file used to control the partition read.
        
        Output: None
        Creates or overwrite the file used to control the partition read.
        """
        
        # Get only the path without file
        path = "/".join(file_path.split("/")[:-1])
        
        # Check if path exist. If not, create
        if not os.path.exists(path):
            os.makedirs(path)

        # Save ts
        with open(file_path, "w+") as outfile:
            json.dump({"last_read_ts":round(time.time() * 1000)}, outfile)

        return

    def convert_columns_to_string(self, schema, parent = "", lvl = 0):
        """
        Input:
        - schema: Dataframe schema as StructType
        
        Output: List
        Returns a list of columns in the schema casting them to String to use in a selectExpr Spark function.
        """
        
        lst=[]
        
        for x in schema:
            if lvl > 0 and len(parent.split(".")) > 0:
                parent = ".".join(parent.split(".")[0:lvl])
            else:
                parent = ""
        
            if isinstance(x.dataType, StructType):
                parent = parent + "." + x.name
                nested_casts = ",".join(self.convert_columns_to_string(x.dataType, parent.strip("."), lvl = lvl + 1))
                lst.append("struct({nested_casts}) as {col}".format(nested_casts=nested_casts, col=x.name))
            else:
                if parent == "":
                    lst.append("cast({col} as string) as {col}".format(col=x.name))
                else:
                    lst.append("cast({parent}.{col} as string) as {col}".format(col=x.name, parent=parent))
                    
        return lst


class MergeSchema:

    def __init__(self, spark, sc):
        self.spark = spark
        self.sc = sc

    def is_schema_equal(self, path1, path2, file_format):
        """
        Input:
        - path1: Complete Parquet file path
        - path2: Complete Parquet file path
        - file_format: File format (parquet or avro)

        Output: Bool
        Returns true if path1 and path2 have the same schema.
        """
        
        df1 = self.spark.read.format(file_format).load(path1).limit(0)
        df2 = self.spark.read.format(file_format).load(path2).limit(0)
        
        return df1.schema == df2.schema

    def identify_schema_changes(self, path, file_format, partition_list = []):
        """
        Input:
        - path: Entity path containing partitions
        - file_format: File format (parquet or avro)
        
        Output: Dict
        Returns a dict where each key represents a different schema with the initial path and final 
        path containing that schema version, delimiting the boundaries of that version.
        """
        
        dict = {}
        idx = 0
        
        for dir in sorted([d for d in os.listdir(path) if d.find("=") != -1]) if partition_list == [] else partition_list:
            # Add new key if not exist in dict
            if idx not in dict:
                dict[idx] = {"init_path": path + "/" + dir, "final_path": path + "/" + dir}
            # When schema is different, add new key to dict
            elif not self.is_schema_equal(dict[idx]["init_path"], path + "/" + dir, file_format):
                idx = idx + 1
                dict[idx] = {"init_path": path + "/" + dir, "final_path": path + "/" + dir}
            # When schema is equal, update final_path
            else:
                dict[idx]["final_path"] = path + "/" + dir      

        return dict

    def merge_schemas(self, dir_path, file_format, mode, ctrl_file):
        """
        Input: 
        - dir_path: Entity path containing partitions.
        - file_format: File format to read (parquet or avro)
        - mode: "I" for incremental or "F" for full
        - ctrl_file: Complete file path of the file used to control the last time the path was read.
        
        Output: JSON RDD
        If mode = I returns a JSON RDD containing the union of all partitions modified since the timestamp in control partition read.
        If mode = F returns a JSON RDD containing the union of all partitions.
        All columns are converted to String in the returning RDD.
        """
        
        merge_schema_helper = MergeSchemaHelper()

        idx = 0
        last_modified_ts_ctrl = 0 if mode == "F" else merge_schema_helper.get_last_read_ts(ctrl_file)
        
        # Check if there are new files to process
        if merge_schema_helper.get_modified_partitions(dir_path, last_modified_ts_ctrl) != []:
            lst_dir = [dir for dir, last_mod_ts in merge_schema_helper.get_modified_partitions(dir_path, last_modified_ts_ctrl)] 

            try:
                if len(self.identify_schema_changes(dir_path, file_format, lst_dir).keys()) > 1:
                    print("Different schemas identified:")
                    print(json.dumps(self.identify_schema_changes(dir_path, file_format, lst_dir), indent = 4))
                
                print("\nProcessing files:")
                # Read each directory and create a JSON RDD making a union of all directories
                for dir in lst_dir:
                    print("idx: " + str(idx) + " | path: " + dir_path + "/" + dir)

                    # Get schema
                    schema = self.spark.read.format(file_format).load(dir_path + "/" + dir).limit(0).schema

                    # Read file converting to string
                    df_temp = (self.spark.read
                                .format(file_format)
                                .load(dir_path + "/" + dir)
                                .selectExpr(merge_schema_helper.convert_columns_to_string(schema))
                                .withColumn(dir.split("=")[0], lit(dir.split("=")[1]))
                            )

                    # Convert to JSON to avoid error when union different schemas
                    if idx == 0:
                        rdd_json = df_temp.toJSON()
                    else:
                        rdd_json = rdd_json.union(df_temp.toJSON())

                    idx = idx + 1

                # Set Timestamp in control file
                merge_schema_helper.set_last_read_ts(ctrl_file)

                return rdd_json

            except Exception as e:
                print("Unexpected error: ", e)
                    
        else:
            return None