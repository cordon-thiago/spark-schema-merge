from mimesis import Person, Address
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType, FloatType

class DataGenerator:

    def __init__(self, spark, sc):
        self.spark = spark
        self.sc = sc

    def gen_data_simple_schema(self, data_path, partition_date, num_rows):
        """
        Input
        - data_path: path where the partition will be created (string)
        - partition_date: partition date to be created (date)
        - num_rows: number of rows to be generated (integer)

        This function creates a data sample with a simple schema
        """

        person = Person('en')
        
        # Create a simple schema
        schema_df = StructType(
            [
                StructField('identifier', StringType(), True),
                StructField('first_name', StringType(), True),
                StructField('last_name', StringType(), True),
                StructField('occupation', StringType(), True),
                StructField('age', IntegerType(), True),
                StructField('date', DateType(), True)
            ]
        )
        
        # generate data
        for _ in range(num_rows):
            df_temp = self.spark.createDataFrame([
                [
                    person.identifier(),
                    person.first_name(),
                    person.last_name(),
                    person.occupation(),
                    person.age(),
                    partition_date
                ]
            ], schema_df)

            try:
                df = df.union(df_temp)
            except:
                df = df_temp
        
        df.coalesce(1).write.partitionBy('date').mode('overwrite').parquet(data_path)
        
        print('Partition created: {data_path}/date={date}'.format(data_path=data_path,date=partition_date))
        print('# Rows:',df.count())
        print('Schema:')
        df.printSchema()
        print('\n')
        
        return

    def gen_data_add_nested_struct(self, data_path, partition_date, num_rows):
        """
        Input
        - data_path: path where the partition will be created (string)
        - partition_date: partition date to be created (date)
        - num_rows: number of rows to be generated (integer)

        This function creates a data sample adding a nested struct to the schema
        """

        person = Person('en')
        address = Address('en')
        
        # Create schema
        schema_address = StructType(
            [
                StructField('address', StringType(), True),
                StructField('city', StringType(), True),
                StructField('country', StringType(), True),
                StructField('state', StringType(), True),
                StructField('postal_code', StringType(), True)
            ]
        )

        schema_df = StructType(
            [
                StructField('identifier', StringType(), True),
                StructField('first_name', StringType(), True),
                StructField('last_name', StringType(), True),
                StructField('occupation', StringType(), True),
                StructField('age', IntegerType(), True),
                StructField('address', schema_address, True),
                StructField('date', DateType(), True)


            ]
        )
        
        # Generate data
        for _ in range(num_rows):
            df_temp = self.spark.createDataFrame([
                [
                    person.identifier(),
                    person.first_name(),
                    person.last_name(),
                    person.occupation(),
                    person.age(),
                    [
                        address.address(),
                        address.city(),
                        address.country(),
                        address.state(),
                        address.postal_code()
                    ],
                    partition_date
                ]
            ], schema_df)

            try:
                df = df.union(df_temp)
            except:
                df = df_temp
                
        df.coalesce(1).write.partitionBy('date').mode('overwrite').parquet(data_path)
        
        print('Partition created: {data_path}/date={date}'.format(data_path=data_path,date=partition_date))
        print('# Rows:',df.count())
        print('Schema:')
        df.printSchema()
        print('\n')
        
        return

    def gen_data_add_columns(self, data_path, partition_date, num_rows):
        """
        Input
        - data_path: path where the partition will be created (string)
        - partition_date: partition date to be created (date)
        - num_rows: number of rows to be generated (integer)

        This function creates a data sample adding columns to the schema
        """

        person = Person('en')
        address = Address('en')
        
        # Create schema
        schema_address = StructType(
            [
                StructField('address', StringType(), True),
                StructField('city', StringType(), True),
                StructField('country', StringType(), True),
                StructField('country_code', StringType(), True), #New column
                StructField('state', StringType(), True),
                StructField('postal_code', StringType(), True)
            ]
        )

        schema_df = StructType(
            [
                StructField('identifier', StringType(), True),
                StructField('first_name', StringType(), True),
                StructField('last_name', StringType(), True),
                StructField('occupation', StringType(), True),
                StructField('age', IntegerType(), True),
                StructField('address', schema_address, True),
                StructField('title', StringType(), True), #New column
                StructField('date', DateType(), True)


            ]
        )
        
        # Generate data
        for _ in range(num_rows):
            df_temp = self.spark.createDataFrame([
                [
                    person.identifier(),
                    person.first_name(),
                    person.last_name(),
                    person.occupation(),
                    person.age(),
                    [
                        address.address(),
                        address.city(),
                        address.country(),
                        address.country_code(),
                        address.state(),
                        address.postal_code()
                    ],
                    person.title(),
                    partition_date
                ]
            ], schema_df)

            try:
                df = df.union(df_temp)
            except:
                df = df_temp
                
        df.coalesce(1).write.partitionBy('date').mode('overwrite').parquet(data_path)
        
        print('Partition created: {data_path}/date={date}'.format(data_path=data_path,date=partition_date))
        print('# Rows:',df.count())
        print('Schema:')
        df.printSchema()
        print('\n')
        
        return

    def gen_data_change_datatype_add_struct(self, data_path, partition_date, num_rows):
        """
        Input
        - data_path: path where the partition will be created (string)
        - partition_date: partition date to be created (date)
        - num_rows: number of rows to be generated (integer)

        This function creates a data sample changing data types and adding a struct to the schema
        """

        person = Person('en')
        address = Address('en')
        
        # Create schema
        schema_street = StructType( #added
            [
                StructField('street_name', StringType(), True),
                StructField('latitude', FloatType(), True),
                StructField('longitude', FloatType(), True)
            ]
        )

        schema_address_details = StructType(
            [
                StructField('street', schema_street, True),
                StructField('number', IntegerType(), True)
            ]
        )


        schema_address = StructType(
            [
                StructField('address_details', schema_address_details, True),
                StructField('city', StringType(), True),
                StructField('country', StringType(), True),
                StructField('country_code', StringType(), True),
                StructField('state', StringType(), True),
                StructField('postal_code', IntegerType(), True) #datatype changed
            ]
        )

        schema_df = StructType(
            [
                StructField('identifier', StringType(), True),
                StructField('first_name', StringType(), True),
                StructField('last_name', StringType(), True),
                StructField('occupation', StringType(), True),
                StructField('age', IntegerType(), True),
                StructField('address', schema_address, True),
                StructField('title', StringType(), True),
                StructField('date', DateType(), True)


            ]
        )
        
        # Generate data
        for _ in range(num_rows):
            df_temp = self.spark.createDataFrame([
                [
                    person.identifier(),
                    person.first_name(),
                    person.last_name(),
                    person.occupation(),
                    person.age(),
                    [
                        [
                            [
                                address.street_name(),
                                float(address.latitude()),
                                float(address.longitude())
                            ],
                            int(address.street_number())
                        ],
                        address.city(),
                        address.country(),
                        address.country_code(),
                        address.state(),
                        int(address.postal_code())
                    ],
                    person.title(),
                    partition_date
                ]
            ], schema_df)

            try:
                df = df.union(df_temp)
            except:
                df = df_temp
                
        df.coalesce(1).write.partitionBy('date').mode('overwrite').parquet(data_path)
        
        print('Partition created: {data_path}/date={date}'.format(data_path=data_path,date=partition_date))
        print('# Rows:',df.count())
        print('Schema:')
        df.printSchema()
        print('\n')
        
        return

    def gen_data_change_column_name(self, data_path, partition_date, num_rows):
        """
        Input
        - data_path: path where the partition will be created (string)
        - partition_date: partition date to be created (date)
        - num_rows: number of rows to be generated (integer)

        This function creates a data sample changing column name
        """

        person = Person('en')
        address = Address('en')
        
        # Create schema
        schema_street = StructType(
            [
                StructField('street_name', StringType(), True),
                StructField('lat', FloatType(), True), #column renamed
                StructField('long', FloatType(), True) #column renamed
            ]
        )

        schema_address_details = StructType(
            [
                StructField('street', schema_street, True),
                StructField('number', IntegerType(), True)
            ]
        )


        schema_address = StructType(
            [
                StructField('address_details', schema_address_details, True),
                StructField('city', StringType(), True),
                StructField('country', StringType(), True),
                StructField('country_code', StringType(), True),
                StructField('state', StringType(), True),
                StructField('postal_code', IntegerType(), True)
            ]
        )

        schema_df = StructType(
            [
                StructField('identifier', StringType(), True),
                StructField('first_name', StringType(), True),
                StructField('last_name', StringType(), True),
                StructField('occupation', StringType(), True),
                StructField('age', IntegerType(), True),
                StructField('address', schema_address, True),
                StructField('title_name', StringType(), True), #column renamed
                StructField('date', DateType(), True)


            ]
        )
        
        # Generate data
        for _ in range(num_rows):
            df_temp = self.spark.createDataFrame([
                [
                    person.identifier(),
                    person.first_name(),
                    person.last_name(),
                    person.occupation(),
                    person.age(),
                    [
                        [
                            [
                                address.street_name(),
                                float(address.latitude()),
                                float(address.longitude())
                            ],
                            int(address.street_number())
                        ],
                        address.city(),
                        address.country(),
                        address.country_code(),
                        address.state(),
                        int(address.postal_code())
                    ],
                    person.title(),
                    partition_date
                ]
            ], schema_df)

            try:
                df = df.union(df_temp)
            except:
                df = df_temp
                
        df.coalesce(1).write.partitionBy('date').mode('overwrite').parquet(data_path)
        
        print('Partition created: {data_path}/date={date}'.format(data_path=data_path,date=partition_date))
        print('# Rows:',df.count())
        print('Schema:')
        df.printSchema()
        print('\n')
        
        return

    def gen_data_remove_column(self, data_path, partition_date, num_rows):
        """
        Input
        - data_path: path where the partition will be created (string)
        - partition_date: partition date to be created (date)
        - num_rows: number of rows to be generated (integer)

        This function creates a data sample removing some columns
        """

        person = Person('en')
        address = Address('en')
        
        schema_street = StructType(
            [
                StructField('street_name', StringType(), True)
                # StructField('lat', FloatType(), True), #column removed
                # StructField('long', FloatType(), True) #column removed
            ]
        )

        schema_address_details = StructType(
            [
                StructField('street', schema_street, True),
                StructField('number', IntegerType(), True)
            ]
        )


        schema_address = StructType(
            [
                StructField('address_details', schema_address_details, True),
                StructField('city', StringType(), True),
                StructField('country', StringType(), True),
                # StructField('country_code', StringType(), True), #column removed
                StructField('state', StringType(), True),
                StructField('postal_code', IntegerType(), True)
            ]
        )

        schema_df = StructType(
            [
                StructField('identifier', StringType(), True),
                StructField('first_name', StringType(), True),
                StructField('last_name', StringType(), True),
                StructField('occupation', StringType(), True),
                StructField('age', IntegerType(), True),
                StructField('address', schema_address, True),
                # StructField('title_name', StringType(), True), #column removed
                StructField('date', DateType(), True)


            ]
        )

        for _ in range(num_rows):
            df_temp = self.spark.createDataFrame([
                [
                    person.identifier(),
                    person.first_name(),
                    person.last_name(),
                    person.occupation(),
                    person.age(),
                    [
                        [
                            [
                                address.street_name()
                                #float(address.latitude()),
                                #float(address.longitude())
                            ],
                            int(address.street_number())
                        ],
                        address.city(),
                        address.country(),
                        #address.country_code(),
                        address.state(),
                        int(address.postal_code())
                    ],
                    #person.title(),
                    partition_date
                ]
            ], schema_df)

            try:
                df = df.union(df_temp)
            except:
                df = df_temp
                
        df.coalesce(1).write.partitionBy('date').mode('overwrite').parquet(data_path)
        
        print('Partition created: {data_path}/date={date}'.format(data_path=data_path,date=partition_date))
        print('# Rows:',df.count())
        print('Schema:')
        df.printSchema()
        print('\n')
        
        return