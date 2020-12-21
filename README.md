# spark-schema-merge
Spark app to merge different schemas.

# Setup and run via Jupyter Notebook

If you want to use Jupyter Notebook, follow these steps:

1 - Build docker image.
```
cd spark-schema-merge/docker-jupyter
docker build -t jupyter/all-spark-notebook-custom .
```

2 - Start docker container.
```
docker-compose up -d
```

3 - Get the notebook URL.
``` 
docker logs docker-jupyter_spark_1 
``` 
The output of the log contains the URL to the notebook:

![](./doc/notebook_url.png "Notebook URL")

4 - Run the notebook **mergeSchemasParquet.ipynb**


# Setup and run via Spark Cluster

If you want to use a Spark Cluster, follow these steps:

1 - Build docker image.
``` 
cd spark-schema-merge/docker-spark
docker build -t bitnami/spark:3 .
``` 

2 - Start docker container.
```
docker-compose up -d
```

3 - Run the spark application via spark-cubmit.
```
docker exec -it docker-spark_spark_1 spark-submit --master spark://spark:7077 /home/spark-apps/mergeSchemasParquet.py
```

4 - Optionally, you can access the spark UI to check the process running: http://localhost:8080

# Notes

If you already run one of the solutions above and want to run the other, clean the spark-data directory before. It will prevent from the error when creating the files: *java.io.IOException: Mkdirs failed to create file*.
```
cd spark-schema-merge/aux-scripts
sudo ./remove-files.sh
```