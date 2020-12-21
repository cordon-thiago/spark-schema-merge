FROM docker.io/bitnami/spark:3

USER root

############################
# Install python packages
############################
RUN pip install pyspark==3.0.1 \
    && pip install mimesis

############################
# Install avro dependency
############################
RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.0.1/spark-avro_2.12-3.0.1.jar --output /opt/bitnami/spark/jars/spark-avro_2.12-3.0.1.jar

USER 1001