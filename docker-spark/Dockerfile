FROM docker.io/bitnami/spark:3

USER root

############################
# Install python packages
############################
RUN pip install pyspark==3.0.1 \
    && pip install mimesis

USER 1001