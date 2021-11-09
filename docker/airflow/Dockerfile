FROM apache/airflow

USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME
RUN echo 'airflow:airflow' | chpasswd
USER airflow
COPY dse-5.1.24-1.noarch.rpm /app/spark-submit/dse-5.1.24-1.noarch.rpm

RUN apt-get install rpm dse-libcassandra-5.1.24-1 dse-libhadoop2-client-5.1.24-1 which procps -y && \
    rpm -i /app/spark-submit/dse-5.1.24-1.noarch.rpm && \ 
    echo 'airflow:airflow' | chpasswd && \ 
    echo 'root:password' | chpasswd

USER airflow

WORKDIR /app

COPY requirements.txt /app

RUN pip install --trusted-host pypi.python.org -r requirements.txt