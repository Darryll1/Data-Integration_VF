FROM apache/airflow:2.7.3

USER root

# Installer Java
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean

# Définir JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Repasser à l'utilisateur airflow AVANT le pip install
USER airflow

# Installer les dépendances Python sous l'utilisateur airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
