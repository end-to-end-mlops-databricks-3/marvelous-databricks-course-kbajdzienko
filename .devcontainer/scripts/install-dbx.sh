curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks completion bash > /etc/bash_completion.d/databricks

#Install Apache Spark with Hadoop
curl -fsSL https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz | tar xz -C /opt/
ln -s /opt/spark-3.4.1-bin-hadoop3 /opt/spark
# Set environment variables for Spark
echo 'export SPARK_HOME=/opt/spark' >> /etc/bash.bashrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> /etc/bash.bashrc
