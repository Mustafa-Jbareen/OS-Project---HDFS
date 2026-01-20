#!/bin/bash
################################################################################
# SCRIPT: generate-mapred-site-xml.sh
# DESCRIPTION: Generates the mapred-site.xml configuration file for Hadoop
# USAGE: bash generate-mapred-site-xml.sh <output_directory>
# PREREQUISITES: None
################################################################################

set -e

OUTPUT_DIR=${1:-/home/mostufa.j/hadoop/etc/hadoop}
HADOOP_HOME="/home/mostufa.j/hadoop"

cat > "$OUTPUT_DIR/mapred-site.xml" <<EOF
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.env</name>
    <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
  </property>
</configuration>
EOF

echo "mapred-site.xml generated at $OUTPUT_DIR"