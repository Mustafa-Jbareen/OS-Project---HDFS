#!/bin/bash
################################################################################
# SCRIPT: generate-hdfs-site-xml.sh
# DESCRIPTION: Generates the hdfs-site.xml configuration file for Hadoop
# USAGE: bash generate-hdfs-site-xml.sh <output_directory>
# PREREQUISITES: None
################################################################################

set -e

OUTPUT_DIR=${1:-/home/mostufa.j/hadoop/etc/hadoop}
REPLICATION_FACTOR=2
HADOOP_DATA_DIR="/home/mostufa.j/hadoop_data"

cat > "$OUTPUT_DIR/hdfs-site.xml" <<EOF
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>$REPLICATION_FACTOR</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>$HADOOP_DATA_DIR/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>$HADOOP_DATA_DIR/datanode</value>
  </property>
  <property>
    <name>dfs.namenode.fs-limits.min-block-size</name>
    <value>131072</value>
  </property>
</configuration>
EOF

echo "hdfs-site.xml generated at $OUTPUT_DIR"
