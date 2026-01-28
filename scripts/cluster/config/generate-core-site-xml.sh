#!/bin/bash
################################################################################
# SCRIPT: generate-core-site-xml.sh
# DESCRIPTION: Generates the core-site.xml configuration file for Hadoop
# USAGE: bash generate-core-site-xml.sh <output_directory>
# PREREQUISITES: None
################################################################################

set -e

OUTPUT_DIR=${1:-/home/mostufa.j/hadoop/etc/hadoop}
MASTER_NODE="tapuz14"
NAMENODE_PORT=9000

cat > "$OUTPUT_DIR/core-site.xml" <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://$MASTER_NODE:$NAMENODE_PORT</value>
  </property>
  <property>
    <name>dfs.client.use.datanode.hostname</name>
    <value>true</value>
  </property>
  <property>
    <name>dfs.datanode.use.datanode.hostname</name>
    <value>true</value>
  </property>
</configuration>
EOF

echo "core-site.xml generated at $OUTPUT_DIR"
