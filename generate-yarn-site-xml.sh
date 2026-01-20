#!/bin/bash
################################################################################
# SCRIPT: generate-yarn-site-xml.sh
# DESCRIPTION: Generates the yarn-site.xml configuration file for Hadoop
# USAGE: bash generate-yarn-site-xml.sh <output_directory>
# PREREQUISITES: None
################################################################################

set -e

OUTPUT_DIR=${1:-/home/mostufa.j/hadoop/etc/hadoop}
MASTER_NODE="tapuz14"

cat > "$OUTPUT_DIR/yarn-site.xml" <<EOF
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>$MASTER_NODE</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
EOF

echo "yarn-site.xml generated at $OUTPUT_DIR"