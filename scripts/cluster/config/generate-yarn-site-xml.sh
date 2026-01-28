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
  <!-- Memory settings for NodeManagers -->
  <!-- Adjust based on actual RAM: use ~80% of physical memory -->
  <!-- For 16GB RAM nodes, use 12288; for 32GB nodes, use 24576 -->
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>16384</value>
  </property>
  <property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>512</value>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>16384</value>
  </property>
  <!-- CPU cores per NodeManager (adjust based on actual cores) -->
  <property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>8</value>
  </property>
  <!-- Virtual memory check disabled (often causes issues) -->
  <property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
  </property>
  <!-- Reduce log aggregation overhead -->
  <property>
    <name>yarn.log-aggregation-enable</name>
    <value>true</value>
  </property>
</configuration>
EOF

echo "yarn-site.xml generated at $OUTPUT_DIR"
