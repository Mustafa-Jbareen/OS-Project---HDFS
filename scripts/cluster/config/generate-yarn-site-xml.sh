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
  <!-- Optimized for 4-core, ~8GB RAM nodes (tapuz10-14) -->
  <!-- 7900 MB - 1500 MB for OS = 6400 MB available for YARN tasks -->
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>6400</value>
  </property>
  <property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>100</value>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>2048</value>
  </property>
  <!-- CPU cores per NodeManager (4 cores per node) -->
  <property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>4</value>
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
