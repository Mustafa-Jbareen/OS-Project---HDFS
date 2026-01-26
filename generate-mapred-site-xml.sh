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
  <!-- Memory per map task - needs enough heap for WordCount -->
  <property>
    <name>mapreduce.map.memory.mb</name>
    <value>1536</value>
  </property>
  <property>
    <name>mapreduce.map.java.opts</name>
    <value>-Xmx1280m</value>
  </property>
  <property>
    <name>mapreduce.reduce.memory.mb</name>
    <value>2048</value>
  </property>
  <property>
    <name>mapreduce.reduce.java.opts</name>
    <value>-Xmx1536m</value>
  </property>
  <!-- JVM reuse: run multiple tasks per JVM to reduce startup overhead -->
  <property>
    <name>mapreduce.job.jvm.numtasks</name>
    <value>-1</value>
  </property>
  <!-- Increase AM memory for jobs with many tasks -->
  <property>
    <name>yarn.app.mapreduce.am.resource.mb</name>
    <value>2048</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.command-opts</name>
    <value>-Xmx1536m</value>
  </property>
  <!-- Sort settings -->
  <property>
    <name>mapreduce.task.io.sort.mb</name>
    <value>128</value>
  </property>
  <property>
    <name>mapreduce.task.io.sort.factor</name>
    <value>48</value>
  </property>
  <!-- Speculative execution off to save resources -->
  <property>
    <name>mapreduce.map.speculative</name>
    <value>false</value>
  </property>
  <property>
    <name>mapreduce.reduce.speculative</name>
    <value>false</value>
  </property>
</configuration>
EOF

echo "mapred-site.xml generated at $OUTPUT_DIR"