#!/usr/bin/env bash



echo "启动!!!"


# 调用Main.main方法启动

set -e

export ARGUS_HOME="$(cd "`dirname "$0"`"/..; pwd)"

# Find the java binary
if [ -n "${JAVA_HOME}" ]; then
  JAVA_RUN="${JAVA_HOME}/bin/java"
else
  if [ `command -v java` ]; then
    JAVA_RUN="java"
  else
    echo "JAVA_HOME is not set" >&2
    exit 1
  fi
fi

JAR_DIR=$ARGUS_HOME/lib/*
JAR_DIR=$JAR_DIR + $ARGUS_HOME/FlinkArgus-1.0.0.jar
CLASS_NAME=org.weiwan.argus.start.DataSyncStarter

echo "Flink Argus starting ..."
nohup $JAVA_RUN -cp $JAR_DIR $CLASS_NAME $@ &
echo "Flink Argus started ..."
