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

JAR_HOME=$ARGUS_HOME/lib

CLASS_PATH=".:$JAVA_HOME/lib:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar"
for jar in $JAR_HOME/*.jar
do
CLASS_PATH=$CLASS_PATH:$jar
done
echo $CLASS_PATH


CLASS_NAME=org.weiwan.argus.start.DataSyncStarter

echo "Flink Argus starting ..."
$JAVA_RUN -cp $CLASS_PATH $CLASS_NAME $@
#nohup $JAVA_RUN -cp $cp $CLASS_NAME $@ &
echo "Flink Argus started ..."
