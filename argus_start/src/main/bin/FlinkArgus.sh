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
PLUGINS_DIR=$ARGUS_HOME/plugins
READER_PLUGINS_DIR=$PLUGINS_DIR/reader
CHANNEL_PLUGINS_DIR=$PLUGINS_DIR/channel
WRITER_PLUGINS_DIR=$PLUGINS_DIR/writer
CLASS_PATH=".:$JAVA_HOME/lib:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar"
for jar in $JAR_HOME/*.jar
do
CLASS_PATH=$CLASS_PATH:$jar
done
echo $CLASS_PATH

for jar in $READER_PLUGINS_DIR/*.jar
do
  CLASS_PATH=$CLASS_PATH:$jar
done
for jar in $WRITER_PLUGINS_DIR/*.jar
do
  CLASS_PATH=$CLASS_PATH:$jar
done

for jar in $CHANNEL_PLUGINS_DIR/*.jar
do
  CLASS_PATH=$CLASS_PATH:$jar
done


CLASS_NAME=org.weiwan.argus.start.DataSyncStarter

echo "Flink Argus starting ..."
$JAVA_RUN -cp $CLASS_PATH $CLASS_NAME -Dlogback.configurationFile="${ARGUS_HOME/conf/logback.xml}" $@
#nohup $JAVA_RUN -cp $cp $CLASS_NAME $@ &
echo "Flink Argus started ..."
