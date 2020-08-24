#!/bin/bash


${FLINK_HOME}/bin/yarn-session.sh -n 1 -tm 1024m -jm 1024m -s 2 -qu root.users.easylife > /dev/null 2>&1 &


exit 0