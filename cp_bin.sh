#!/bin/bash

#nodes=`cat ip.list` 

for node in `cat /etc/hosts | grep hadoop | awk '{print $1}'`
do
        echo "开始推送${node}"
        scp -P65508 -r $1 ${node}:~/apps
done


for node in `cat /etc/hosts | grep hadoop | awk '{print $1}'`
do
ssh -p 65508 -tt $node << EOF
cd ~/apps
tar -zxvf FlinkArgus-1.0.0.tar.gz
exit
EOF
echo $node
done
