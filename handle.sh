#! /bin/bash


source_dir="$1"

function read_dir() {
for file in `ls $1`
do
	if [ -d $1"/"$file ]; # 判断是否是目录，是目录则递归
	then
		read_dir $1"/"$file
	elif [ -f $1"/"$file ]; # 判断是否是文件，输出屏幕
	then
		echo "文件:" $1"/"$file
#		if [ ! -d "$source_dir/tmp/easylife_dw/$1"  ];then
#  			mkdir $source_dir/tmp/easylife_dw/$1 -p
#		else
#  			echo "dir exist"
#		fi
		dos2unix -k "$source_dir/$1/$file" #"$source_dir/tmp/easylife_dw/$1/$file"
	else
		echo $1"/"$file
	fi
done
}

read_dir $source_dir

