#!/usr/bin/env bash
while getopts "p:b:" arg #选项后面的冒号表示该选项需要参数
do
        case $arg in
             p)
                port=$OPTARG;
                #echo "p's arg:$OPTARG"; #参数存在$OPTARG中
                ;;
             b)
                echo "b"
                ;;
             c)
                echo "c"
                ;;
             ?)  #当有不认识的选项的时候arg为?
            echo "unkonw argument"
        exit 1
        ;;
        esac
done

#根据端口号查询对应的pid
pid=$(netstat -nlp | grep :$port | awk '{print $7}' | awk -F"/" '{ print $1 }');

#杀掉对应的进程，如果pid不存在，则不执行
if [  -n  "$pid"  ];  then
    echo "kill -9 ${pid}"
    kill  -9  $pid;
fi