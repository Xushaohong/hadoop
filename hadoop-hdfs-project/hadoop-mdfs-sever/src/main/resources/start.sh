#!/usr/bin/env bash

JAVA=${JAVA_HOME}/bin/java

GC_LOG_FILE=/data/home/mdfs/mdfs.gc

HEAP_OPTS="-Xmx4096m"
export VNN_DOMAIN=test

VERSION=2.8.5-tq-0.1.0



JVM_OPTS="-XX:+UseConcMarkSweepGC
-XX:CMSInitiatingOccupancyFraction=80 -XX:SurvivorRatio=2
-XX:+UseCMSCompactAtFullCollection
-XX:CMSFullGCsBeforeCompaction=2
-XX:+UseCMSCompactAtFullCollection
-XX:ParallelGCThreads=12
-XX:+PrintGCDetails
-XX:+PrintGCDateStamps
-XX:+CMSClassUnloadingEnabled
-XX:+PrintGCTimeStamps
-XX:+PrintGCApplicationStoppedTime
-verbose:gc
-Xloggc:${GC_LOG_FILE}"

JVM_OPTS="${HEAP_OPTS} ${JVM_OPTS}"

#${JAVA} -jar hadoop-mdfs-server-${VERSION}.jar ${JVM_OPTS}  "$@" &
nohup ${JAVA} -cp ${CLASSPATH}:${jarsLocation}/hadoop-mdfs-server-${VERSION}.jar ${JVM_OPTS} com.tencent.mdfs.Mdfs  "$@" &
