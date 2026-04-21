#!/bin/bash
ZOOKEEPER_HOME=/opt/zookeeper
CLASSPATH=$ZOOKEEPER_HOME/conf:$ZOOKEEPER_HOME/lib/*

java -cp $CLASSPATH org.apache.zookeeper.server.quorum.QuorumPeerMain $ZOOKEEPER_HOME/conf/zoo.cfg
