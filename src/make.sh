mkdir preCreat
javac -classpath $HBASE_HOME/hbase-0.90.2.jar:$HBASE_HOME/lib/*:$HADOOP_HOME/hadoop-core-*.jar:$HADOOP_HOME/lib/* -d preCreat/ PreCreateTable.java
jar -cvf PreCreateTable.jar -C preCreat/ .