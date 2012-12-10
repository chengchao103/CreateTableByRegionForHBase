mkdir preCreat
javac -classpath $HBASE_HOME/hbase-*.jar:$HBASE_HOME/lib/*:$HADOOP_HOME/hadoop-core-*.jar:$HADOOP_HOME/lib/* -d preCreat/ PreCreateTableFromFile.java
jar -cvf PreCreateTable.jar -C preCreat/ .