sh run_qfrag_spark.sh cluster-spark.yaml search-configs.yaml

Start hdfs
bash /usr/local/hadoop/sbin/start-dfs.sh
bash /usr/local/hadoop/sbin/stop-dfs.sh

hdfs namenode -format

hdfs mkdir 
hdfs dfs -put <filename> /<dest>


STEP 1 : stop hadoop and clean temp files from hduser
sudo rm -R /tmp/*

