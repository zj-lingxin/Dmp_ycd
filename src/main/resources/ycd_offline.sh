source /etc/profile

PROJECT_HOME=/data/work/ycd
PROJECT_LOG_HOME=$PROJECT_HOME/logs

date_cur=`date +%Y%m%d`
time_cur=`date +%s`
year_month=`date +%Y%m`
day=`date +%d`


if [ ! -d $PROJECT_LOG_HOME/offline/$date_cur ];then
   mkdir $PROJECT_LOG_HOME/offline/$date_cur
 fi

echo "start!!"${store_id}" time:"${time_cur} >> $PROJECT_LOG_HOME/$date_cur/process.log

$SQOOP_HOME/bin/sqoop import --connect "jdbc:mysql://192.168.7.7:3306/retail_loan?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true" --username root --password password --query 'SELECT store_id,order_id,order_date,cigar_name,wholesale_price,purchase_amount,order_amount,money_amount,producer_name FROM  datag_yancao_historyOrderDetailData where $CONDITIONS'   -m 1  --target-dir /ycd/input/offline/$year_month/$day/tobacco_order_details --fields-terminated-by '\t' >> $PROJECT_LOG_HOME/offline/$date_cur/sqoop_ycd.log

#spark-submit  --master=local[4] --driver-memory 1G --executor-memory 2G  --executor-cores 4 --driver-java-options -DPropPath=$PROJECT_HOME/prop.properties  --jars /data/spark/lib/mysql-connector-java-5.1.35.jar  --class  com.asto.dmp.ycd.base.Main $PROJECT_HOME/dmp_ycd.jar $time_cur >> $PROJECT_LOG_HOME/offline/$date_cur/spark_ycd.log

hdfs dfs -ls /ycd/offline/output/$year_month/$day/

echo "exec end"