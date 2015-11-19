source /etc/profile

PROJECT_HOME=/data/work/ycd
PROJECT_LOG_HOME=$PROJECT_HOME/logs
HDFS_PATH=/ycd/online

store_id=$1
date_cur=`date +%Y%m%d`
time_cur=`date +%s`
year_month=`date +%Y%m`
day=`date +%d`

if [ -z "$1" ]; then
    echo "1st argument is empty!"
    exit
fi
if [ ! -d $PROJECT_LOG_HOME/$date_cur ];then
   mkdir $PROJECT_LOG_HOME/$date_cur
   echo "Created dir for ycd store Id ["$store_id"]!"
 fi
#transfer table alipay_account_trade

echo "start!!"${store_id}" time:"${time_cur} >> $PROJECT_LOG_HOME/$date_cur/process.log

hdfs dfs -rm -r $HDFS_PATH/input/tobacco_order_details

$SQOOP_HOME/bin/sqoop import --connect jdbc:mysql://192.168.7.7:3306/retail_loan --username root --password password --query 'SELECT store_id,order_id,order_date,cigar_name,wholesale_price,purchase_amount,order_amount,money_amount,producer_name FROM  datag_yancao_historyOrderDetailData WHERE store_id = "'$store_id'" and $CONDITIONS '   -m 1  --target-dir $HDFS_PATH/input/tobacco_order_details --fields-terminated-by '\t' &> $PROJECT_LOG_HOME/$date_cur/sqoop_ycd[$store_id].log

spark-submit  --master=local[4] --driver-memory 1G --executor-memory 2G  --executor-cores 4 --driver-java-options -DPropPath=$PROJECT_HOME/prop.properties  --jars /data/spark/lib/mysql-connector-java-5.1.35.jar  --class  com.asto.dmp.ycd.base.Main $PROJECT_HOME/dmp_ycd.jar $store_id $time_cur &> $PROJECT_LOG_HOME/$date_cur/spark_ycd[$store_id].log

hdfs dfs -ls $HDFS_PATH/output/$year_month/$day/$store_id'_'$time_cur

echo "exec end"