source /etc/profile

mysql_jdbc="192.168.7.7:3306/asto_ec_origin"
mysql_username="root"
mysql_password="password"

PROJECT_HOME=/data/work/ycd
PROJECT_LOG_HOME=$PROJECT_HOME/logs

date_cur=`date +%Y%m%d`
time_cur=`date +%s`
year_month=`date +%Y%m`
day=`date +%d`


if [ ! -d $PROJECT_LOG_HOME/offline/$date_cur ];then
   mkdir -p $PROJECT_LOG_HOME/offline/$date_cur
 fi

echo "start!!"${store_id}" time:"${time_cur} &>> $PROJECT_LOG_HOME/$date_cur/process.log

$SQOOP_HOME/bin/sqoop import --connect "jdbc:mysql://$mysql_jdbc?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true" --username $mysql_username --password $mysql_password --query 'SELECT  s.shop_src_id,y.order_id,y.order_date,y.cigar_name,y.wholesale_price,y.purchase_amount,y.order_amount,y.money_amount,y.producer_name,t.area_code FROM datag_yancao_historyOrderDetailData y inner join (select ss.shop_src_id as shop_src_id ,ss.shop_id as shop_id from  datag_shop ss) s on s.shop_id = y.shop_id inner join datag_shop_tobacco t on s.shop_id = t.shop_id  where $CONDITIONS'   -m 1  --target-dir /ycd/input/offline/$year_month/$day/tobacco_order_details_$time_cur --fields-terminated-by '\t' >> $PROJECT_LOG_HOME/offline/$date_cur/sqoop_ycd.log

$SQOOP_HOME/bin/sqoop import --connect "jdbc:mysql://$mysql_jdbc?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true" --username $mysql_username --password $mysql_password --query 'select distinct a.order_id as order_id from datag_yancao_historyOrderData a where a.status = "作废" and $CONDITIONS'   -m 1  --target-dir /ycd/input/offline/$year_month/$day/invalid_order_id_$time_cur --fields-terminated-by '\t' >> $PROJECT_LOG_HOME/offline/$date_cur/sqoop_ycd.log


hdfs dfs -ls /ycd/input/offline/$year_month/$day/tobacco_order_details_$time_cur  >> $PROJECT_LOG_HOME/offline/$date_cur/sqoop_ycd.log

spark-submit  --master=local[4] --driver-memory 1G --executor-memory 2G  --executor-cores 4 --driver-java-options -DPropPath=$PROJECT_HOME/prop.properties  --jars /data/spark/lib/mysql-connector-java-5.1.35.jar  --class  com.asto.dmp.ycd.base.Main $PROJECT_HOME/dmp_ycd.jar 200 $time_cur >> $PROJECT_LOG_HOME/offline/$date_cur/spark_ycd.log

hdfs dfs -ls /ycd/output/offline/$year_month/$day/$time_cur >> $PROJECT_LOG_HOME/offline/$date_cur/spark_ycd.log

echo "exec end"