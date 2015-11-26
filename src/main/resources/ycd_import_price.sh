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


if [ ! -d $PROJECT_LOG_HOME/offline/$date_cur/import_prices ];then
   mkdir $PROJECT_LOG_HOME/offline/$date_cur/import_prices
 fi

echo "start!!"${store_id}" time:"${time_cur} &>> $PROJECT_LOG_HOME/$date_cur/process.log

hdfs dfs -rm -r /ycd/input/tobacco_price

$SQOOP_HOME/bin/sqoop import --connect "jdbc:mysql://$mysql_jdbc?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true" --username $mysql_username --password $mysql_password --query 'select  name,brand,retail_price,producer_name,wholesale_price,area_code from datag_tobacco_price_list where $CONDITIONS' -m 1  --target-dir /ycd/input/tobacco_price --fields-terminated-by '\t' &>> $PROJECT_LOG_HOME/offline/$date_cur/import_prices/sqoop_ycd.log

hdfs dfs -ls  /ycd/input/tobacco_price

echo "exec end"