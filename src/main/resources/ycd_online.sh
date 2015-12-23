source /etc/profile

mysql_jdbc="192.168.7.7:3306/asto_ec_origin"
mysql_username="root"
mysql_password="password"

PROJECT_HOME=/data/work/ycd
PROJECT_LOG_HOME=$PROJECT_HOME/logs

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

echo "start!!"${store_id}" time:"${time_cur} &>> $PROJECT_LOG_HOME/$date_cur/process.log

# 订单详情
$SQOOP_HOME/bin/sqoop import --connect "jdbc:mysql://$mysql_jdbc?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true" --username $mysql_username --password $mysql_password --query 'SELECT "'$store_id'",y.order_id,y.order_date,y.cigar_name,y.wholesale_price,y.purchase_amount,y.order_amount,y.money_amount,y.producer_name,t.area_code FROM datag_yancao_historyOrderDetailData y inner join (select ss.shop_src_id as shop_src_id ,ss.shop_id as shop_id from  datag_shop ss where ss.shop_src_id = "'$store_id'" ) s on s.shop_id = y.shop_id inner join datag_shop_tobacco t on s.shop_id = t.shop_id where $CONDITIONS ' -m 1  --target-dir /ycd/input/online/$year_month/$day/$store_id'_'$time_cur/tobacco_order_details --fields-terminated-by '\t' >> $PROJECT_LOG_HOME/$date_cur/sqoop_ycd[$store_id].log

# 每个店铺需要计算几个月份的数据。
$SQOOP_HOME/bin/sqoop import --connect "jdbc:mysql://$mysql_jdbc?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true" --username $mysql_username --password $mysql_password --query 'SELECT distinct "'$store_id'",ac.count_month FROM  (select ss.shop_src_id as shop_src_id ,ss.shop_id as shop_id from  datag_shop ss where ss.shop_src_id = "'$store_id'" ) s inner join  datag_shop_tobacco y  on s.shop_id = y.shop_id inner join datag_tobacco_area_code ac on ac.area_code = y.area_code where $CONDITIONS'   -m 1  --target-dir /ycd/input/online/$year_month/$day/$store_id'_'$time_cur/store_id_calc_months  --fields-terminated-by '\t' >> $PROJECT_LOG_HOME/$date_cur/sqoop_ycd[$store_id].log

# 无效订单id
$SQOOP_HOME/bin/sqoop import --connect "jdbc:mysql://$mysql_jdbc?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true" --username $mysql_username --password $mysql_password --query 'select distinct a.order_id as order_id from datag_yancao_historyOrderData a where a.status = "作废" and $CONDITIONS'   -m 1  --target-dir /ycd/input/online/$year_month/$day/$store_id'_'$time_cur/invalid_order_id --fields-terminated-by '\t' >> $PROJECT_LOG_HOME/$date_cur/sqoop_ycd[$store_id].log

hdfs dfs -ls /ycd/input/online/$year_month/$day/$store_id'_'$time_cur/tobacco_order_details >> $PROJECT_LOG_HOME/$date_cur/sqoop_ycd[$store_id].log

#spark-submit  --master=local[4] --driver-memory 1G --executor-memory 2G  --executor-cores 4 --driver-java-options -DPropPath=$PROJECT_HOME/prop.properties  --jars /data/spark/lib/mysql-connector-java-5.1.35.jar  --class  com.asto.dmp.ycd.base.Main $PROJECT_HOME/dmp_ycd.jar 100 $time_cur $store_id >> $PROJECT_LOG_HOME/$date_cur/spark_ycd[$store_id].log

hdfs dfs -ls /ycd/output/online/$year_month/$day/$store_id'_'$time_cur/ >> $PROJECT_LOG_HOME/$date_cur/spark_ycd[$store_id].log

echo "exec end"