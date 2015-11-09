source /etc/profile
store_id=$1
date_cur=`date +%Y%m%d`
time_cur=`date +%s`
year_month=`date +%Y%m`
day=`date +%d`
HOME=/data/work/ycd/logs
online_hdfs_path=/ycd/online
if [ -z "$1" ]; then
    echo "1st argument is empty!"
    exit
fi
if [ ! -d $HOME/$date_cur ];then
   mkdir $HOME/$date_cur
   echo "Created dir for ycd store Id ["$store_id"]!"
 fi
#transfer table alipay_account_trade
echo "start!!"${store_id}" time:"${time_cur} >> $HOME/$date_cur/process.log
hdfs dfs -rm -r /ycd/online/input/tobacco_order_details
/data/sqoop-1.4.4/bin/sqoop import --connect jdbc:mysql://192.168.7.7:3306/retail_loan --username root --password password --query 'SELECT store_id,orderId,orderDate,cigarName,wholesalePrice,purchaseAmount,orderAmount,moneyAmount,producerName FROM  datag_yancao_historyOrderDetailData WHERE store_id = "'$store_id'" and $CONDITIONS '   -m 1  --target-dir /ycd/online/input/tobacco_order_details --fields-terminated-by '\t'

spark-submit  --master=local[4] --driver-memory 1G --executor-memory 2G  --executor-cores 4 --driver-java-options  --jars  /home/hadoop/project/amqp-client-3.5.4.jar,/data/spark/lib/mysql-connector-java-5.1.35.jar  --class  com.asto.dmp.ycd.base.Main /home/hadoop/project/main.jar $store_id $time_cur &>> $HOME/$date_cur/spark_ycd[$store_id].log hadoop dfs -ls /ycd/online/output/$year_month/$day/$store_id