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


$SQOOP_HOME/bin/sqoop import --connect "jdbc:mysql://192.168.4.108:3306/xdgc?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true" --username xdgc --password xdgc0708 --query 'SELECT d.property_uuid FROM loan_app AS la JOIN loan_approved a JOIN loan_app_party_property_pct b JOIN party_property_pct c JOIN property d WHERE  a.loan_app_uuid = b.loan_apply_uuid AND a.loan_app_uuid = la.loan_app_uuid AND b.party_property_pct_uuid = c.party_property_pct_uuid AND c.property_uuid = d.property_uuid AND a.current_flag = 1 AND a.del_flag = 0 AND b.del_flag = 0 AND c.del_flag = 0 AND d.del_flag = 0 AND la.apply_status IN ("AWAIT_SIGN_FINISH","REPAYMENT","PAY_OFF","CASH_APPLY","CASH_FAILURE") AND la.loan_type = "LOAN_TYPE_TOBACCO"  AND $CONDITIONS  GROUP BY d.property_uuid '   -m 1  --target-dir /ycd/input/offline/$year_month/$day/loan_store_$time_cur --fields-terminated-by '\t'

#spark-submit  --master=local[4] --driver-memory 1G --executor-memory 2G  --executor-cores 4 --driver-java-options -DPropPath=$PROJECT_HOME/prop.properties  --jars /data/spark/lib/mysql-connector-java-5.1.35.jar  --class  com.asto.dmp.ycd.base.Main $PROJECT_HOME/dmp_ycd.jar $time_cur >> $PROJECT_LOG_HOME/offline/$date_cur/spark_ycd.log

#hdfs dfs -ls /ycd/offline/output/$year_month/$day/

echo "exec end"