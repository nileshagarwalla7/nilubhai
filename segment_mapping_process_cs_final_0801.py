from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.functions import lit,udf,concat_ws
from pyspark.sql.types import *
import pandas as pd
import time
import argparse
import sys
from time import gmtime, strftime
from pytz import timezone
import pytz
from datetime import datetime
from pyspark import SparkConf,SparkContext
from pyspark.sql.functions import *
from pyspark.sql import *
import datetime
from pyspark.sql import functions as F
import logging

#I love pies


def get_pst_date():
	from datetime import datetime
	dt = datetime.utcnow()
	#dt=datetime.now(tz=pytz.utc)
	###(dt, micro) =  datetime.now(tz=pytz.utc).astimezone(timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S.%f').split('.')
	###dt = "%s.%03d" % (dt, int(micro) / 1000)
	return dt


segmentMappingStartDate = get_pst_date()
LogID = 0
#log_df = pd.DataFrame()
properties = {"user" : "occuser" , "password":"Exp3dia22" , "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"}
url = "jdbc:sqlserver://10.23.18.135"

#(Remove this )


def log_df_update(spark,IsComplete,Status,EndDate,ErrorMessage,RowCounts,StartDate,FilePath,tablename):
	import pandas as pd
	l = [(process_name_log.value,process_id_log.value,IsComplete,Status,StartDate,EndDate,ErrorMessage,int(RowCounts),FilePath)]
	schema = (StructType([StructField("SourceName", StringType(), True),StructField("SourceID", IntegerType(), True),StructField("IsComplete", IntegerType(), True),StructField("Status", StringType(), True),StructField("StartDate", TimestampType(), True),StructField("EndDate", TimestampType(), True),StructField("ErrorMessage", StringType(), True),StructField("RowCounts", IntegerType(), True),StructField("FilePath", StringType(), True)]))
	rdd_l = sc.parallelize(l)
	log_df = spark.createDataFrame(rdd_l,schema)
	#  "Orchestration.dbo.AlphaProcessDetailsLog"
	log_df.withColumn("StartDate",from_utc_timestamp(log_df.StartDate,"PST")).withColumn("EndDate",from_utc_timestamp(log_df.EndDate,"PST")).write.jdbc(url=url, table=tablename,mode="append", properties=properties)

conf = SparkConf()
conf.setAppName('segment-mapping')
sc = SparkContext(conf=conf)

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


print(".....Started segment mapping process....")


parser = argparse.ArgumentParser()
parser.add_argument("--locale_name", help="Write locale_name like en_nz")
parser.add_argument("--data_environ", help="Write dev or prod or test")

StartDate = get_pst_date()
LogID+=1
try:
	args = parser.parse_args()
	locale_name = args.locale_name
	data_environ = args.data_environ
	pos = locale_name.split('_')[1].upper()
	current_date =  time.strftime("%Y/%m/%d")
		
	#locale_name = 'en_nz'
	#data_environ = 'prod'
	#pos = locale_name.split('_')[1].upper()
	#current_date =  '2017/07/31'
	
	status_table = (sqlContext.read.format("jdbc").option("url", "jdbc:sqlserver://10.23.18.135").option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").option("dbtable", "Orchestration.dbo.AlphaConfig").option("user", "occuser").option("password", "Exp3dia22").load())
	pos1 = locale_name.split('_')[1].upper()
	pos0 = locale_name.split('_')[0]
	search_string = pos0+"_"+pos1
	required_row = status_table.filter(status_table.Locale == search_string).filter("brand like 'Brand Expedia'").collect()
	global process_id
	global process_name 
	process_id = required_row[0]['id']
	process_name = 'Segment_mapping'

	process_id_log = sc.broadcast(process_id)
	process_name_log = sc.broadcast(process_name)
	
	log_df = log_df_update(sqlContext,1,'parameters are correct',get_pst_date(),' ',0,StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
except:
	log_df = log_df_update(sqlContext,0,'failed',get_pst_date(),'parameters are improper',0,StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	raise Exception("Parameters not imported properly. Please check the parameters")


print("locale_name",locale_name)
print("data_environ",data_environ)


StartDate = get_pst_date()
LogID+=1

try:
	TPG_data = ("s3n://big-data-analytics-scratch-prod/project_traveler_profile/affine/email_campaign/merged_shop_phase_date_format/{}/{}/{}".format(pos,locale_name,current_date))
	df = (sqlContext.read.parquet(TPG_data).filter("mer_status = 1"))
	log_df = log_df_update(sqlContext, 1,'traveler data imported',get_pst_date(),' ',str(df.count()),StartDate,TPG_data,'Orchestration.dbo.AlphaProcessDetailsLog')
except:
	log_df = log_df_update(sqlContext, 0,'failed',get_pst_date(),'current traveler data was not present',0,StartDate,TPG_data,'Orchestration.dbo.AlphaProcessDetailsLog')
	raise Exception("----Traveler data not imported properly.----")


first_row_df = df.first()
tpid = first_row_df["tpid"]
eapid = first_row_df["eapid"]

StartDate = get_pst_date()
LogID+=1
def importFiles(x):
	temp = (sqlContext.read.format("jdbc")
					 .option("url", "jdbc:sqlserver://10.23.18.135")
					 .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
					 .option("dbtable", "AlphaMVP.dbo."+x).option("user", "occuser")
					.option("password", "Exp3dia22").load()
			 .filter("tpid = " + str(tpid) + " and eapid = " + str(eapid))
			 )
	return temp
try:
	seg_info_ls = (importFiles("segment_definition"+"_"+ data_environ) .drop("id").filter("segment_deleted_flag = 0").select("segment_type_id","segment_criteria").collect())
	log_df = log_df_update(sqlContext, 1,'segment defintition imported',get_pst_date(),' ',0,StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
except:
	log_df = log_df_update(sqlContext, 0,'failed',get_pst_date(),'Problem in importing segment_definition_prod file.',0,StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	raise Exception("----segment_definition data not imported properly.----")


def chunks(l, n):
		for i in range(0, len(l), n):
				yield l[i:i + n]
		
def rm_duplicates(segment_type_id):
		ls = list(set(segment_type_id.split("#")))
		if '0' in ls :
				ls.remove('0')
		return '#'.join(ls)

rm_duplicates_udf = udf(rm_duplicates,StringType())

def make_ls (row):
		email_id = row["email_address"]
		test_key = row["test_keys"]
		
		seg_ls = list(set(row["segment_type_id"].split("#")))
		
		final_ls = [[tpid,eapid,locale_name,email_id,test_key,i,row["segment_type_id"]] for i in seg_ls]
		return final_ls
				

seg_info_ls_batch = (list(chunks(seg_info_ls,10)))

batchCount = 1


for i in range(0,len(seg_info_ls_batch)):
	cnt = 1
	col_list = []
	case_when_query = ""
	
	for seg_info in seg_info_ls_batch[i]:
	
		if seg_info["segment_criteria"].find('point_balance_available') <0 :
			if cnt < len(seg_info_ls_batch[i]):
				case_when_query += (" CASE WHEN "+ seg_info["segment_criteria"].replace('[','').replace(']','')
									.replace('member_tier_id','e_member_tier_id')
									+" THEN " + str(seg_info["segment_type_id"]) 
									+" ELSE 0 END as " + "seg_id_"+str(cnt)+",")
				col_list += ["seg_id_"+str(cnt)]

			else :
				case_when_query += (" CASE WHEN "+ seg_info["segment_criteria"].replace('[','').replace(']','')
									.replace('member_tier_id','e_member_tier_id')
									+" THEN " + str(seg_info["segment_type_id"]) 
									+" ELSE 0 END as " + "seg_id_"+str(cnt))
				col_list += ["seg_id_"+str(cnt)]
		
		cnt+=1
		
	
	df.createOrReplaceTempView("df")
	df_transformed= (sqlContext.sql("select *, "+ case_when_query +" from df")
					.withColumn("segment_type_id",concat_ws("#",*col_list))
					.withColumn("segment_type_id_new",rm_duplicates_udf("segment_type_id"))
					.drop("segment_type_id")
					.withColumnRenamed("segment_type_id_new","segment_type_id")
					)
					
	#df_transformed_rdd = df_transformed.rdd.flatMap(lambda x: make_ls(x))

	schema = StructType([StructField('tpid', StringType(), True),
						StructField('eapid', StringType(), True),
						StructField('locale', StringType(), True),
						StructField('email_address', StringType(), True),
						StructField('test_keys', StringType(), True),
						StructField('segment_type_id', StringType(), True)])
						#StructField('segment_type_id_concat', StringType(), True)])
							 
	df_final = df_transformed 
	#sqlContext.createDataFrame(df_transformed,schema).cache()
	
	
	if batchCount == 1:
		df_join = df_final.select('tpid','eapid','locale','email_address','test_keys','segment_type_id')
		batchCount+=1
	else:
		df_join = df_join.join(df_final,["email_address"],"left").select(df_join['*'],df_final.segment_type_id.alias("segment_type_id_nxt"))
		df_join = df_join\
		.withColumn("segment_type_id_concat",concat_ws("#",df_join.segment_type_id_nxt,df_join.segment_type_id)).drop("segment_type_id").drop("segment_type_id_nxt")\
		.withColumn("segment_type_id_new",rm_duplicates_udf("segment_type_id_concat"))\
				.drop("segment_type_id_concat")\
				.withColumnRenamed("segment_type_id_new","segment_type_id")
		
		batchCount+=1
		
df_transformed_rdd = df_join.rdd.flatMap(lambda x: make_ls(x))

schema = StructType([StructField('tpid', StringType(), True),StructField('eapid', StringType(), True),StructField('locale', StringType(), True),StructField('email_address', StringType(), True),StructField('test_keys', StringType(), True),StructField('segment_type_id', StringType(), True),StructField('segment_type_id_concat', StringType(), True)])
							 
df_final = sqlContext.createDataFrame(df_transformed_rdd,schema).filter("segment_type_id != ''").cache()

def rm_extra_hash(segment_type_id_concat):
		ls = segment_type_id_concat.replace("##","#").split("#")
		ls_final = [i for i in ls if i!='']
		return "#".join(ls_final)
		
rm_extra_hash_udf = udf(rm_extra_hash,StringType())

df_final1 = df_final.withColumn("segment_type_id_concat",rm_extra_hash_udf("segment_type_id_concat"))

segment_path = ("s3n://big-data-analytics-scratch-prod/project_traveler_profile/affine/email_campaign/segmentLookUp/{}/{}/{}/{}".format(data_environ,pos,locale_name,current_date))

df_final1.write.mode("overwrite").parquet(segment_path)
print("-----Completed Segment Mapping process------")

LogID+=1
log_df = log_df_update(sqlContext,1,'Segment Mapping process completed.',get_pst_date(),' ',str(df_final.count()),segmentMappingStartDate,segment_path,'Orchestration.dbo.AlphaProcessDetailsLog')
log_df = log_df_update(sqlContext,1,'Segment Mapping process completed.',get_pst_date(),' ',str(df_final.count()),segmentMappingStartDate,segment_path,'Orchestration.dbo.CentralLog')






