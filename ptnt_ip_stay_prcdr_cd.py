import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import col, when
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

args = getResolvedOptions(sys.argv, ['JOB_NAME','srcDb','srcTbl','trgtDb','trgtTbl'])

#--srcDb = 'ado1-eqrs-sbx2-1-data-migration-database'
#--srcTbl = 'sbx2_1_croldv85_remisprd_surgery_codes'
#--trgDb = 'ado1-eqrs-sbx2-1-data-migration-database'
#--trgtTbl = 'eqrs_eqrs_cvrg_calc_ptnt_ip_stay_prcdr_cd'

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

surgery_codes = glueContext.create_dynamic_frame.from_catalog(
    database = args['srcDb'], 
    table_name = args['srcTbl'], 
    transformation_ctx = "surgery_codes")

#surgery_codes.printSchema()
#print ("Count:", surgery_codes.count() )
surgery_codes_df = surgery_codes.toDF() #.printSchema()

surgery_codes_df  =  surgery_codes_df \
.select(col("SC4_ID").alias("PTNT_IP_STAY_PRCDR_CD_ID")
        ,col("IS1_ID").alias("PTNT_IP_STAY_ID")
        ,col("SURGERY_CODE").alias("PRCDR_CD")
        ,col("SURGERY_DATE").alias("PRCDR_DT")
        ,col("SURGERY_CODE_VERSION").alias("PRCDR_CD_VRSN_NUM")
        ,when(col("DATE_CREATED").isNull(),current_date()).otherwise(col("DATE_CREATED")).alias("SYS_CREAT_TIME")
        ,lit("ETL_DataMigration").alias("SYS_CREAT_NAME")
        ,col("DATE_MODIFIED").alias("SYS_UPDT_TIME")
        ,col("MODIFIED_BY").alias("SYS_UPDT_NAME")
        ) \
        
eqrs_trgt = DynamicFrame.fromDF(surgery_codes_df,glueContext,"eqrs_trgt")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = eqrs_trgt, database = args['trgtDb'], table_name = args['trgtTbl'], transformation_ctx = "datasink5")

job.commit()
