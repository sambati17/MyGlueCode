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

args = getResolvedOptions(sys.argv, ['JOB_NAME','srcDb', 'srcTbl', 'srcTbl1', 'trgtDb', 'trgtTbl'])

#--srcDb = 'ado1-eqrs-sbx2-1-data-migration-database'
#--srcTbl = 'sbx2_1_croldv85_remisprd_inpatient_stays'
#--srcTbl1 = 'ado1-eqrs-sbx2-1-patient-eqrs_ptnt_ptnt'
#--trgtDb = 'ado1-eqrs-sbx2-1-data-migration-database'
#--trgtTbl = 'eqrs_eqrs_cvrg_calc_ptnt_ptnt_ip_stay'

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


ptnt_inpatient_stays = glueContext.create_dynamic_frame.from_catalog(
    database = args['srcDb'],
    table_name = args['srcTbl'],  
    transformation_ctx = "ptnt_inpatient_stays")

ptnt_inpatient_stays_df = ptnt_inpatient_stays.toDF()

ptnt = glueContext.create_dynamic_frame.from_catalog(
    database = args['srcDb'], 
    table_name = args['srcTbl1'],
    transformation_ctx = "ptnt")

ptnt_df = ptnt.toDF()

resultDf = ptnt_inpatient_stays_df.join(ptnt_df, ptnt_inpatient_stays_df.PAT_ID == ptnt_df.remis_ptnt_id, "inner")

resultDf = resultDf \
.select(col("IS1_ID").alias("PTNT_IP_STAY_ID")
       ,col("PTNT_ID").alias("PTNT_ID")
       ,col("DIAG_RELATED_GROUP_DEST").alias("DRG_DSTNTN_CD")
       ,col("DIAG_RELATED_GROUP_OUT").alias("DRG_OUT_CD")
       ,col("IP_DX_IND").alias("IP_STAY_DLYS_IND")
       ,col("IP_INTR").alias("IP_STAY_INTRMDRY_CD")
       ,col("IP_KAC").alias("KDNY_ACQSTN_CHRG_NUM")
       ,col("IP_SESSIONS").alias("IP_STAY_DLYS_SESN_NUM")
       ,col("IP_STATUS").alias("IP_STAY_STUS_CD")
       ,col("IP_TYPE_DX").alias("IP_STAY_DLYS_TYPE_CD")
       ,col("IP_TX").alias("IP_STAY_TRNSPLNT_IND")
       ,col("IP_START_DATE").alias("IP_STAY_STRT_DT")
       ,col("IP_END_DATE").alias("IP_STAY_END_DT")
       ,col("IP_PROVIDER").alias("IP_STAY_FAC_CCN_NUM")
       ,col("EPO_ID").alias("PTNT_EPO_ID")
       ,col("IP_DRG_CODE").alias("IP_STAY_DRG_CD")
       ,col("NPI").alias("NPI_ID")
       ,col("COMMENTS").alias("CMMTS_TXT")
       ,col("DATE_CREATED").alias("SYS_CREAT_TIME")
       ,col("CREATED_BY").alias("SYS_CREAT_NAME")
       ,col("DATE_MODIFIED").alias("SYS_UPDT_TIME")
       ,col("MODIFIED_BY").alias("SYS_UPDT_NAME")
         ) \

eqrs_trgt = DynamicFrame.fromDF(resultDf,glueContext,"eqrs_trgt")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = eqrs_trgt, database = args['trgtDb'], table_name = args['trgtTbl'], transformation_ctx = "datasink5")

job.commit()
