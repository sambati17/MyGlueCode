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
#--srcTbl = 'sbx2_1_croldv85_remisprd_entitlements'
#--srcTbl1 = 'ado1-eqrs-sbx2-1-patient-eqrs_ptnt_ptnt'
#--trgtDb = 'ado1-eqrs-sbx2-1-data-migration-database'
#--trgtTbl = 'eqrs_eqrs_cvrg_calc_ptnt_entlmt'

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


ptnt_entitlements = glueContext.create_dynamic_frame.from_catalog(
    database = args['srcDb'],
    table_name = args['srcTbl'],  
    transformation_ctx = "ptnt_entitlements")

ptnt_entitlements_df = ptnt_entitlements.toDF()

ptnt = glueContext.create_dynamic_frame.from_catalog(
    database = args['srcDb'], 
    table_name = args['srcTbl1'],
    transformation_ctx = "ptnt")

ptnt_df = ptnt.toDF()

resultDf = ptnt_entitlements_df.join(ptnt_df, ptnt_entitlements_df.PAT_ID == ptnt_df.remis_ptnt_id, "inner")

resultDf = resultDf \
.select(col("ENT_ID").alias("PTNT_ENTLMT_ID")
       ,col("PTNT_ID").alias("PTNT_ID")
       ,col("ENT_TYPE").alias("PTNT_ENTLMT_TYPE")
       ,col("ENTITLEMENT_START_DATE").alias("PTNT_ENTLMT_STRT_DT")
       ,col("ENTITLEMENT_TERM_DATE").alias("PTNT_ENTLMT_TRMNTN_DT")
       ,col("COMMENTS").alias("CMMTS_TXT")
       ,col("DATE_CREATED").alias("SYS_CREAT_TIME")
       ,col("CREATED_BY").alias("SYS_CREAT_NAME")
       ,col("DATE_MODIFIED").alias("SYS_UPDT_TIME")
       ,col("MODIFIED_BY").alias("SYS_UPDT_NAME")
         ) \

eqrs_trgt = DynamicFrame.fromDF(resultDf,glueContext,"eqrs_trgt")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = eqrs_trgt, database = args['trgtDb'], table_name = args['trgtTbl'], transformation_ctx = "datasink5")

job.commit()
