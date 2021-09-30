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

#--srcDb = 'ado1-eqrs-sbx2-1-data-migration-database']
#--srcTbl = 'sbx2_1_croldv85_remisprd_inpatient_stays']
#--srcTbl1 = 'ado1-eqrs-sbx2-1-patient-eqrs_ptnt_ptnt']
#--trgtDb = 'ado1-eqrs-sbx2-1-data-migration-database']
#--trgtTbl = 'eqrs_eqrs_cvrg_calc_ptnt_epo']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


ptnt_epo = glueContext.create_dynamic_frame.from_catalog(
    database = args['srcDb'],
    table_name = args['srcTbl'],  
    transformation_ctx = "ptnt_epo")

ptnt_epo_df = ptnt_epo.toDF()

ptnt = glueContext.create_dynamic_frame.from_catalog(
    database = args['srcDb'], 
    table_name = args['srcTbl1'],
    transformation_ctx = "ptnt")

ptnt_df = ptnt.toDF()

resultDf = ptnt_epo_df.join(ptnt_df, ptnt_epo_df.PAT_ID == ptnt_df.remis_ptnt_id, "inner")

resultDf = resultDf \
.select(col("EPO_ID").alias("PTNT_EPO_ID")
       ,col("PTNT_ID").alias("PTNT_ID")
       ,col("EPO_ADMIN").alias("EPO_ADMNG_NUM")
       ,col("EPO_CHARGES").alias("EPO_CHRGS_NUM")
       ,col("EPO_DOSAGE").alias("EPO_DSG_TXT")
       ,col("EPO_HEMAT").alias("EPO_HCT_TXT")
       ,col("EPO_SELF_IND").alias("EPO_SELF_ADMIN_IND")
       ,col("EPO_SELF_START_DATE").alias("EPO_SELF_STRT_ADMIN_DT")
       ,col("EPO_TYPE").alias("EPO_TYPE_IND")
       ,col("COMMENTS").alias("CMMTS_TXT")
       ,col("DATE_CREATED").alias("SYS_CREAT_TIME")
       ,col("CREATED_BY").alias("SYS_CREAT_NAME")
       ,col("DATE_MODIFIED").alias("SYS_UPDT_TIME")
       ,col("MODIFIED_BY").alias("SYS_UPDT_NAME")
         ) \

eqrs_trgt = DynamicFrame.fromDF(resultDf,glueContext,"eqrs_trgt")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = eqrs_trgt, database = args['trgtDb'], table_name = args['trgtTbl'], transformation_ctx = "datasink5")

job.commit()
