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
#--srcTbl = 'sbx2_1_croldv85_remisprd_esrd_coverages'
#--srcTbl1 = 'ado1-eqrs-sbx2-1-patient-eqrs_ptnt_ptnt'
#--trgtDb = 'ado1-eqrs-sbx2-1-data-migration-database'
#--trgtTbl = 'eqrs_eqrs_cvrg_calc_ptnt_esrd_cvrg_prd'

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

ptnt_esrd_cvrg_prd = glueContext.create_dynamic_frame.from_catalog(
    database = args['srcDb'], 
    table_name = args['srcTbl'], 
    transformation_ctx = "ptnt_esrd_cvrg_prd")

#ptnt_esrd_cvrg_prd.printSchema()
#print ("Count:", ptnt_esrd_cvrg_prd.count() )
#ptnt_esrd_cvrg_prd_df = ptnt_esrd_cvrg_prd.toDF().show(100)
ptnt_esrd_cvrg_prd_df = ptnt_esrd_cvrg_prd.toDF() #.printSchema()

ptnt = glueContext.create_dynamic_frame.from_catalog(
    database = args['srcDb'], 
    table_name = args['srcTbl1'], 
    transformation_ctx = "ptnt")

#ptnt.printSchema()
#print ("Count:", ptnt.count() )
ptnt_df = ptnt.toDF() #.printSchema()

#inner join#
resultDf = ptnt_esrd_cvrg_prd_df.join(ptnt_df, ptnt_esrd_cvrg_prd_df.PAT_ID == ptnt_df.remis_ptnt_id, "inner")


resultDf = resultDf \
.select(col("EC_ID").alias("PTNT_ESRD_CVRG_PRD_ID")
       ,col("PTNT_ID").alias("PTNT_ID")
       ,col("CVRG_START_DATE").alias("CVRG_STRT_DT")
       ,col("COVERAGE_START_DATE_SRC").alias("CVRG_STRT_RSN_CD")
       ,col("TERMINATION_DATE").alias("CVRG_TRMNTN_DT")
       ,col("COVERAGE_TERM_DATE_REASON").alias("CVRG_TRMNTN_RSN_CD")
       ,col("CVRG_EXPECTED_TRMNT_DATE").alias("CVRG_EXPTD_TRMNTN_DT")
       ,col("COMMENTS").alias("CMMTS_TXT")
       ,col("DATE_CREATED").alias("SYS_CREAT_TIME")
       ,col("CREATED_BY").alias("SYS_CREAT_NAME")
       ,col("DATE_MODIFIED").alias("SYS_UPDT_TIME")
       ,col("MODIFIED_BY").alias("SYS_UPDT_NAME")
         ) \

eqrs_trgt = DynamicFrame.fromDF(resultDf,glueContext,"eqrs_trgt")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = eqrs_trgt, database = args['trgtDb'], table_name = args['trgtTbl'], transformation_ctx = "datasink5")

job.commit()
