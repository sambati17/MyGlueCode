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


#--srcDb = 'ado1-eqrs-sbx2-1-data-migration-database'
#--srcTbl = 'sbx2_1_croldv85_remisprd_transplant_followups'
#--trgtDb = 'ado1-eqrs-sbx2-1-data-migration-database'
#--trgtTbl = 'eqrs_eqrs_cvrg_calc_ptnt_trnsplnt_flwp'

args = getResolvedOptions(sys.argv, ['JOB_NAME','srcDb','srcTbl','trgtDb','trgtTbl'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

transplant_followups = glueContext.create_dynamic_frame.from_catalog(
    database =  args['srcDb'], 
    table_name = args['srcTbl'], 
    transformation_ctx = "transplant_followups")

transplant_followups_df = transplant_followups.toDF() #.show(100)

transplant_followups_df  =  transplant_followups_df \
.select(col("TF_ID").alias("PTNT_TRNSPLNT_FLWP_ID")
        ,col("TX_ID").alias("PTNT_TRNSPLNT_ID")
        ,col("UNOS_PERIOD").alias("UNOS_FLWP_PRD_CD")
        ,col("ACUTE_REJ").alias("ACUTE_RJCTN_IND")
        ,col("UNOS_TRR_ID").alias("UNOS_TRNSPLNT_ID")
        ,col("DEATH_FUNCTION").alias("TRNSPLNT_FNCTN_AT_DEATH_IND")
        ,col("DIAL_DATE").alias("DLYS_DT")
        ,col("FU_DEATH_DATE").alias("FLWP_DEATH_DT")
        ,col("FU_DIALYSIS_PERFORMED").alias("FLWP_DLYS_PRFMD_IND")
        ,col("FU_FAIL_DIALYSIS").alias("FLWP_FAILD_DLYS_IND")
        ,col("FU_FAIL_OTHER").alias("FLWP_FAILD_OTHR_IND")
        ,col("FU_FAIL_TX").alias("FLWP_FAILD_TRNSPLNT_IND")
        ,col("FU_GRAFT_FAIL").alias("FLWP_GRFT_FAILD_IND")
        ,col("FU_GRAFT_FAIL_DATE").alias("FLWP_GRFT_FAILD_DT")
        ,col("FU_LOST_DATE").alias("FLWP_LOST_DT")
        ,col("FU_PERIOD").alias("FLWP_PRD_NUM")
        ,col("FU_PT_LIVING").alias("FLWP_PTNT_LVG_IND")
        ,col("FU_PT_LOST").alias("FLWP_PTNT_LOST_IND")
        ,col("FU_TX_DATE").alias("FLWP_TRNSPLNT_DT")
        ,col("FU_VA").alias("FLWP_VA_ORG_CD")
        ,col("FUNCTIONAL_STATUS").alias("TRNSPLNT_FNCTNL_STUS_CD")
        ,col("ICU").alias("ICU_STUS_IND")
        ,col("IM_OTHER").alias("IMNTY_SPRSN_OTHR_TXT")
        ,col("PAT_STATUS_DATE").alias("PTNT_STUS_DT")
        ,col("PATIENT_STATUS").alias("PTNT_STUS_CD")
        ,col("TF_PROVIDER_ID").alias("FLWP_FAC_CCN_NUM")
        ,col("UNOS_PRIMARY_FAIL").alias("UNOS_PRMRY_FAILR_CD")
        ,col("UNOS_PX_ID").alias("UNOS_PTNT_ID")
        ,col("UPIN").alias("UPIN_CD")
        ,col("DIAL_PROVIDER_ID").alias("DLYS_FAC_CCN_NUM")
        ,col("TRRFOLID").alias("UNOS_TRNSPLNT_FLWP_ID")
        ,col("CMS_DEATH_CAUSE_CD").alias("CMS_DEATH_CAUSE_CD")
        ,col("UNOS_DEATH_CAUSE_CD").alias("UNOS_DEATH_CAUSE_CD")
        ,col("COMMENTS").alias("CMMTS_TXT")
        ,current_timestamp().alias("SYS_CREAT_TIME")
        ,lit("ETL_DataMigration").alias("SYS_CREAT_NAME")
        ,col("DATE_MODIFIED").alias("SYS_UPDT_TIME")
        ,col("MODIFIED_BY").alias("SYS_UPDT_NAME")
        ) \

test = DynamicFrame.fromDF(transplant_followups_df,glueContext,"test")
#test.show()
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = test, 
            database = args['trgtDb'],
            table_name = args['trgtTbl'], 
            transformation_ctx = "datasink5")

job.commit()
