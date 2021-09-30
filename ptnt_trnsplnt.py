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
#--srcTbl = 'sbx2_1_croldv85_remisprd_transplants'
#--srcTbl1 = 'ado1-eqrs-sbx2-1-patient-eqrs_ptnt_ptnt'
#--trgtDb = 'ado1-eqrs-sbx2-1-data-migration-database'
#--trgtTbl = 'eqrs_eqrs_cvrg_calc_ptnt_trnsplnt'

args = getResolvedOptions(sys.argv, ['JOB_NAME','srcDb','srcTbl','srcTbl1','trgtDb','trgtTbl'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

transplants = glueContext.create_dynamic_frame.from_catalog(
    database = args['srcDb'], 
    table_name = args['srcTbl'],
    transformation_ctx = "transplants")

transplants_df = transplants.toDF() #.printSchema()

ptnt = glueContext.create_dynamic_frame.from_catalog(
    database = args['srcDb'], 
    table_name = args['srcTbl1'],
    transformation_ctx = "ptnt")

ptnt_df = ptnt.toDF() #.printSchema()

#INNER join#
resultDf = transplants_df.join(ptnt_df, transplants_df.PAT_ID == ptnt_df.remis_ptnt_id, "inner")
#resultDf  =  resultDf.show(10)

resultDf = resultDf \
.select(col("TX_ID").alias("PTNT_TRNSPLNT_ID")
            ,col("UNOS_TRR_ID").alias("UNOS_TRNSPLNT_ID")
            ,col("ptnt_id").alias("PTNT_ID")
            ,col("CADAVERTX_TYPE").alias("CDVRC_TRNSPLNT_IND")
            ,col("DONOR_AGE").alias("DONOR_AGE_NUM")
            ,col("UNOS_DONOR_ID").alias("UNOS_DONOR_ID")
            ,col("LIVING_DONOR_RELATED").alias("LVG_DONOR_RLTD_CD")
            ,col("LIVING_RELATED").alias("LVG_RLTD_CD")
            ,col("TX_IP_IND").alias("TRNSPLNT_IP_STAY_IND")
            ,col("TX_STAY_DAYS").alias("TRNSPLNT_STAY_DAYS_NUM")
            ,col("UNOS_PX_ID").alias("UNOS_PTNT_ID")
            ,col("UPIN").alias("UPIN_CD")
            ,col("DONOR_TYPE").alias("DONOR_TYPE_CD")
            ,col("TX_DATE").alias("TRNSPLNT_DT")
            ,when(col("DONOR_GENDER") == "1" , "M") \
                .when(col("DONOR_GENDER") == "2" , "F") \
                .when(col("DONOR_GENDER") == "3" , "U").alias("DONOR_GNDR_TXT")
            ,col("TX_FAIL_DATE").alias("TRNSPLNT_FAILD_DT")
            ,col("HCFA_TEMP_ID").alias("HCFA_TEMP_ID")
            ,col("TX_PROVIDER_ID").alias("TRNSPLNT_FAC_CCN_NUM")
            ,col("RACE_CODE").alias("RACE_TYPE_CD")
            ,col("DONOR_RACE_CODE").alias("DONOR_RACE_CD")
            ,col("NPI").alias("NPI_ID")
            ,col("TX_KP_IND").alias("TRNSPLNT_KDNY_PNCRT_IND")
            ,col("COMMENTS").alias("CMMTS_TXT")
            ,col("DATE_CREATED").alias("SYS_CREAT_TIME")
            ,col("CREATED_BY").alias("SYS_CREAT_NAME")
            ,col("DATE_MODIFIED").alias("SYS_UPDT_TIME")
            ,col("MODIFIED_BY").alias("SYS_UPDT_NAME")
         ) \

eqrs_trgt = DynamicFrame.fromDF(resultDf,glueContext,"eqrs_trgt")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = eqrs_trgt, 
                database = args['trgtDb'], 
                table_name = args['trgtTbl'], transformation_ctx = "datasink5")

job.commit()
