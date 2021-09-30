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
#--srcTbl = 'sbx2_1_croldv85_remisprd_diagnostic_type_codes'
#--trgtDb = 'ado1-eqrs-sbx2-1-data-migration-database'
#--trgtTbl = 'eqrs_eqrs_cvrg_calc_ptnt_ip_stay_dgns_cd'

args = getResolvedOptions(sys.argv, ['JOB_NAME','srcDb','srcTbl','trgtDb','trgtTbl'])


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "ado1-eqrs-sbx2-1-data-migration-database", table_name = "sbx2_1_croldv85_remisprd_diagnostic_type_codes", transformation_ctx = "datasource0")

diagnostic_type_codes = glueContext.create_dynamic_frame.from_catalog(
    database = args['srcDb'], 
    table_name =  args['srcTbl'], 
    transformation_ctx = "diagnostic_type_codes")

diagnostic_type_codes_df = diagnostic_type_codes.toDF()

#diagnostic_type_codes.printSchema()
#print ("Count:", diagnostic_type_codes.count() )
diagnostic_type_codes_df = diagnostic_type_codes.toDF() #.printSchema()

diagnostic_type_codes_df  =  diagnostic_type_codes_df \
.select(col("DTC_ID").alias("PTNT_IP_STAY_DGNS_CD_ID")
        ,col("IS1_ID").alias("PTNT_IP_STAY_ID")
        ,col("DIAG_CODE").alias("DGNS_CD")
        ,col("DIAG_CODE_VERSION").alias("DGNS_CD_VRSN_NUM")
        ,current_date().alias("SYS_CREAT_TIME")
        ,lit("ETL_DataMigration").alias("SYS_CREAT_NAME")
        ,col("DATE_MODIFIED").alias("SYS_UPDT_TIME")
        ,col("MODIFIED_BY").alias("SYS_UPDT_NAME")
        ) \

eqrs_trgt = DynamicFrame.fromDF(diagnostic_type_codes_df,glueContext,"eqrs_trgt")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = eqrs_trgt, 
            database = args['trgtDb'], table_name = args['trgtTbl'], transformation_ctx = "datasink5")

job.commit()
