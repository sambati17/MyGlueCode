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
#--srcTbl = 'sbx2_1_croldv85_remisprd_dialyses'
#--srcTbl1 = 'ado1-eqrs-sbx2-1-patient-eqrs_ptnt_ptnt'
#--trgtDb = 'ado1-eqrs-sbx2-1-data-migration-database'
#--trgtTbl = 'eqrs_eqrs_cvrg_calc_ptnt_dlys'

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#glueContext = GlueContext(SparkContext.getOrCreate())
print("Hello!!!")


ptnt_dlys = glueContext.create_dynamic_frame.from_catalog(
    database = args['srcDb'], 
    table_name = args['srcTbl'], 
    transformation_ctx = "ptnt_dlys")

#ptnt_dlys.printSchema()
#print ("Count:", ptnt_dlys.count() )
ptnt_dlys_df = ptnt_dlys.toDF() #.printSchema()


ptnt = glueContext.create_dynamic_frame.from_catalog(
    database = args['srcDb'], 
    table_name = args['srcTbl1'], 
    transformation_ctx = "ptnt")

#ptnt.printSchema()
#print ("Count:", ptnt.count() )
ptnt_df = ptnt.toDF() #.printSchema()

#inner join#
resultDf = ptnt_dlys_df.join(ptnt_df, ptnt_dlys_df.PAT_ID == ptnt_df.remis_ptnt_id, "inner")

resultDf = resultDf \
.select(col("DIAL_ID").alias("PTNT_DLYS_ID")
            ,col("ptnt_id").alias("PTNT_ID")
            ,col("CUR_DIALTYPE").cast(IntegerType()).alias("CRNT_DLYS_TYPE_ID")
            ,col("DX_BILL").alias("DLYS_BILL_IND")
            ,col("DX_CHARGE").alias("DLYS_CHRG_TXT")
            ,col("DX_FIRST_DATE").alias("DLYS_1ST_DT")
            ,col("DX_SESSIONS").alias("DLYS_SESN_NUM")
            ,col("DX_YEAR_YYYY").alias("DLYS_YR")
            ,col("PRIOR_DIALTYPE").cast(IntegerType()).alias("PRIOR_DLYS_TYPE_ID")
            ,col("DIALSET").cast(IntegerType()).alias("DLYS_SETG_ID")
            ,col("EPO_ID").alias("PTNT_EPO_ID")
            ,col("DX_QUARTER").alias("DLYS_QTR_NUM")
            ,col("DX_PROVIDER_ID").alias("DLYS_FAC_CCN_NUM")
            ,col("DX_LAST_DATE").alias("DLYS_LAST_DT")
            ,col("NURSING_HOME_INDIC").alias("NH_IND")
            ,col("NPI").alias("NPI_ID")
            ,col("COMMENTS").alias("CMMTS_TXT")
            ,col("DATE_CREATED").alias("SYS_CREAT_TIME")
            ,col("CREATED_BY").alias("SYS_CREAT_NAME")
            ,col("DATE_MODIFIED").alias("SYS_UPDT_TIME")
            ,col("MODIFIED_BY").alias("SYS_UPDT_NAME")
         ) \


eqrs_trgt = DynamicFrame.fromDF(resultDf,glueContext,"eqrs_trgt")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = eqrs_trgt, 
            database = args['trgtDb'], 
            table_name = args['trgtTbl'], 
            transformation_ctx = "datasink5")

job.commit()
