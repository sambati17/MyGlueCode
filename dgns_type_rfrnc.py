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

args = getResolvedOptions(sys.argv, ['JOB_NAME','srcDb', 'srcTbl', 'trgtDb', 'trgtTbl'])

#--srcDb = 'ado1-eqrs-sbx2-1-data-migration-database']
#--srcTbl = 'sbx2_1_croldv85_remisprd_diag_codes_lov']
#--trgtDb = 'ado1-eqrs-sbx2-1-data-migration-database']
#--trgtTbl = 'eqrs_eqrs_cvrg_calc_dgns_type_rfrnc']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

diag_codes_lov = glueContext.create_dynamic_frame.from_catalog(
    database = args['srcDb'],
    table_name =  args['srcTbl'], 
    transformation_ctx = "diag_codes_lov")

diag_codes_lov_df = diag_codes_lov.toDF()


diag_codes_lov_df  =  diag_codes_lov_df \
.select(col("DCL_ID").alias("DGNS_TYPE_ID")
        ,col("DIAG_CODE").alias("DGNS_TYPE_CD")
        ,col("DESCRIPTION").alias("DGNS_TYPE_DESC")
        ,col("DIAG_CODE_VERSION").alias("ICD_VRSN_NUM")
        ,col("COMMENTS").alias("CMMTS_TXT")
        ,col("DATE_CREATED").alias("SYS_CREAT_TIME")
        ,col("CREATED_BY").alias("SYS_CREAT_NAME")
        ,col("DATE_MODIFIED").alias("SYS_UPDT_TIME")
        ,col("MODIFIED_BY").alias("SYS_UPDT_NAME")
        ) \
#.show()
        

eqrs_trgt = DynamicFrame.fromDF(diag_codes_lov_df,glueContext,"eqrs_trgt")
#test.show()
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = eqrs_trgt, 
                                                         database = args['trgtDb'],
                                                         table_name = args['trgtTbl'], 
                                                         transformation_ctx = "datasink5")

job.commit()