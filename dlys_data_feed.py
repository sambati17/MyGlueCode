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
from pyspark.sql.functions import lit

args = getResolvedOptions(sys.argv, ['JOB_NAME','srcDb', 'srcTbl','trgtDb', 'trgtTbl'])

#--srcDb = 'ado1-eqrs-sbx2-1-data-migration-database'
#--srcTbl = 'remis_source_croldv85_feeddat_dialdata_feed'
#--trgtDb = 'ado1-eqrs-sbx2-1-data-migration-database'
#--trgtTbl = 'eqrs_eqrs_cvrg_calc_dlys_data_feed'

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dialdata_feed = glueContext.create_dynamic_frame.from_catalog(
    database = args['srcDb'], 
    table_name = args['srcTbl'], 
    transformation_ctx = "dialdata_feed")

#dialdata_feed.printSchema()

dialdata_feed_df = dialdata_feed.toDF()
dialdata_feed_df.createOrReplaceTempView("dialdata_feed")

dlys_data_feed_final= spark.sql(""" SELECT 
                                        row_number() OVER( ORDER BY CLAIM_NUM) as DLYS_DATA_FEED_ID
                                        ,RIC_CD	as	RIC_CD
                                        ,CLAIM_NUM	as	CLM_NUM
                                        ,BIC	as	BIC_CD
                                        ,SURNAME	as	PTNT_LAST_NAME
                                        ,FRSTINIT	as	PTNT_1ST_NAME
                                        ,MDL_INIT	as	PTNT_MDL_INITL_TXT
                                        ,CASE WHEN SEX_CODE ='1' THEN 'M' WHEN SEX_CODE ='2' THEN 'F' ELSE 'U' END as GNDR_TXT
                                        ,cast(to_date(BIRTH_DATE, 'yyyymmdd') as date)	as	BIRTH_DT
                                        ,DISP_CODE	as	DISP_CODE
                                        ,RCREASON	as	REC_RSN_CD
                                        ,DX_PROVIDER	as	DLYS_FAC_CCN_NUM
                                        ,cast(to_date(DX_FIRST_DATE, 'yyyymmdd')as date)	as	DLYS_1ST_DT
                                        ,cast(to_date(DX_LAST_DATE, 'yyyymmdd')as date)	    as	DLYS_LAST_DT
                                        ,cast(to_date(DX_ACRTN_DATE, 'yyyymmdd')as date)	as	DLYS_ACRTN_DT
                                        ,DX_EPO_ADMIN	as	DLYS_EPO_ADMNG_NUM
                                        ,DX_EPO_DOSE	as	DLYS_EPO_DSG_TXT
                                        ,DX_EPO_HEMAT	as	DLYS_EPO_HCT_TXT
                                        ,DX_EPO_CHARGE	as	DLYS_EPO_CHRGS_NUM
                                        ,DX_CHARGE	as	DLYS_CHRG_TXT
                                        ,DX_SETTING	as	DLYS_SETG_TYPE_ID
                                        ,DX_TYPE	as	DLYS_TYPE_CD
                                        ,DX_SESSIONS	as	DLYS_SESN_NUM
                                        ,DX_CERT_CODE	as	DLYS_CRTFCTN_CD
                                        ,DX_EPO_IND	as	DLYS_EPO_IND
                                        ,REC_LVL	as	REC_LVL_CD
                                        ,DX_BLOOD	as	DLYS_BLOOD_CD 
                                        ,NURSING_HOME_INDIC	as	NH_IND
                                        ,NPI	as	NPI_ID
                                        ,DATE_CREATED	as	SYS_CREAT_TIME
                                        ,'ETL_DataMigration'	as	SYS_CREAT_NAME
                                        ,current_timestamp	as	SYS_UPDT_TIME
                                        ,'ETL_DataMigration'	as	SYS_UPDT_NAME
                                        FROM dialdata_feed 
                                    WHERE  RECORD_STATUS IS NULL OR (RECORD_STATUS in ('R', ''))
                                    """
                               )  #.show()


eqrs_trgt = DynamicFrame.fromDF(dlys_data_feed_final,glueContext,"eqrs_trgt")
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = eqrs_trgt, 
                                                         database = args['trgtDb'], 
                                                         table_name = args['trgtTbl'], 
                                                         transformation_ctx = "datasink5")


job.commit()
