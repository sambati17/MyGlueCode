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

args = getResolvedOptions(sys.argv, ['JOB_NAME','srcDb','srcTbl','srcTbl1','trgtDb', 'trgtTbl'])

#--srcDb = ado1-eqrs-sbx2-1-data-migration-database
#--srcTbl = remis_source_croldv85_feeddat_ipdata_feed
#--srcTbl1 = remis_source_croldv85_feeddat_iptran_feed
#--trgtDb = ado1-eqrs-sbx2-1-data-migration-database
#--trgtTbl = eqrs_eqrs_cvrg_calc_ip_stay_trnsplnt_data_feed

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

ipdata_feed = glueContext.create_dynamic_frame.from_catalog(
    database = args['srcDb'], 
    table_name = args['srcTbl'], 
    transformation_ctx = "ipdata_feed")

#ipdata_feed.printSchema()
#print ("Count:", ipdata_feed.count() )
ipdata_feed_df = ipdata_feed.toDF()
ipdata_feed_df.createOrReplaceTempView("ipdata_feed")

iptran_feed = glueContext.create_dynamic_frame.from_catalog(
    database = args['srcDb'], 
    table_name = args['srcTbl1'], 
    transformation_ctx = "iptran_feed")

#iptran_feed.printSchema()
#print ("Count:", iptran_feed.count() )
iptran_feed_df = iptran_feed.toDF()
iptran_feed_df.createOrReplaceTempView("iptran_feed")

resultDf= spark.sql("SELECT CAST((ROW_NUMBER() OVER(ORDER BY CLM_NUM) + 1099999999) AS DECIMAL(35)) AS IP_STAY_TRNSPLNT_DATA_FEED_ID \
                          , RIC_CD \
                          , CLM_NUM \
                          , BIC_CD \
                          , PTNT_LAST_NAME \
                          , PTNT_1ST_NAME \
                          , PTNT_MDL_INITL_TXT \
                          , GNDR_TXT \
                          , BIRTH_DT \
                          , DISP_CODE \
                          , REC_RSN_CD \
                          , IP_STAY_FAC_CCN_NUM \
                          , IP_STAY_STRT_DT \
                          , IP_STAY_END_DT \
                          , IP_STAY_ACRTN_DT \
                          , CAST(IP_STAY_EPO_ADMNG_NUM AS DECIMAL(10)) AS IP_STAY_EPO_ADMNG_NUM \
                          , IP_STAY_EPO_DSG_TXT \
                          , IP_STAY_EPO_HCT_TXT \
                          , CAST(IP_STAY_EPO_CHRG_NUM AS DECIMAL(10)) AS IP_STAY_EPO_CHRG_NUM \
                          , IP_STAY_DLYS_CHRG_TXT \
                          , IP_STAY_SETG_TYPE_ID \
                          , IP_STAY_DLYS_TYPE_CD \
                          , IP_STAY_SESN_NUM \
                          , BLD_FRNSH_CD \
                          , REC_LVL_CD \
                          , IP_STAY_DLYS_IND \
                          , IP_STAY_EPO_IND \
                          , PRSTATE_CD \
                          , IP_STAY_INTRMDRY_CD \
                          , ADMSN_DT \
                          , DSCHRG_DT \
                          , IP_STAY_STUS_TXT \
                          , IP_STAY_STUS_CD \
                          , IP_STAY_DRG_CD \
                          , IP_STAY_DRG_OL_CD \
                          , KDNY_ACQSTN_CHRG_NUM \
                          , DONOR_TYPE_CD \
                          , IP_STAY_DGNS_IND \
                          , CAST(IP_STAY_DGNS_CNT AS DECIMAL(5)) AS IP_STAY_DGNS_CNT \
                          , IP_STAY_DGNS_1_CD \
                          , IP_STAY_DGNS_1_RVSN_CD \
                          , IP_STAY_DGNS_2_CD \
                          , IP_STAY_DGNS_2_RVSN_CD \
                          , IP_STAY_DGNS_3_CD \
                          , IP_STAY_DGNS_3_RVSN_CD \
                          , IP_STAY_DGNS_4_CD \
                          , IP_STAY_DGNS_4_RVSN_CD \
                          , IP_STAY_DGNS_5_CD \
                          , IP_STAY_DGNS_5_RVSN_CD \
                          , IP_STAY_DGNS_6_CD \
                          , IP_STAY_DGNS_6_RVSN_CD \
                          , IP_STAY_DGNS_7_CD \
                          , IP_STAY_DGNS_7_RVSN_CD \
                          , IP_STAY_DGNS_8_CD \
                          , IP_STAY_DGNS_8_RVSN_CD \
                          , IP_STAY_DGNS_9_CD \
                          , IP_STAY_DGNS_9_RVSN_CD \
                          , IP_STAY_DGNS_10_CD \
                          , IP_STAY_DGNS_10_RVSN_CD \
                          , IP_STAY_DGNS_11_CD \
                          , IP_STAY_DGNS_11_RVSN_CD \
                          , IP_STAY_DGNS_12_CD \
                          , IP_STAY_DGNS_12_RVSN_CD \
                          , IP_STAY_DGNS_13_CD \
                          , IP_STAY_DGNS_13_RVSN_CD \
                          , IP_STAY_DGNS_14_CD \
                          , IP_STAY_DGNS_14_RVSN_CD \
                          , IP_STAY_DGNS_15_CD \
                          , IP_STAY_DGNS_15_RVSN_CD \
                          , IP_STAY_DGNS_16_CD \
                          , IP_STAY_DGNS_16_RVSN_CD \
                          , IP_STAY_DGNS_17_CD \
                          , IP_STAY_DGNS_17_RVSN_CD \
                          , IP_STAY_DGNS_18_CD \
                          , IP_STAY_DGNS_18_RVSN_CD \
                          , IP_STAY_DGNS_19_CD \
                          , IP_STAY_DGNS_19_RVSN_CD \
                          , IP_STAY_DGNS_20_CD \
                          , IP_STAY_DGNS_20_RVSN_CD \
                          , IP_STAY_DGNS_21_CD \
                          , IP_STAY_DGNS_21_RVSN_CD \
                          , IP_STAY_DGNS_22_CD \
                          , IP_STAY_DGNS_22_RVSN_CD \
                          , IP_STAY_DGNS_23_CD \
                          , IP_STAY_DGNS_23_RVSN_CD \
                          , IP_STAY_DGNS_24_CD \
                          , IP_STAY_DGNS_24_RVSN_CD \
                          , IP_STAY_DGNS_25_CD \
                          , IP_STAY_DGNS_25_RVSN_CD \
                          , IP_STAY_PRCDR_IND \
                          , CAST(IP_STAY_PRCDR_CNT AS DECIMAL(5)) AS IP_STAY_PRCDR_CNT \
                          , IP_STAY_PRCDR_1_CD \
                          , IP_STAY_PRCDR_1_DT \
                          , IP_STAY_PRCDR_1_RVSN_CD \
                          , IP_STAY_PRCDR_2_CD \
                          , IP_STAY_PRCDR_2_DT \
                          , IP_STAY_PRCDR_2_RVSN_CD \
                          , IP_STAY_PRCDR_3_CD \
                          , IP_STAY_PRCDR_3_DT \
                          , IP_STAY_PRCDR_3_RVSN_CD \
                          , IP_STAY_PRCDR_4_CD \
                          , IP_STAY_PRCDR_4_DT \
                          , IP_STAY_PRCDR_4_RVSN_CD \
                          , IP_STAY_PRCDR_5_CD \
                          , IP_STAY_PRCDR_5_DT \
                          , IP_STAY_PRCDR_5_RVSN_CD \
                          , IP_STAY_PRCDR_6_CD \
                          , IP_STAY_PRCDR_6_DT \
                          , IP_STAY_PRCDR_6_RVSN_CD \
                          , IP_STAY_PRCDR_7_CD \
                          , IP_STAY_PRCDR_7_DT \
                          , IP_STAY_PRCDR_7_RVSN_CD \
                          , IP_STAY_PRCDR_8_CD \
                          , IP_STAY_PRCDR_8_DT \
                          , IP_STAY_PRCDR_8_RVSN_CD \
                          , IP_STAY_PRCDR_9_CD \
                          , IP_STAY_PRCDR_9_DT \
                          , IP_STAY_PRCDR_9_RVSN_CD \
                          , IP_STAY_PRCDR_10_CD \
                          , IP_STAY_PRCDR_10_DT \
                          , IP_STAY_PRCDR_10_RVSN_CD \
                          , IP_STAY_PRCDR_11_CD \
                          , IP_STAY_PRCDR_11_DT \
                          , IP_STAY_PRCDR_11_RVSN_CD \
                          , IP_STAY_PRCDR_12_CD \
                          , IP_STAY_PRCDR_12_DT \
                          , IP_STAY_PRCDR_12_RVSN_CD \
                          , IP_STAY_PRCDR_13_CD \
                          , IP_STAY_PRCDR_13_DT \
                          , IP_STAY_PRCDR_13_RVSN_CD \
                          , IP_STAY_PRCDR_14_CD \
                          , IP_STAY_PRCDR_14_DT \
                          , IP_STAY_PRCDR_14_RVSN_CD \
                          , IP_STAY_PRCDR_15_CD \
                          , IP_STAY_PRCDR_15_DT \
                          , IP_STAY_PRCDR_15_RVSN_CD \
                          , IP_STAY_PRCDR_16_CD \
                          , IP_STAY_PRCDR_16_DT \
                          , IP_STAY_PRCDR_16_RVSN_CD \
                          , IP_STAY_PRCDR_17_CD \
                          , IP_STAY_PRCDR_17_DT \
                          , IP_STAY_PRCDR_17_RVSN_CD \
                          , IP_STAY_PRCDR_18_CD \
                          , IP_STAY_PRCDR_18_DT \
                          , IP_STAY_PRCDR_18_RVSN_CD \
                          , IP_STAY_PRCDR_19_CD \
                          , IP_STAY_PRCDR_19_DT \
                          , IP_STAY_PRCDR_19_RVSN_CD \
                          , IP_STAY_PRCDR_20_CD \
                          , IP_STAY_PRCDR_20_DT \
                          , IP_STAY_PRCDR_20_RVSN_CD \
                          , IP_STAY_PRCDR_21_CD \
                          , IP_STAY_PRCDR_21_DT \
                          , IP_STAY_PRCDR_21_RVSN_CD \
                          , IP_STAY_PRCDR_22_CD \
                          , IP_STAY_PRCDR_22_DT \
                          , IP_STAY_PRCDR_22_RVSN_CD \
                          , IP_STAY_PRCDR_23_CD \
                          , IP_STAY_PRCDR_23_DT \
                          , IP_STAY_PRCDR_23_RVSN_CD \
                          , IP_STAY_PRCDR_24_CD \
                          , IP_STAY_PRCDR_24_DT \
                          , IP_STAY_PRCDR_24_RVSN_CD \
                          , IP_STAY_PRCDR_25_CD \
                          , IP_STAY_PRCDR_25_DT \
                          , IP_STAY_PRCDR_25_RVSN_CD \
                          , NPI_ID \
                          , SYS_CREAT_TIME \
                          , SYS_CREAT_NAME \
                          , SYS_UPDT_TIME \
                          , SYS_UPDT_NAME \
                          , IP_STAY_FEED_IND \
                     FROM (SELECT RIC_CD AS RIC_CD \
                                , CLAIM_NUM AS CLM_NUM \
                                , BIC AS BIC_CD \
                                , SURNAME AS PTNT_LAST_NAME \
                                , FRSTINIT AS PTNT_1ST_NAME \
                                , MDL_INIT AS PTNT_MDL_INITL_TXT \
                                , CASE WHEN SEX_CODE='1' THEN 'M' WHEN SEX_CODE='2' THEN 'F' ELSE 'U' END AS GNDR_TXT \
                                , CAST(TO_DATE(BIRTH_DATE,'yyyymmdd') AS DATE) AS BIRTH_DT \
                                , DISP_CODE AS DISP_CODE \
                                , RCREASON AS REC_RSN_CD \
                                , IP_PROVIDER AS IP_STAY_FAC_CCN_NUM \
                                , CAST(TO_DATE(IP_START_DATE,'yyyymmdd') AS DATE) AS IP_STAY_STRT_DT \
                                , CAST(TO_DATE(IP_END_DATE,'yyyymmdd') AS DATE) AS IP_STAY_END_DT \
                                , CAST(TO_DATE(IP_ACRTN_DATE,'yyyymmdd') AS DATE) AS IP_STAY_ACRTN_DT \
                                , IP_EPO_ADMIN AS IP_STAY_EPO_ADMNG_NUM \
                                , IP_EPO_DOSE AS IP_STAY_EPO_DSG_TXT \
                                , IP_EPO_HEMAT AS IP_STAY_EPO_HCT_TXT \
                                , IP_EPO_CHARGE AS IP_STAY_EPO_CHRG_NUM \
                                , IP_DIAL_CHARGE AS IP_STAY_DLYS_CHRG_TXT \
                                , SETTING AS IP_STAY_SETG_TYPE_ID \
                                , IP_TYPE_DX AS IP_STAY_DLYS_TYPE_CD \
                                , IP_SESS AS IP_STAY_SESN_NUM \
                                , BLDFRNSH AS BLD_FRNSH_CD \
                                , REC_LVL AS REC_LVL_CD \
                                , IP_DX_IND AS IP_STAY_DLYS_IND \
                                , IP_EPO_IND AS IP_STAY_EPO_IND \
                                , PRSTATE AS PRSTATE_CD \
                                , IP_INTR AS IP_STAY_INTRMDRY_CD \
                                , CAST(TO_DATE(ADMSN_DATE,'yyyymmdd') AS DATE) AS ADMSN_DT \
                                , CAST(TO_DATE(DSCHRG_DATE,'yyyymmdd') AS DATE) AS DSCHRG_DT \
                                , IP_STATUS AS IP_STAY_STUS_TXT \
                                , IP_STATUS_CODE AS IP_STAY_STUS_CD \
                                , IP_DRG_CODE AS IP_STAY_DRG_CD \
                                , IP_DRG_OL AS IP_STAY_DRG_OL_CD \
                                , CAST(IP_KAC AS DECIMAL(10,3)) AS KDNY_ACQSTN_CHRG_NUM \
                                , IP_DONOR_TYPE AS DONOR_TYPE_CD \
                                , IP_DGNS_IND AS IP_STAY_DGNS_IND \
                                , IP_DGC_COUNT AS IP_STAY_DGNS_CNT \
                                , IP_DIAG_CODE1 AS IP_STAY_DGNS_1_CD \
                                , IP_DIAG_VER1 AS IP_STAY_DGNS_1_RVSN_CD \
                                , IP_DIAG_CODE2 AS IP_STAY_DGNS_2_CD \
                                , IP_DIAG_VER2 AS IP_STAY_DGNS_2_RVSN_CD \
                                , IP_DIAG_CODE3 AS IP_STAY_DGNS_3_CD \
                                , IP_DIAG_VER3 AS IP_STAY_DGNS_3_RVSN_CD \
                                , IP_DIAG_CODE4 AS IP_STAY_DGNS_4_CD \
                                , IP_DIAG_VER4 AS IP_STAY_DGNS_4_RVSN_CD \
                                , IP_DIAG_CODE5 AS IP_STAY_DGNS_5_CD \
                                , IP_DIAG_VER5 AS IP_STAY_DGNS_5_RVSN_CD \
                                , IP_DIAG_CODE6 AS IP_STAY_DGNS_6_CD \
                                , IP_DIAG_VER6 AS IP_STAY_DGNS_6_RVSN_CD \
                                , IP_DIAG_CODE7 AS IP_STAY_DGNS_7_CD \
                                , IP_DIAG_VER7 AS IP_STAY_DGNS_7_RVSN_CD \
                                , IP_DIAG_CODE8 AS IP_STAY_DGNS_8_CD \
                                , IP_DIAG_VER8 AS IP_STAY_DGNS_8_RVSN_CD \
                                , IP_DIAG_CODE9 AS IP_STAY_DGNS_9_CD \
                                , IP_DIAG_VER9 AS IP_STAY_DGNS_9_RVSN_CD \
                                , IP_DIAG_CODE10 AS IP_STAY_DGNS_10_CD \
                                , IP_DIAG_VER10 AS IP_STAY_DGNS_10_RVSN_CD \
                                , IP_DIAG_CODE11 AS IP_STAY_DGNS_11_CD \
                                , IP_DIAG_VER11 AS IP_STAY_DGNS_11_RVSN_CD \
                                , IP_DIAG_CODE12 AS IP_STAY_DGNS_12_CD \
                                , IP_DIAG_VER12 AS IP_STAY_DGNS_12_RVSN_CD \
                                , IP_DIAG_CODE13 AS IP_STAY_DGNS_13_CD \
                                , IP_DIAG_VER9 AS IP_STAY_DGNS_13_RVSN_CD \
                                , IP_DIAG_CODE14 AS IP_STAY_DGNS_14_CD \
                                , IP_DIAG_VER14 AS IP_STAY_DGNS_14_RVSN_CD \
                                , IP_DIAG_CODE15 AS IP_STAY_DGNS_15_CD \
                                , IP_DIAG_VER15 AS IP_STAY_DGNS_15_RVSN_CD \
                                , IP_DIAG_CODE16 AS IP_STAY_DGNS_16_CD \
                                , IP_DIAG_VER16 AS IP_STAY_DGNS_16_RVSN_CD \
                                , IP_DIAG_CODE17 AS IP_STAY_DGNS_17_CD \
                                , IP_DIAG_VER17 AS IP_STAY_DGNS_17_RVSN_CD \
                                , IP_DIAG_CODE18 AS IP_STAY_DGNS_18_CD \
                                , IP_DIAG_VER18 AS IP_STAY_DGNS_18_RVSN_CD \
                                , IP_DIAG_CODE19 AS IP_STAY_DGNS_19_CD \
                                , IP_DIAG_VER19 AS IP_STAY_DGNS_19_RVSN_CD \
                                , IP_DIAG_CODE20 AS IP_STAY_DGNS_20_CD \
                                , IP_DIAG_VER20 AS IP_STAY_DGNS_20_RVSN_CD \
                                , IP_DIAG_CODE21 AS IP_STAY_DGNS_21_CD \
                                , IP_DIAG_VER21 AS IP_STAY_DGNS_21_RVSN_CD \
                                , IP_DIAG_CODE22 AS IP_STAY_DGNS_22_CD \
                                , IP_DIAG_VER22 AS IP_STAY_DGNS_22_RVSN_CD \
                                , IP_DIAG_CODE23 AS IP_STAY_DGNS_23_CD \
                                , IP_DIAG_VER23 AS IP_STAY_DGNS_23_RVSN_CD \
                                , IP_DIAG_CODE24 AS IP_STAY_DGNS_24_CD \
                                , IP_DIAG_VER24 AS IP_STAY_DGNS_24_RVSN_CD \
                                , IP_DIAG_CODE25 AS IP_STAY_DGNS_25_CD \
                                , IP_DIAG_VER25 AS IP_STAY_DGNS_25_RVSN_CD \
                                , IP_SURG_IND AS IP_STAY_PRCDR_IND \
                                , IP_PRCNT AS IP_STAY_PRCDR_CNT \
                                , IP_SURG_CODE1 AS IP_STAY_PRCDR_1_CD \
                                , CAST(TO_DATE(IP_SURG_DATE1,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_1_DT \
                                , IP_SURG_VER1 AS IP_STAY_PRCDR_1_RVSN_CD \
                                , IP_SURG_CODE2 AS IP_STAY_PRCDR_2_CD \
                                , CAST(TO_DATE(IP_SURG_DATE2,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_2_DT \
                                , IP_SURG_VER2 AS IP_STAY_PRCDR_2_RVSN_CD \
                                , IP_SURG_CODE3 AS IP_STAY_PRCDR_3_CD \
                                , CAST(TO_DATE(IP_SURG_DATE3,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_3_DT \
                                , IP_SURG_VER3 AS IP_STAY_PRCDR_3_RVSN_CD \
                                , IP_SURG_CODE4 AS IP_STAY_PRCDR_4_CD \
                                , CAST(TO_DATE(IP_SURG_DATE4,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_4_DT \
                                , IP_SURG_VER4 AS IP_STAY_PRCDR_4_RVSN_CD \
                                , IP_SURG_CODE5 AS IP_STAY_PRCDR_5_CD \
                                , CAST(TO_DATE(IP_SURG_DATE5,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_5_DT \
                                , IP_SURG_VER5 AS IP_STAY_PRCDR_5_RVSN_CD \
                                , IP_SURG_CODE6 AS IP_STAY_PRCDR_6_CD \
                                , CAST(TO_DATE(IP_SURG_DATE6,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_6_DT \
                                , IP_SURG_VER6 AS IP_STAY_PRCDR_6_RVSN_CD \
                                , IP_SURG_CODE7 AS IP_STAY_PRCDR_7_CD \
                                , CAST(TO_DATE(IP_SURG_DATE7,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_7_DT \
                                , IP_SURG_VER7 AS IP_STAY_PRCDR_7_RVSN_CD \
                                , IP_SURG_CODE8 AS IP_STAY_PRCDR_8_CD \
                                , CAST(TO_DATE(IP_SURG_DATE8,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_8_DT \
                                , IP_SURG_VER8 AS IP_STAY_PRCDR_8_RVSN_CD \
                                , IP_SURG_CODE9 AS IP_STAY_PRCDR_9_CD \
                                , CAST(TO_DATE(IP_SURG_DATE9,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_9_DT \
                                , IP_SURG_VER9 AS IP_STAY_PRCDR_9_RVSN_CD \
                                , IP_SURG_CODE10 AS IP_STAY_PRCDR_10_CD \
                                , CAST(TO_DATE(IP_SURG_DATE10,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_10_DT \
                                , IP_SURG_VER10 AS IP_STAY_PRCDR_10_RVSN_CD \
                                , IP_SURG_CODE11 AS IP_STAY_PRCDR_11_CD \
                                , CAST(TO_DATE(IP_SURG_DATE11,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_11_DT \
                                , IP_SURG_VER11 AS IP_STAY_PRCDR_11_RVSN_CD \
                                , IP_SURG_CODE12 AS IP_STAY_PRCDR_12_CD \
                                , CAST(TO_DATE(IP_SURG_DATE12,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_12_DT \
                                , IP_SURG_VER12 AS IP_STAY_PRCDR_12_RVSN_CD \
                                , IP_SURG_CODE13 AS IP_STAY_PRCDR_13_CD \
                                , CAST(TO_DATE(IP_SURG_DATE13,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_13_DT \
                                , IP_SURG_VER13 AS IP_STAY_PRCDR_13_RVSN_CD \
                                , IP_SURG_CODE14 AS IP_STAY_PRCDR_14_CD \
                                , CAST(TO_DATE(IP_SURG_DATE14,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_14_DT \
                                , IP_SURG_VER14 AS IP_STAY_PRCDR_14_RVSN_CD \
                                , IP_SURG_CODE15 AS IP_STAY_PRCDR_15_CD \
                                , CAST(TO_DATE(IP_SURG_DATE15,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_15_DT \
                                , IP_SURG_VER15 AS IP_STAY_PRCDR_15_RVSN_CD \
                                , IP_SURG_CODE16 AS IP_STAY_PRCDR_16_CD \
                                , CAST(TO_DATE(IP_SURG_DATE16,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_16_DT \
                                , IP_SURG_VER16 AS IP_STAY_PRCDR_16_RVSN_CD \
                                , IP_SURG_CODE17 AS IP_STAY_PRCDR_17_CD \
                                , CAST(TO_DATE(IP_SURG_DATE17,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_17_DT \
                                , IP_SURG_VER17 AS IP_STAY_PRCDR_17_RVSN_CD \
                                , IP_SURG_CODE18 AS IP_STAY_PRCDR_18_CD \
                                , CAST(TO_DATE(IP_SURG_DATE18,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_18_DT \
                                , IP_SURG_VER18 AS IP_STAY_PRCDR_18_RVSN_CD \
                                , IP_SURG_CODE19 AS IP_STAY_PRCDR_19_CD \
                                , CAST(TO_DATE(IP_SURG_DATE19,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_19_DT \
                                , IP_SURG_VER19 AS IP_STAY_PRCDR_19_RVSN_CD \
                                , IP_SURG_CODE20 AS IP_STAY_PRCDR_20_CD \
                                , CAST(TO_DATE(IP_SURG_DATE20,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_20_DT \
                                , IP_SURG_VER20 AS IP_STAY_PRCDR_20_RVSN_CD \
                                , IP_SURG_CODE21 AS IP_STAY_PRCDR_21_CD \
                                , CAST(TO_DATE(IP_SURG_DATE21,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_21_DT \
                                , IP_SURG_VER21 AS IP_STAY_PRCDR_21_RVSN_CD \
                                , IP_SURG_CODE22 AS IP_STAY_PRCDR_22_CD \
                                , CAST(TO_DATE(IP_SURG_DATE22,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_22_DT \
                                , IP_SURG_VER22 AS IP_STAY_PRCDR_22_RVSN_CD \
                                , IP_SURG_CODE23 AS IP_STAY_PRCDR_23_CD \
                                , CAST(TO_DATE(IP_SURG_DATE23,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_23_DT \
                                , IP_SURG_VER23 AS IP_STAY_PRCDR_23_RVSN_CD \
                                , IP_SURG_CODE24 AS IP_STAY_PRCDR_24_CD \
                                , CAST(TO_DATE(IP_SURG_DATE24,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_24_DT \
                                , IP_SURG_VER24 AS IP_STAY_PRCDR_24_RVSN_CD \
                                , IP_SURG_CODE25 AS IP_STAY_PRCDR_25_CD \
                                , CAST(TO_DATE(IP_SURG_DATE25,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_25_DT \
                                , IP_SURG_VER25 AS IP_STAY_PRCDR_25_RVSN_CD \
                                , NPI AS NPI_ID \
                                , DATE_CREATED AS SYS_CREAT_TIME \
                                , 'MIGRATION' AS SYS_CREAT_NAME \
                                , CURRENT_TIMESTAMP AS SYS_UPDT_TIME \
                                , 'MIGRATION' AS SYS_UPDT_NAME \
                                , 'IPS' AS IP_STAY_FEED_IND \
                           FROM ipdata_feed \
                           WHERE RECORD_STATUS IN ('R','M','H') \
                           UNION \
                           SELECT RIC_CD AS RIC_CD \
                                , CLAIM_NUM AS CLM_NUM \
                                , BIC AS BIC_CD \
                                , SURNAME AS PTNT_LAST_NAME \
                                , FRSTINIT AS PTNT_1ST_NAME \
                                , MDL_INIT AS PTNT_MDL_INITL_TXT \
                                , CASE WHEN SEX_CODE='1' THEN 'M' WHEN SEX_CODE='2' THEN 'F' ELSE 'U' END AS GNDR_TXT \
                                , CAST(TO_DATE(BIRTH_DATE,'yyyymmdd') AS DATE) AS BIRTH_DT \
                                , DISP_CODE AS DISP_CODE \
                                , RCREASON AS REC_RSN_CD \
                                , IP_PROVIDER AS IP_STAY_FAC_CCN_NUM \
                                , CAST(TO_DATE(IP_START_DATE,'yyyymmdd') AS DATE) AS IP_STAY_STRT_DT \
                                , CAST(TO_DATE(IP_END_DATE,'yyyymmdd') AS DATE) AS IP_STAY_END_DT \
                                , CAST(TO_DATE(IP_ACRTN_DATE,'yyyymmdd') AS DATE) AS IP_STAY_ACRTN_DT \
                                , IP_EPO_ADMIN AS IP_STAY_EPO_ADMNG_NUM \
                                , IP_EPO_DOSE AS IP_STAY_EPO_DSG_TXT \
                                , IP_EPO_HEMAT AS IP_STAY_EPO_HCT_TXT \
                                , IP_EPO_CHARGE AS IP_STAY_EPO_CHRG_NUM \
                                , IP_DIAL_CHARGE AS IP_STAY_DLYS_CHRG_TXT \
                                , SETTING AS IP_STAY_SETG_TYPE_ID \
                                , IP_TYPE_DX AS IP_STAY_DLYS_TYPE_CD \
                                , IP_SESS AS IP_STAY_SESN_NUM \
                                , BLDFRNSH AS BLD_FRNSH_CD \
                                , REC_LVL AS REC_LVL_CD \
                                , IP_DX_IND AS IP_STAY_DLYS_IND \
                                , IP_EPO_IND AS IP_STAY_EPO_IND \
                                , PRSTATE AS PRSTATE_CD \
                                , IP_INTR AS IP_STAY_INTRMDRY_CD \
                                , CAST(TO_DATE(ADMSN_DATE,'yyyymmdd') AS DATE) AS ADMSN_DT \
                                , CAST(TO_DATE(DSCHRG_DATE,'yyyymmdd') AS DATE) AS DSCHRG_DT \
                                , IP_STATUS AS IP_STAY_STUS_TXT \
                                , IP_STATUS_CODE AS IP_STAY_STUS_CD \
                                , IP_DRG_CODE AS IP_STAY_DRG_CD \
                                , IP_DRG_OL AS IP_STAY_DRG_OL_CD \
                                , CAST(IP_KAC AS DECIMAL(10,3)) AS KDNY_ACQSTN_CHRG_NUM \
                                , IP_DONOR_TYPE AS DONOR_TYPE_CD \
                                , IP_DGNS_IND AS IP_STAY_DGNS_IND \
                                , IP_DGC_COUNT AS IP_STAY_DGNS_CNT \
                                , IP_DIAG_CODE1 AS IP_STAY_DGNS_1_CD \
                                , IP_DIAG_VER1 AS IP_STAY_DGNS_1_RVSN_CD \
                                , IP_DIAG_CODE2 AS IP_STAY_DGNS_2_CD \
                                , IP_DIAG_VER2 AS IP_STAY_DGNS_2_RVSN_CD \
                                , IP_DIAG_CODE3 AS IP_STAY_DGNS_3_CD \
                                , IP_DIAG_VER3 AS IP_STAY_DGNS_3_RVSN_CD \
                                , IP_DIAG_CODE4 AS IP_STAY_DGNS_4_CD \
                                , IP_DIAG_VER4 AS IP_STAY_DGNS_4_RVSN_CD \
                                , IP_DIAG_CODE5 AS IP_STAY_DGNS_5_CD \
                                , IP_DIAG_VER5 AS IP_STAY_DGNS_5_RVSN_CD \
                                , IP_DIAG_CODE6 AS IP_STAY_DGNS_6_CD \
                                , IP_DIAG_VER6 AS IP_STAY_DGNS_6_RVSN_CD \
                                , IP_DIAG_CODE7 AS IP_STAY_DGNS_7_CD \
                                , IP_DIAG_VER7 AS IP_STAY_DGNS_7_RVSN_CD \
                                , IP_DIAG_CODE8 AS IP_STAY_DGNS_8_CD \
                                , IP_DIAG_VER8 AS IP_STAY_DGNS_8_RVSN_CD \
                                , IP_DIAG_CODE9 AS IP_STAY_DGNS_9_CD \
                                , IP_DIAG_VER9 AS IP_STAY_DGNS_9_RVSN_CD \
                                , IP_DIAG_CODE10 AS IP_STAY_DGNS_10_CD \
                                , IP_DIAG_VER10 AS IP_STAY_DGNS_10_RVSN_CD \
                                , IP_DIAG_CODE11 AS IP_STAY_DGNS_11_CD \
                                , IP_DIAG_VER11 AS IP_STAY_DGNS_11_RVSN_CD \
                                , IP_DIAG_CODE12 AS IP_STAY_DGNS_12_CD \
                                , IP_DIAG_VER12 AS IP_STAY_DGNS_12_RVSN_CD \
                                , IP_DIAG_CODE13 AS IP_STAY_DGNS_13_CD \
                                , IP_DIAG_VER9 AS IP_STAY_DGNS_13_RVSN_CD \
                                , IP_DIAG_CODE14 AS IP_STAY_DGNS_14_CD \
                                , IP_DIAG_VER14 AS IP_STAY_DGNS_14_RVSN_CD \
                                , IP_DIAG_CODE15 AS IP_STAY_DGNS_15_CD \
                                , IP_DIAG_VER15 AS IP_STAY_DGNS_15_RVSN_CD \
                                , IP_DIAG_CODE16 AS IP_STAY_DGNS_16_CD \
                                , IP_DIAG_VER16 AS IP_STAY_DGNS_16_RVSN_CD \
                                , IP_DIAG_CODE17 AS IP_STAY_DGNS_17_CD \
                                , IP_DIAG_VER17 AS IP_STAY_DGNS_17_RVSN_CD \
                                , IP_DIAG_CODE18 AS IP_STAY_DGNS_18_CD \
                                , IP_DIAG_VER18 AS IP_STAY_DGNS_18_RVSN_CD \
                                , IP_DIAG_CODE19 AS IP_STAY_DGNS_19_CD \
                                , IP_DIAG_VER19 AS IP_STAY_DGNS_19_RVSN_CD \
                                , IP_DIAG_CODE20 AS IP_STAY_DGNS_20_CD \
                                , IP_DIAG_VER20 AS IP_STAY_DGNS_20_RVSN_CD \
                                , IP_DIAG_CODE21 AS IP_STAY_DGNS_21_CD \
                                , IP_DIAG_VER21 AS IP_STAY_DGNS_21_RVSN_CD \
                                , IP_DIAG_CODE22 AS IP_STAY_DGNS_22_CD \
                                , IP_DIAG_VER22 AS IP_STAY_DGNS_22_RVSN_CD \
                                , IP_DIAG_CODE23 AS IP_STAY_DGNS_23_CD \
                                , IP_DIAG_VER23 AS IP_STAY_DGNS_23_RVSN_CD \
                                , IP_DIAG_CODE24 AS IP_STAY_DGNS_24_CD \
                                , IP_DIAG_VER24 AS IP_STAY_DGNS_24_RVSN_CD \
                                , IP_DIAG_CODE25 AS IP_STAY_DGNS_25_CD \
                                , IP_DIAG_VER25 AS IP_STAY_DGNS_25_RVSN_CD \
                                , IP_SURG_IND AS IP_STAY_PRCDR_IND \
                                , IP_PRCNT AS IP_STAY_PRCDR_CNT \
                                , IP_SURG_CODE1 AS IP_STAY_PRCDR_1_CD \
                                , CAST(TO_DATE(IP_SURG_DATE1,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_1_DT \
                                , IP_SURG_VER1 AS IP_STAY_PRCDR_1_RVSN_CD \
                                , IP_SURG_CODE2 AS IP_STAY_PRCDR_2_CD \
                                , CAST(TO_DATE(IP_SURG_DATE2,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_2_DT \
                                , IP_SURG_VER2 AS IP_STAY_PRCDR_2_RVSN_CD \
                                , IP_SURG_CODE3 AS IP_STAY_PRCDR_3_CD \
                                , CAST(TO_DATE(IP_SURG_DATE3,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_3_DT \
                                , IP_SURG_VER3 AS IP_STAY_PRCDR_3_RVSN_CD \
                                , IP_SURG_CODE4 AS IP_STAY_PRCDR_4_CD \
                                , CAST(TO_DATE(IP_SURG_DATE4,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_4_DT \
                                , IP_SURG_VER4 AS IP_STAY_PRCDR_4_RVSN_CD \
                                , IP_SURG_CODE5 AS IP_STAY_PRCDR_5_CD \
                                , CAST(TO_DATE(IP_SURG_DATE5,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_5_DT \
                                , IP_SURG_VER5 AS IP_STAY_PRCDR_5_RVSN_CD \
                                , IP_SURG_CODE6 AS IP_STAY_PRCDR_6_CD \
                                , CAST(TO_DATE(IP_SURG_DATE6,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_6_DT \
                                , IP_SURG_VER6 AS IP_STAY_PRCDR_6_RVSN_CD \
                                , IP_SURG_CODE7 AS IP_STAY_PRCDR_7_CD \
                                , CAST(TO_DATE(IP_SURG_DATE7,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_7_DT \
                                , IP_SURG_VER7 AS IP_STAY_PRCDR_7_RVSN_CD \
                                , IP_SURG_CODE8 AS IP_STAY_PRCDR_8_CD \
                                , CAST(TO_DATE(IP_SURG_DATE8,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_8_DT \
                                , IP_SURG_VER8 AS IP_STAY_PRCDR_8_RVSN_CD \
                                , IP_SURG_CODE9 AS IP_STAY_PRCDR_9_CD \
                                , CAST(TO_DATE(IP_SURG_DATE9,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_9_DT \
                                , IP_SURG_VER9 AS IP_STAY_PRCDR_9_RVSN_CD \
                                , IP_SURG_CODE10 AS IP_STAY_PRCDR_10_CD \
                                , CAST(TO_DATE(IP_SURG_DATE10,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_10_DT \
                                , IP_SURG_VER10 AS IP_STAY_PRCDR_10_RVSN_CD \
                                , IP_SURG_CODE11 AS IP_STAY_PRCDR_11_CD \
                                , CAST(TO_DATE(IP_SURG_DATE11,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_11_DT \
                                , IP_SURG_VER11 AS IP_STAY_PRCDR_11_RVSN_CD \
                                , IP_SURG_CODE12 AS IP_STAY_PRCDR_12_CD \
                                , CAST(TO_DATE(IP_SURG_DATE12,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_12_DT \
                                , IP_SURG_VER12 AS IP_STAY_PRCDR_12_RVSN_CD \
                                , IP_SURG_CODE13 AS IP_STAY_PRCDR_13_CD \
                                , CAST(TO_DATE(IP_SURG_DATE13,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_13_DT \
                                , IP_SURG_VER13 AS IP_STAY_PRCDR_13_RVSN_CD \
                                , IP_SURG_CODE14 AS IP_STAY_PRCDR_14_CD \
                                , CAST(TO_DATE(IP_SURG_DATE14,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_14_DT \
                                , IP_SURG_VER14 AS IP_STAY_PRCDR_14_RVSN_CD \
                                , IP_SURG_CODE15 AS IP_STAY_PRCDR_15_CD \
                                , CAST(TO_DATE(IP_SURG_DATE15,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_15_DT \
                                , IP_SURG_VER15 AS IP_STAY_PRCDR_15_RVSN_CD \
                                , IP_SURG_CODE16 AS IP_STAY_PRCDR_16_CD \
                                , CAST(TO_DATE(IP_SURG_DATE16,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_16_DT \
                                , IP_SURG_VER16 AS IP_STAY_PRCDR_16_RVSN_CD \
                                , IP_SURG_CODE17 AS IP_STAY_PRCDR_17_CD \
                                , CAST(TO_DATE(IP_SURG_DATE17,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_17_DT \
                                , IP_SURG_VER17 AS IP_STAY_PRCDR_17_RVSN_CD \
                                , IP_SURG_CODE18 AS IP_STAY_PRCDR_18_CD \
                                , CAST(TO_DATE(IP_SURG_DATE18,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_18_DT \
                                , IP_SURG_VER18 AS IP_STAY_PRCDR_18_RVSN_CD \
                                , IP_SURG_CODE19 AS IP_STAY_PRCDR_19_CD \
                                , CAST(TO_DATE(IP_SURG_DATE19,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_19_DT \
                                , IP_SURG_VER19 AS IP_STAY_PRCDR_19_RVSN_CD \
                                , IP_SURG_CODE20 AS IP_STAY_PRCDR_20_CD \
                                , CAST(TO_DATE(IP_SURG_DATE20,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_20_DT \
                                , IP_SURG_VER20 AS IP_STAY_PRCDR_20_RVSN_CD \
                                , IP_SURG_CODE21 AS IP_STAY_PRCDR_21_CD \
                                , CAST(TO_DATE(IP_SURG_DATE21,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_21_DT \
                                , IP_SURG_VER21 AS IP_STAY_PRCDR_21_RVSN_CD \
                                , IP_SURG_CODE22 AS IP_STAY_PRCDR_22_CD \
                                , CAST(TO_DATE(IP_SURG_DATE22,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_22_DT \
                                , IP_SURG_VER22 AS IP_STAY_PRCDR_22_RVSN_CD \
                                , IP_SURG_CODE23 AS IP_STAY_PRCDR_23_CD \
                                , CAST(TO_DATE(IP_SURG_DATE23,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_23_DT \
                                , IP_SURG_VER23 AS IP_STAY_PRCDR_23_RVSN_CD \
                                , IP_SURG_CODE24 AS IP_STAY_PRCDR_24_CD \
                                , CAST(TO_DATE(IP_SURG_DATE24,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_24_DT \
                                , IP_SURG_VER24 AS IP_STAY_PRCDR_24_RVSN_CD \
                                , IP_SURG_CODE25 AS IP_STAY_PRCDR_25_CD \
                                , CAST(TO_DATE(IP_SURG_DATE25,'yyyymmdd') AS DATE) AS IP_STAY_PRCDR_25_DT \
                                , IP_SURG_VER25 AS IP_STAY_PRCDR_25_RVSN_CD \
                                , NPI AS NPI_ID \
                                , DATE_CREATED AS SYS_CREAT_TIME \
                                , 'MIGRATION' AS SYS_CREAT_NAME \
                                , CURRENT_TIMESTAMP AS SYS_UPDT_TIME \
                                , 'MIGRATION' AS SYS_UPDT_NAME \
                                , 'IPT' AS IP_STAY_FEED_IND \
                           FROM iptran_feed \
                           WHERE RECORD_STATUS IN ('R','') OR RECORD_STATUS IS NULL) \
                  ") \

eqrs_trgt = DynamicFrame.fromDF(resultDf,glueContext,"eqrs_trgt")

datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = eqrs_trgt, database = args['trgtDb'], table_name = args['trgtTbl'], transformation_ctx = "datasink5")

job.commit()
