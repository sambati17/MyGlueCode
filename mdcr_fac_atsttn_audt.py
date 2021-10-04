import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, 
            ['JOB_NAME',
             'srcDb',
             'srcTbl',
             'trgtDb',
             'trgtTbl'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Description: Filter out any records that are not new
# Inputs: (None)
# Returns: 
# Preconditions: (None)
# Postconditions: 
# - 
def getCwMdcrFacAtsttnAudt():
    """ Get CW MDCR_FAC_ATSTTN_AUDT table """
    ## @type: DataSource
    ## @args: [database = args['srcDb'], table_name = args['srcTbl'], transformation_ctx = "datasource0"]
    ## @return: datasource0
    ## @inputs: []
    return glueContext.create_dynamic_frame.from_catalog(database = args['srcDb'], table_name = args['srcTbl'], transformation_ctx = "CwMdcrFacAtsttnAudt")

# Description: Filter out any records that are not new
# Inputs: (None)
# Returns: 
# Preconditions: (None)
# Postconditions: 
# - 
def mapColsFromCwToEqrs(MdcrFacAtsttnAudt):
    """ Convert column names and type from CW schema to EQRS schema. """
    ## @type: ApplyMapping
    ## @args: [mapping = [("audt_ts", "timestamp", "audt_time", "timestamp"), ("fac_id", "decimal(38,0)", "org_id", "decimal(38,0)"), ("audt_oprtn_name", "string", "audt_oprtn_txt", "string"), ("audt_id", "decimal(38,0)", "audt_id", "decimal(19,0)"), ("on_bhlf_of_name", "string", "obhlfo_name", "string"), ("audt_user_name", "string", "audt_user_name", "string"), ("creat_by_name", "string", "sys_creat_name", "string"), ("fac_atsttn_msrmnt_id", "decimal(38,0)", "mdcr_fac_atsttn_type_id", "decimal(19,0)"), ("fac_atsttn_id", "decimal(38,0)", "fac_atsttn_id", "decimal(19,0)"), ("updt_by_name", "string", "sys_updt_name", "string"), ("atsttn_cd", "string", "atsttn_cd", "string"), ("na_rsn_txt", "string", "na_rsn_txt", "string"), ("updt_ts", "timestamp", "sys_updt_time", "timestamp"), ("creat_ts", "timestamp", "sys_creat_time", "timestamp"), ("yr_cd", "decimal(38,0)", "prfmnc_yr_num", "decimal(4,0)")], transformation_ctx = "applymapping1"]
    ## @return: applymapping1
    ## @inputs: [frame = datasource0]
    return ApplyMapping.apply(frame = MdcrFacAtsttnAudt, mappings = [("audt_ts", "timestamp", "audt_time", "timestamp"), ("fac_id", "decimal(38,0)", "org_id", "decimal(38,0)"), ("audt_oprtn_name", "string", "audt_oprtn_txt", "string"), ("audt_id", "decimal(38,0)", "audt_id", "decimal(19,0)"), ("on_bhlf_of_name", "string", "obhlfo_name", "string"), ("audt_user_name", "string", "audt_user_name", "string"), ("creat_by_name", "string", "sys_creat_name", "string"), ("fac_atsttn_msrmnt_id", "decimal(38,0)", "mdcr_fac_atsttn_type_id", "decimal(19,0)"), ("fac_atsttn_id", "decimal(38,0)", "fac_atsttn_id", "decimal(19,0)"), ("updt_by_name", "string", "sys_updt_name", "string"), ("atsttn_cd", "string", "atsttn_cd", "string"), ("na_rsn_txt", "string", "na_rsn_txt", "string"), ("updt_ts", "timestamp", "sys_updt_time", "timestamp"), ("creat_ts", "timestamp", "sys_creat_time", "timestamp"), ("yr_cd", "decimal(38,0)", "prfmnc_yr_num", "decimal(4,0)")], transformation_ctx = "applymapping1")

# Description: Filter out any records that are not new
# Inputs: (None)
# Returns: 
# Preconditions: (None)
# Postconditions: 
# - 
def ensureRequiredFieldsSelected(MdcrFacAtsttnAudt):
    """ Convert from DataFrame back to DynamicFrame, and ensure the required fields are Present. """
    ## @type: SelectFields
    ## @args: [paths = ["atstr_last_name", "audt_time", "sys_updt_time", "mdcr_fac_atsttn_type_id", "sys_creat_time", "sys_updt_name", "audt_id", "obhlfo_name", "prfmnc_yr_num", "audt_user_name", "atstr_1st_name", "audt_oprtn_txt", "fac_atsttn_id", "org_id", "atsttn_cd", "na_rsn_txt", "sys_creat_name"], transformation_ctx = "selectfields2"]
    ## @return: selectfields2
    ## @inputs: [frame = applymapping1]
    return SelectFields.apply(frame = MdcrFacAtsttnAudt, paths = ["atstr_last_name", "audt_time", "sys_updt_time", "mdcr_fac_atsttn_type_id", "sys_creat_time", "sys_updt_name", "audt_id", "obhlfo_name", "prfmnc_yr_num", "audt_user_name", "atstr_1st_name", "audt_oprtn_txt", "fac_atsttn_id", "org_id", "atsttn_cd", "na_rsn_txt", "sys_creat_name"], transformation_ctx = "selectfields2")

# Description: Filter out any records that are not new
# Inputs: (None)
# Returns: 
# Preconditions: (None)
# Postconditions: 
# - 
def matchTargetCatalog(args, MdcrFacAtsttnAudt):
    """ Ensure that the data types for the columns match the target schema. """
    ## @type: ResolveChoice
    ## @args: [choice = "MATCH_CATALOG", database = args['trgtDb'], table_name = args['trgtTbl'], transformation_ctx = "resolvechoice3"]
    ## @return: resolvechoice3
    ## @inputs: [frame = MdcrFacAtsttnAudt]
    return ResolveChoice.apply(frame = MdcrFacAtsttnAudt, choice = "MATCH_CATALOG", database = args['trgtDb'], table_name = args['trgtTbl'], transformation_ctx = "resolvechoice3")

# Description: Filter out any records that are not new
# Inputs: (None)
# Returns: 
# Preconditions: (None)
# Postconditions: 
# - 
def writeEqrsMdcrFacAtsttn(args, MdcrFacAtsttnAudt):
    """ Write Data to the Target Data Source. """
    ## @type: DataSink
    ## @args: [database = args['trgtDb'], table_name = args['trgtTbl'], transformation_ctx = "datasink5"]
    ## @return: datasink5
    ## @inputs: [frame = resolvechoice4]
    datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = MdcrFacAtsttnAudt, database = args['trgtDb'], table_name = args['trgtTbl'], transformation_ctx = "datasink5")
    job.commit()

def main():
    MdcrFacAtsttnAudt = getCwMdcrFacAtsttnAudt()
    MdcrFacAtsttnAudtEqrs = mapColsFromCwToEqrs(MdcrFacAtsttnAudt)
    RequiredMdcrFacAtsttnAudt = ensureRequiredFieldsSelected(MdcrFacAtsttnAudtEqrs)
    MatchedMdcrFacAtsttnAudt = matchTargetCatalog(args, RequiredMdcrFacAtsttnAudt)
    writeEqrsMdcrFacAtsttn(args, MatchedMdcrFacAtsttnAudt)

if __name__ == "__main__":
    main()