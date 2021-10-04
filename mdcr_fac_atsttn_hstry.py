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
def getCwMdcrFacAtsttnHstry():
    """ Get CW MDCR_FAC_ATSTTN_HSTRY table """
    ## @type: DataSource
    ## @args: [database = args['srcDb'], table_name = args['srcTbl'], transformation_ctx = "datasource0"]
    ## @return: datasource0
    ## @inputs: []
    return glueContext.create_dynamic_frame.from_catalog(database = args['srcDb'], table_name = args['srcTbl'], transformation_ctx = "datasource0")

# Description: Filter out any records that are not new
# Inputs: (None)
# Returns: 
# Preconditions: (None)
# Postconditions: 
# - 
def mapColsFromCwToEqrs(mdcrFacAtsttnHstry):
    """ Convert column names and type from CW schema to EQRS schema """
    ## @type: ApplyMapping
    ## @args: [mapping = [("fac_id", "decimal(38,0)", "org_id", "decimal(38,0)"), ("hstry_user_name", "string", "hstry_user_name", "string"), ("new_rprsntv_name", "string", "new_obhlfo_name", "string"), ("hstry_oprtn_name", "string", "hstry_oprtn_name", "string"), ("new_atsttn_cd", "string", "new_atsttn_cd", "string"), ("creat_by_name", "string", "sys_creat_name", "string"), ("updt_by_name", "string", "sys_updt_name", "string"), ("fac_atsttn_msrmnt_id", "decimal(38,0)", "mdcr_fac_atsttn_type_id", "decimal(19,0)"), ("fac_atsttn_id", "decimal(38,0)", "fac_atsttn_id", "decimal(19,0)"), ("creat_ts", "timestamp", "sys_creat_time", "timestamp"), ("mdcr_fac_atsttn_hstry_id", "decimal(38,0)", "mdcr_fac_atsttn_hstry_id", "decimal(19,0)"), ("hstry_ts", "timestamp", "hstry_ts", "timestamp"), ("old_atsttn_cd", "string", "old_atsttn_cd", "string"), ("updt_ts", "timestamp", "sys_updt_time", "timestamp"), ("old_rprsntv_name", "string", "old_obhlfo_name", "string"), ("not_aplcbl_rsn_txt", "string", "na_rsn_txt", "string"), ("yr_cd", "decimal(38,0)", "prfmnc_yr_num", "decimal(4,0)")], transformation_ctx = "applymapping1"]
    ## @return: applymapping1
    ## @inputs: [frame = mdcrFacAtsttnHstry]
    return ApplyMapping.apply(frame = mdcrFacAtsttnHstry, mappings = [("fac_id", "decimal(38,0)", "org_id", "decimal(38,0)"), ("hstry_user_name", "string", "hstry_user_name", "string"), ("new_rprsntv_name", "string", "new_obhlfo_name", "string"), ("hstry_oprtn_name", "string", "hstry_oprtn_name", "string"), ("new_atsttn_cd", "string", "new_atsttn_cd", "string"), ("creat_by_name", "string", "sys_creat_name", "string"), ("updt_by_name", "string", "sys_updt_name", "string"), ("fac_atsttn_msrmnt_id", "decimal(38,0)", "mdcr_fac_atsttn_type_id", "decimal(19,0)"), ("fac_atsttn_id", "decimal(38,0)", "fac_atsttn_id", "decimal(19,0)"), ("creat_ts", "timestamp", "sys_creat_time", "timestamp"), ("mdcr_fac_atsttn_hstry_id", "decimal(38,0)", "mdcr_fac_atsttn_hstry_id", "decimal(19,0)"), ("hstry_ts", "timestamp", "hstry_ts", "timestamp"), ("old_atsttn_cd", "string", "old_atsttn_cd", "string"), ("updt_ts", "timestamp", "sys_updt_time", "timestamp"), ("old_rprsntv_name", "string", "old_obhlfo_name", "string"), ("not_aplcbl_rsn_txt", "string", "na_rsn_txt", "string"), ("yr_cd", "decimal(38,0)", "prfmnc_yr_num", "decimal(4,0)")], transformation_ctx = "applymapping1")

# Description: Filter out any records that are not new
# Inputs: (None)
# Returns: 
# Preconditions: (None)
# Postconditions: 
# - 
def ensureRequiredFieldsSelected(mdcrFacAtsttnHstry):
    """ Convert from DataFrame back to DynamicFrame, and ensure the required fields are Present. """
    ## @type: SelectFields
    ## @args: [paths = ["hstry_user_name", "sys_updt_time", "mdcr_fac_atsttn_type_id", "sys_creat_time", "old_obhlfo_name", "sys_updt_name", "new_obhlfo_name", "hstry_oprtn_name", "prfmnc_yr_num", "new_atsttn_cd", "fac_atsttn_id", "org_id", "mdcr_fac_atsttn_hstry_id", "hstry_ts", "old_atsttn_cd", "na_rsn_txt", "sys_creat_name"], transformation_ctx = "selectfields2"]
    ## @return: selectfields2
    ## @inputs: [frame = mdcrFacAtsttnHstry]
    return SelectFields.apply(frame = mdcrFacAtsttnHstry, paths = ["hstry_user_name", "sys_updt_time", "mdcr_fac_atsttn_type_id", "sys_creat_time", "old_obhlfo_name", "sys_updt_name", "new_obhlfo_name", "hstry_oprtn_name", "prfmnc_yr_num", "new_atsttn_cd", "fac_atsttn_id", "org_id", "mdcr_fac_atsttn_hstry_id", "hstry_ts", "old_atsttn_cd", "na_rsn_txt", "sys_creat_name"], transformation_ctx = "selectfields2")

# Description: Filter out any records that are not new
# Inputs: (None)
# Returns: 
# Preconditions: (None)
# Postconditions: 
# - 
def matchTargetCatalog(args, mdcrFacAtsttnHstry):
    """ Ensure that the data types for the columns match the target schema. """
    ## @type: ResolveChoice
    ## @args: [choice = "MATCH_CATALOG", database = args['trgtDb'], table_name = args['trgtTbl'], transformation_ctx = "resolvechoice3"]
    ## @return: resolvechoice3
    ## @inputs: [frame = selectfields2]
    return ResolveChoice.apply(frame = mdcrFacAtsttnHstry, choice = "MATCH_CATALOG", database = args['trgtDb'], table_name = args['trgtTbl'], transformation_ctx = "resolvechoice3")

# Description: Filter out any records that are not new
# Inputs: (None)
# Returns: 
# Preconditions: (None)
# Postconditions: 
# - 
def writeEqrsMdcrFacAtsttn(args, mdcrFacAtsttnHstry):
    """ Write Data to the Target Data Source. """
    ## @type: DataSink
    ## @args: [database = args['trgtDb'], table_name = args['trgtTbl'], transformation_ctx = "datasink5"]
    ## @return: datasink5
    ## @inputs: [frame = resolvechoice4]
    datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = mdcrFacAtsttnHstry, database = args['trgtDb'], table_name = args['trgtTbl'], transformation_ctx = "datasink5")
    job.commit()

def main():
    MdcrFacAtsttnHstry = getCwMdcrFacAtsttnHstry()
    MdcrFacAtsttnHstryEqrs = mapColsFromCwToEqrs(MdcrFacAtsttnHstry)
    RequiredMdcrFacAtsttnHstry = ensureRequiredFieldsSelected(MdcrFacAtsttnHstryEqrs)
    MatchedMdcrFacAtsttnHstry = matchTargetCatalog(args, RequiredMdcrFacAtsttnHstry)
    writeEqrsMdcrFacAtsttn(args, MatchedMdcrFacAtsttnHstry)

if __name__ == "__main__":
    main()