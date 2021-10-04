import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, 
            ['JOB_NAME',
             'srcDb',
             'srcTbl',
             'srcTbl2',
             'trgtDb',
             'trgtTbl'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Description: Get CW MDCR_FAC_ATSTTN table
# Inputs: (None)
# Returns: DynamicFrame - Contains data from CW MDCR_FAC_ATSTTN table
# Preconditions: (None)
# Postconditions: 
# - New DynamicFrame returned with contents of CW MDCR_FAC_ATSTTN table
# - Columns included in returned DynamicFrame:
#   - FAC_ATSTTN_ID: decimal, FAC_ID: decimal, YR_CD: decimal, ATSTTN_CD: string, NA_RSN_TXT: string, ON_BHLF_OF_NAME: string
#     CREAT_BY_NAME: string, CREAT_TS: timestamp, UPDT_BY_NAME: string, UPDT_TS: timestamp, FAC_ATSTTN_MSRMNT_ID: decimal
def getCwMdcrFacAtsttn(srcDb, srcTbl):
    """ Get CW MDCR_FAC_ATSTTN table. """
    return glueContext.create_dynamic_frame.from_catalog(database = srcDb, table_name = srcTbl, transformation_ctx = "CwMdcrFacAtsttn")

# Description: Get CW CROWN_USERS and drop columns not needed
# Inputs: (None)
# Returns: DataFrame - Contains data from CW MDCR_FAC_ATSTTN table
# Preconditions: (None)
# Postconditions: 
# - New DataFrame returned with contents of CW MDCR_FAC_ATSTTN table
# - Columns included in returned DynamicFrame:
#   - USER_NAME: string, FIRST_NAME: string, LAST_NAME: string
def getCwCrownUsers(srcDb, srcTbl2):
    """ Get CW CROWN_USERS and drop columns not needed. """
    crownUsers = glueContext.create_dynamic_frame.from_catalog(database = srcDb, table_name = srcTbl2, transformation_ctx = "crownUsers")
    return SelectFields.apply(frame = crownUsers, paths = ["USER_NAME", "FIRST_NAME", "LAST_NAME"], transformation_ctx = "context").toDF()


# Description: Convert column names and type from CW schema to EQRS schema
# Inputs: mdcrFacAtsttn - DynamicFrame produced from getCwMdcrFacAtsttn()
# Returns: DynamicFrame
# Preconditions: 
# - mdcrFacAtsttn should contain all records from getCwMdcrFacAtsttn()
# - Columns required from mdcrFacAtsttn:
#   - FAC_ATSTTN_ID: decimal, FAC_ID: decimal, YR_CD: decimal, ATSTTN_CD: string, NA_RSN_TXT: string, ON_BHLF_OF_NAME: string
#     CREAT_BY_NAME: string, CREAT_TS: timestamp, UPDT_BY_NAME: string, UPDT_TS: timestamp, FAC_ATSTTN_MSRMNT_ID: decimal
# Postconditions: 
# - New DynamicFrame returned with column names and types mapped what is required for target table
# - Columns included in returned DynamicFrame:
#   - atsttn_cd: string, sys_creat_time: timestamp, sys_creat_name: string, org_id: decimal, sys_updt_time: timestamp, mdcr_fac_atsttn_type_id: decimal
#     fac_atsttn_id: decimal, prfmnc_yr_num: decimal, na_rsn_txt: string, obhlfo_name: string, sys_updt_name: string
def mapColsFromCwToEqrs(mdcrFacAtsttn):
    """ Convert column names and type from CW schema to EQRS schema. """
    return ApplyMapping.apply(frame = mdcrFacAtsttn, mappings = [("atsttn_cd", "string", "atsttn_cd", "string"), ("creat_ts", "timestamp", "sys_creat_time", "timestamp"), ("creat_by_name", "string", "sys_creat_name", "string"), ("fac_id", "decimal(38,0)", "org_id", "decimal(38,0)"), ("updt_ts", "timestamp", "sys_updt_time", "timestamp"), ("fac_atsttn_msrmnt_id", "decimal(19,0)", "mdcr_fac_atsttn_type_id", "decimal(19,0)"), ("fac_atsttn_id", "decimal(19,0)", "fac_atsttn_id", "decimal(19,0)"), ("yr_cd", "decimal(4,0)", "prfmnc_yr_num", "decimal(4,0)"), ("na_rsn_txt", "string", "na_rsn_txt", "string"), ("on_bhlf_of_name", "string", "obhlfo_name", "string"), ("updt_by_name", "string", "sys_updt_name", "string")], transformation_ctx = "applymapping1")


# Description: Filter out any records that are not new
# Inputs:
# - MdcrFacAtsttn - DynamicFrame produced from filterToNewAttestations()
# - crownDF - DataFrame produced from getCwCrownUsers()
# Returns: DataFrame - Contains only records that are new
# Preconditions: (None)
# Postconditions: 
# - New DataFrame returned filter to only new records, joined with CROWN_USERS data
# - Columns included in returned DataFrame:
#   - atsttn_cd: string, sys_creat_time: timestamp, sys_creat_name: string, org_id: decimal(38,0), sys_updt_time: timestamp
#     mdcr_fac_atsttn_type_id: decimal(19,0), fac_atsttn_id: decimal(19,0), prfmnc_yr_num: decimal(4,0), na_rsn_txt: string, obhlfo_name: string
#     sys_updt_name: string, USER_NAME: string, FIRST_NAME: string, LAST_NAME: string
def filterToNewAttestations(MdcrFacAtsttn, crownDF):
    """ Filter out any records that are not new. """
    new = MdcrFacAtsttn.toDF().filter("sys_updt_name is null and sys_creat_name is not null")
    return new.join(crownDF, new['sys_creat_name'] == crownDF['USER_NAME'],how='left')


# Description: Filter out any records that are not updated
# Inputs:
# - MdcrFacAtsttn - DynamicFrame produced from filterToUpdatedAttestations()
# - crownDF - DataFrame produced from getCwCrownUsers()
# Returns: DataFrame - Contains only records that are updated
# Preconditions: (None)
# Postconditions: 
# - New DataFrame returned filter to only updated records, joined with CROWN_USERS data
# - Columns included in returned DataFrame:
#   - atsttn_cd: string, sys_creat_time: timestamp, sys_creat_name: string, org_id: decimal(38,0), sys_updt_time: timestamp, mdcr_fac_atsttn_type_id: decimal(19,0)
#     fac_atsttn_id: decimal(19,0), prfmnc_yr_num: decimal(4,0), na_rsn_txt: string, obhlfo_name: string, sys_updt_name: string, USER_NAME: string
#     FIRST_NAME: string, LAST_NAME: string
def filterToUpdatedAttestations(MdcrFacAtsttn, crownDF):
    """ Filter out any records that have not been updated. """
    updated = MdcrFacAtsttn.toDF().filter("sys_updt_name is not null")
    return updated.join(crownDF, updated['sys_updt_name'] == crownDF['USER_NAME'],how='left')

# Description: Union DF of new attestations with DF of updated attestations
# Inputs:
# - newAttestations - DataFrame produced from filterToNewAttestations()
# - updatedAttestations - DataFrame produced from filterToUpdatedAttestations()
# Returns: DataFrame - Contains all records (both new and updated) with CROWN_USERS data
# Preconditions: (None)
# Postconditions:
# - New DataFrame returned joined with CROWN_USERS data
# - Columns included in returned DataFrame:
#   - atsttn_cd: string, sys_creat_time: timestamp, sys_creat_name: string, org_id: decimal(38,0), sys_updt_time: timestamp, mdcr_fac_atsttn_type_id: decimal(19,0)
#     fac_atsttn_id: decimal(19,0), prfmnc_yr_num: decimal(4,0), na_rsn_txt: string, obhlfo_name: string, sys_updt_name: string, USER_NAME: string
#     FIRST_NAME: string, LAST_NAME: string
def unionAttestations(newAttestations, updatedAttestations):
    """ Union DF of new attestations with DF of updated attestations. """
    return updatedAttestations.unionByName(newAttestations)

# Description: Rename CROWN_USERS columns to EQRS MDCR_FAC_ATSTTN names, and drop USER_NAME column
# Inputs: unionAttestation - DataFrame provided from unionAttestations()
# Returns: DataFrame - 
# Preconditions: (None)
# Postconditions: 
# - New DataFrame returned joined with CROWN_USERS data
# - Columns included in returned DataFrame:
#   - atsttn_cd: string, sys_creat_time: timestamp, sys_creat_name: string, org_id: decimal(38,0), sys_updt_time: timestamp, mdcr_fac_atsttn_type_id: decimal(19,0)
#     fac_atsttn_id: decimal(19,0), prfmnc_yr_num: decimal(4,0), na_rsn_txt: string, obhlfo_name: string, sys_updt_name: string
#     atstr_1st_name: string, atstr_last_name: string
def cleanCrownUsersColumns(unionAttestation):
    """ Rename CROWN_USERS columns to EQRS MDCR_FAC_ATSTTN names, and drop USER_NAME column. """
    return unionAttestation.withColumnRenamed("FIRST_NAME","atstr_1st_name").withColumnRenamed("LAST_NAME","atstr_last_name").drop("USER_NAME")

# Description: 
# Inputs: (None)
# Returns: 
# Preconditions: (None)
# Postconditions: 
# - 
def ensureRequiredFieldsSelected(MdcrFacAtsttnDF):
    """ Convert from DataFrame back to DynamicFrame, and ensure the required fields are Present. """
    MdcrFacAtsttn = DynamicFrame.fromDF(MdcrFacAtsttnDF, glueContext, "MdcrFacAtsttnDF")
    return SelectFields.apply(frame = MdcrFacAtsttn, paths = ["atstr_last_name", "sys_updt_time", "mdcr_fac_atsttn_type_id", "sys_creat_time", "sys_updt_name", "obhlfo_name", "prfmnc_yr_num", "atstr_1st_name", "fac_atsttn_id", "org_id", "atsttn_cd", "na_rsn_txt", "sys_creat_name"], transformation_ctx = "selectfields")

# Description: 
# Inputs: (None)
# Returns: 
# Preconditions: (None)
# Postconditions: 
# - 
def matchTargetCatalog(MdcrFacAtsttn, trgtDb, trgtTbl):
    """ Ensure that the data types for the columns match the target schema. """
    return ResolveChoice.apply(frame = MdcrFacAtsttn, choice = "MATCH_CATALOG", database = trgtDb, table_name = trgtTbl, transformation_ctx = "resolvechoice")

# Description: 
# Inputs: (None)
# Returns: 
# Preconditions: (None)
# Postconditions: 
# - 
def writeEqrsMdcrFacAtsttn(MdcrFacAtsttn, trgtDb, trgtTbl):
    """ Write Data to the Target Data Source. """
    glueContext.write_dynamic_frame.from_catalog(frame = MdcrFacAtsttn, database = trgtDb, table_name = trgtTbl, transformation_ctx = "datasink5")
    job.commit()

def main():
    MdcrFacAtsttn = getCwMdcrFacAtsttn(args['srcDb'], args['srcTbl'])
    CrownUsers = getCwCrownUsers(args['srcDb'], args['srcTbl2'])
    MdcrFacAtsttnEqrs = mapColsFromCwToEqrs(MdcrFacAtsttn)
    NewMdcrFacAtsttn = filterToNewAttestations(MdcrFacAtsttnEqrs, CrownUsers)
    UpdatedMdcrFacAtsttn = filterToUpdatedAttestations(MdcrFacAtsttnEqrs, CrownUsers)
    UnionMdcrFacAtsttn = unionAttestations(NewMdcrFacAtsttn, UpdatedMdcrFacAtsttn)
    CleanMdcrFacAtsttn = cleanCrownUsersColumns(UnionMdcrFacAtsttn)
    RequiredMdcrFacAtsttn = ensureRequiredFieldsSelected(CleanMdcrFacAtsttn)
    MatchedMdcrFacAtsttn = matchTargetCatalog(RequiredMdcrFacAtsttn, args['trgtDb'], args['trgtTbl'])
    writeEqrsMdcrFacAtsttn(MatchedMdcrFacAtsttn, args['trgtDb'], args['trgtTbl'])

if __name__ == "__main__":
    main()