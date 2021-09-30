'''
--target_db EQRS
--reference_dir s3://ado1-eqrs-prodpreview2-glue-data-storage/prodpreview2-1/clinical/mappings/csv_maps
--mapping_dir s3://ado1-eqrs-prodpreview2-glue-data-storage/prodpreview2-1/clinical/mappings
--db_resolver_path DUMMY
--name ptnt_clncl
--env prodpreview2-1
--target_table ptnt_clncl
'''
import sys
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict, Counter, deque

import logging

import pandas as pd

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark import SQLContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession


DataFrame = pd.DataFrame

sc = SparkContext()


class JobSpec:
    def __init__(
        self,
        name: str,
        env: str,
        target_db: str,
        target_table: str,
        mapping_dir: str,
        reference_dir: str,
        db_resolver_path: str,
    ):
        # the job name, needed by Spark/Glue
        self.name = name

        # the environment in which the job will run (e.g., production, sandbox)
        # used to resolve database names from aliases
        self.env = env

        # the database being written to
        self.target_db = target_db

        # the table being written to in the target database
        self.target_table = target_table

        # Path to the directory containing the mapping document for the target table
        self.mapping_dir = mapping_dir

        # Path to the directory containing reference CSVs
        self.reference_dir = reference_dir

        # Path to the file containing mappings from aliases to database and table names
        self.db_resolver_path = db_resolver_path
        
    @property
    def mapping_document_path(self) -> str:
        return f"{self.mapping_dir}/{self.target_table}.csv"

    def get_reference_document_path(self, reference_table: str) -> str:
        return f"{self.reference_dir}/{reference_table}.csv"

class TableSpec(tuple):
    def __new__(cls, db_alias: str, table_alias: str):
        return super().__new__(cls, (db_alias, table_alias))

    @property
    def db_alias(self):
        return self[0]

    @property
    def table_alias(self):
        return self[1]
    
    @classmethod
    def from_dotted(cls, dotted_string):
        fields = dotted_string.split(".")
        try:
            db_alias, table_alias = fields
            return TableSpec(db_alias, table_alias)

        except ValueError:
            raise ValueError(f'dotted_string must be of the form "db_alias.table_alias"')
    
    def __str__(self) -> str:
        return f"{self[0]}.{self[1]}"


TableSpecPair = Tuple[TableSpec, TableSpec]


class DebugTableResolver:
    """
    Terminology:
        `name` refers to 'ado1-eqrs-sbx2-1-data-migration-database'
        `alias_db` refers to 'CW'
        `alias_table` refers to 'PATIENT'
    """

    def __init__(self, env: str):
        self.env = env
        self._from_alias = {}
        self._to_alias = {}
        self.generate_maps()

    @staticmethod
    def get_placeholder_data():
        """In practice, this would likely be supplied from an external resource"""
        table_data = [
            (
                'CW',
                'PTNT_CLNCL',      
                'prodpreview2-1',
                'ado1-eqrs-prodpreview2-1-data-migration-database',
                'prodpreview2-1_eqrs_crown_ptnt_clncl_poc',
                
            ),
            (
                'EQRS',
                'PTNT_CLNCL',      
                'prodpreview2-1',
                'ado1-eqrs-prodpreview2-1-data-migration-database',
                'prodpreview2-1-clinical-eqrs_ptnt_clncl_data_ptnt_clncl',
            ),
        ]
        
        return table_data

    def generate_maps(self):
        for alias_db, alias_table, env, db_name, table_name in self.get_placeholder_data():
            self._from_alias[env, alias_db, alias_table] = db_name, table_name
            self._to_alias[db_name, table_name] = alias_db, alias_table
            self._to_alias[env, db_name, table_name] = alias_db, alias_table
        
    def to_alias(self, db_name: str, table_name: str) -> str:
        return self._to_alias[db_name, table_name]

    def from_alias(self, alias_db: str, alias_table: str) -> TableSpec:
        return self._from_alias[self.env, alias_db, alias_table]


TableResolver = DebugTableResolver


class GlueTableDef:
    TYPE_GLUE_CATALOG = 1
    TYPE_PARQUET = 2
    
    def __init__(self, table_type: int):
        self._type = table_type
    
    @property
    def type(self):
        return self._type


class ParquetGlueTableDef(GlueTableDef):
    def __init__(self, path: str):
        super().__init__(table_type=GlueTableDef.TYPE_PARQUET)
        self.path = path
        
    def __repr__(self) -> str:
        return f"<ParquetGlueTableDef:{self.path}>"

    
class DatabaseGlueTableDef(GlueTableDef):
    def __init__(self, db_name: str, table_name: str):
        super().__init__(table_type=GlueTableDef.TYPE_GLUE_CATALOG)
        self.db_name = db_name
        self.table_name = table_name
        
    @property
    def table_spec(self) -> TableSpec:
        return self.db_name, self.table_name

    def __repr__(self) -> str:
        return f"<DatabaseGlueTableDef:{self.db_name}.{self.table_name}>"
        
        

class GlueTable:
    """Handle Glue-specific operations"""
    
    FORMAT_PARQUET = "parquet"
    TRANSFORMATION_CTX = "src_dyf_ctx"

    def __init__(self, table_def: GlueTableDef, gc: GlueContext, resolver: TableResolver):
        self.table_def = table_def
        self.resolver = resolver
        self.gc = gc

    @staticmethod
    def translate_alias(*alias_names):
        """Take in alias names and replace '.' with '__'
        Also, join them with '__'
        Ex 1: a.b -> a__b
        Ex 2: a, b, c -> a__b__c
        """
        return '__'.join(alias_name.replace('.', '__') for alias_name in alias_names)

    def _alias_if(self, column: str) -> str:
        """Alias column name for sources as GLUEDB_COLUMN
        :param column: Column to be aliased
        :return: Aliased column name as GLUEDB_COLUMN
        """
        table_def = self.table_def
        
        if table_def.type == GlueTableDef.TYPE_GLUE_CATALOG:
            alias_db_name, alias_table_name = self.resolver.to_alias(*table_def.table_spec)
            return GlueTable.translate_alias(alias_db_name, alias_table_name, column)

        elif table_def.type == GlueTableDef.TYPE_PARQUET:
            print("_alias_if parquet column: {column}")
            return column

        else:
            raise ValueError(f"_alias_if: Unrecognized GlueTableDef: {table_def.type}")

    def get_dyf(self) -> DynamicFrame:
        """Read data source as Glue DynamicFrame based on Glue Catalog"""
        table_def = self.table_def
        
        if table_def.type == GlueTableDef.TYPE_GLUE_CATALOG:
            print(f"table_def: {table_def.table_spec}")

            dyf = self.gc.create_dynamic_frame_from_catalog(
                table_def.db_name,
                table_def.table_name,
                transformation_ctx=self.TRANSFORMATION_CTX,
            )
        elif table_def.type == GlueTableDef.TYPE_PARQUET:
            dyf = self.gc.create_dynamic_frame_from_options(
                connection_type="s3",
                format=FORMAT_PARQUET,
                format_options={
                    "path": self.parquet_location,
                },
                transformation_ctx=self.TRANSFORMATION_CTX,
            )
        else:
            raise ValueError(f"get_dyf: Unrecognized GlueTableDef: {table_def.type}")
            
        return dyf
    
    def get_dyf_and_apply_mapping(self) -> DynamicFrame:
        """Get dynamicframe. Use apply mapping to alias column names for source data as GLUEDB_COLUMN"""
        dyf = self.get_dyf()

        # prepare parameters for upcoming ApplyMappings call
        apply_mapping_tuples = [
            (column, col_type, self._alias_if(column), col_type) for column, col_type in dyf.toDF().dtypes
        ]
        return dyf.apply_mapping(apply_mapping_tuples)

    def get_columns(self) -> List[str]:
        """Obtain DynamicFrame columns as list"""
        return self.get_dyf().toDF().columns
    
    def get_columns_after_apply_mapping(self) -> List[str]:
        """Obtain DynamicFrame columns after applying mapping"""
        return self.get_dyf_and_apply_mapping().toDF().columns

class JoinKey(tuple):
    def __new__(cls, db_alias: str, table_alias: str, column_name: str):
        return tuple.__new__(cls, (db_alias, table_alias, column_name))

    @property
    def db_alias(self) -> str:
        return self[0]
    
    @property
    def table_alias(self) -> str:
        return self[1]

    @property
    def column_name(self) -> str:
        return self[2]
    
    @property
    def table_spec(self) -> TableSpec:
        return self.db_alias, self.table_alias

    @classmethod
    def from_dotted(cls, dotted_string: str):
        fields = dotted_string.split(".")
        try:
            db_alias, table_alias, column_name = fields
            return JoinKey(db_alias, table_alias, column_name)

        except ValueError:
            raise ValueError(f'dotted_string must be of the form "db_alias.table_alias.column_name"')
            
    def __str__(self):
        return f"<{self.db_alias}.{self.table_alias}.{self.column_name}>"


JoinKeyPair = Tuple[JoinKey, JoinKey]

class JoinSpec(tuple):
    TYPE_OUTER = "OUTER"
    TYPE_LEFT = "LEFT"

    def __new__(cls, left: JoinKey, right: JoinKey, join_type: str):
        return super().__new__(cls, (left, right, join_type))
    
    @property
    def left(self) -> JoinKey:
        return self[0]

    @property
    def right(self) -> JoinKey:
        return self[1]

    @property
    def type(self) -> str:
        return self[2]

    def __repr__(self) -> str:
        return "<{left_spec} {join_type} JOIN {right_spec} ON {left} = {right}>".format(
            join_type=self.type,
            left=self.left,
            left_spec=self.left.table_spec,
            right=self.right,
            right_spec=self.right.table_spec,
        )
    
    
class LeftJoinSpec(JoinSpec):
    def __new__(cls, left: JoinKey, right: JoinKey):
        return super().__new__(cls, left, right, JoinSpec.TYPE_LEFT)

    
class OuterJoinSpec(JoinSpec):
    def __new__(cls, left: JoinKey, right: JoinKey):
        return super().__new__(cls, left, right, JoinSpec.TYPE_OUTER)
    



class JoinMap:
    """Maintains mapping keys for tables to be joined"""
    
    def __init__(self):
        self._map: Dict[TableSpecPair, JoinSpec] = {}
        self._pairs: List[TableSpecPair] = []

    def add_spec(self, join_spec: JoinSpec):
        left_spec = join_spec.left.table_spec
        right_spec = join_spec.right.table_spec
        
        self._pairs.append((left_spec, right_spec))
        self._map[left_spec, right_spec] = join_spec
        
    def add(self, left: JoinKey, right: JoinKey, join_type: str):
        self.add_spec(JoinSpec(left, right, join_type))
            
    def get(self, table1: TableSpec, table2: TableSpec) -> JoinSpec:
        if (table1, table2) not in self._map:
            table2, table1 = table1, table2
        return self._map[table1, table2]
 
    def __contains__(self, table_spec_pair: TableSpecPair) -> bool:
        table1, table2 = table_spec_pair
        if (table1, table2) in self._map:
            return True
        elif (table2, table1) in self._map:
            return True
        else:
            return False

    @property
    def is_empty(self) -> bool:
        return True if self._map else False            
        
    def get_chain(self) -> List[TableSpec]:
        edges_by_node: DefaultDict[TableSpec, Set[TableSpec]] = defaultdict(set)
        right_side_nodes: DefaultDict[TableSpec, Set[TableSpec]] = defaultdict(set)
            
        for (left_node, right_node), join_spec in self._map.items():
            if join_spec.type == JoinSpec.TYPE_LEFT:
                right_side_nodes[right_node].add(left_node)
            elif join_spec.type == JoinSpec.TYPE_OUTER:
                edges_by_node[right_node].add(left_node)
            edges_by_node[left_node].add(right_node)
            
        nodes = edges_by_node.keys() - right_side_nodes.keys()

        root_nodes = sorted(nodes, key=lambda node: -len(edges_by_node[node]))

        right_node_children = set()
        agenda = deque(right_side_nodes.keys())
        while agenda:
            right_node = agenda.popleft()
            right_node_children.add(right_node)
            for right_child_node in edges_by_node.get(right_node, []):
                if right_child_node in right_node_children:
                    continue
                agenda.append(right_child_node)
            
        left_nodes = []
        for node in root_nodes:
            if node in right_node_children:
                continue
            left_nodes.append(node)
            
        agenda = deque(left_nodes)
        nodes_queued = set(agenda)
        chain = []
        while agenda:
            node = agenda.popleft()
            chain.append(node)
            for adjacent_node in edges_by_node.get(node, []):
                if adjacent_node in nodes_queued:
                    continue
                nodes_queued.add(adjacent_node)               
                agenda.append(adjacent_node)
        
        return chain

do_table_chain_test = True

if do_table_chain_test:
    data = [
        ('FULL', ('1', 'A', 'id'),      ('2', 'B', 'id')),
        ('LEFT', ('1', 'A', 'aaa_id'),  ('3', 'C', 'id')),
        ('LEFT', ('2', 'B', 'bbb_id'),  ('4', 'D', 'id')),
        ('LEFT', ('1', 'A', 'ccc_id'),  ('5', 'E', 'id')),
        ('FULL', ('4', 'D', 'ddd_id'),  ('6', 'F', 'value')),
        ('FULL', ('6', 'F', 'eee_id'),  ('7', 'G', 'id')),
    ]
    
    join_map = JoinMap()
    
    for join_type, ts1, ts2 in data:
        join_map.add(JoinKey(*ts1), JoinKey(*ts2), join_type)
    
    print(" => ".join([str(x) for x in join_map.get_chain()]))

class GlueTransformer(GlueTable):
    """Carry out transformations from source DynamicFrames onto target"""

    def __init__(
        self,
        source_tables: Dict[str, GlueTable],
        target_table: GlueTable,
        join_map: JoinMap,
        resolver: TableResolver,
        gc: GlueContext,
    ):
        """
        :param source_tables: a dictionary of tables whose values will be read to produce target values
        :param target_table: the target table
        """
        self.source_tables = source_tables
        self.target_table = target_table
        self.join_map = join_map

        self.resolver = resolver
        self.gc = gc

    def join_tables(self) -> DataFrame:
        """Join sources based on provided keys"""
        if not self.source_tables:
            raise ValueError(f"No source tables were provided to GlueTransformer: {self}")

        if len(self.source_tables) == 1:
            # extract the first value from the dictionary
            source_table = list(self.source_tables.values())[0]
            return source_table.get_dyf_and_apply_mapping().toDF()
        
        print(self.join_map.get_chain())
        table_chain = self.join_map.get_chain()
        
        print(f"table_chain: {table_chain}")
        
        master_table_name = table_chain[0]
        master_table = self.source_tables[master_table_name]
        #print(dir(master_table))
        print("master_table.get_columns()")
        print(master_table.get_columns())
        master_df = master_table.get_dyf_and_apply_mapping().toDF()
        print(master_df.columns)
        
        absorbed_table_names = [master_table_name]
        
        for next_table_name in table_chain[1:]:
            next_table = self.source_tables[next_table_name]

            '''
            print("next_table.get_columns()")
            print(next_table.get_columns())
            '''
            absorbed_table_names.append(next_table_name)
            
            next_df = next_table.get_dyf_and_apply_mapping().toDF()
            next_table_def = next_table.table_def
            next_table_alias = self.resolver.to_alias(*next_table_def.table_spec)
    
            absorbed_source = None
            master_alias = None
            for absorbed_table_name in absorbed_table_names:
                absorbed_table_def = self.source_tables[absorbed_table_name].table_def
                
                master_alias = self.resolver.to_alias(*absorbed_table_def.table_spec)
                # once we find the relevant JOIN, stop and process it
                if (master_alias, next_table_alias) in self.join_map:
                    break
            else:
                raise ValueError(f'No link found for "{next_alias}"')
                
            join_spec = self.join_map.get(master_alias, next_table_alias)
            # not in base code
            if join_spec.left.table_spec == next_table_alias:
                '''
                print('\n'.join([
                    'A',
                    f'{join_spec.left.table_spec == next_table_alias}',
                    f'{join_spec.left.table_spec}',
                    f'{next_table_alias}',
                ]))
                '''
                next_key, master_key = join_spec.left, join_spec.right
            else:
                '''
                print('\n'.join([
                    'B',
                    f'{join_spec.left.table_spec == next_table_alias}',
                    f'{join_spec.left.table_spec}',
                    f'{next_table_alias}',
                ]))
                '''
                master_key, next_key = join_spec.left, join_spec.right

            '''
            # DEBUG
            print(f"JOINDEBUG1 {join_spec}")
            print(f"{master_key}, {next_key}")
            '''

            join_type = join_spec.type
            
            master_df_key = GlueTable.translate_alias(*master_key)
            next_df_key = GlueTable.translate_alias(*next_key)
            
            master_df_key = GlueTable.translate_alias(*master_key)
            next_df_key = GlueTable.translate_alias(*next_key)
            
            '''            
            # DEBUG
            print(f'Master table name : {master_table_name} with key {master_df_key}')
            # print(f'No. of records before join master : {master_df.count()}')
            print(master_df.columns)
            print(f'Next table name : {next_table_name} with key {next_df_key}')
            # print(f'No. of records before join next : {next_df.count()}')
            print(next_df.columns)
        
            print(f"{master_key}, {next_key}")
        
            '''
            
            try:
                master_df = master_df.join(
                    next_df,
                    master_df[master_df_key] == next_df[next_df_key],
                    join_type,
                )
            except Exception as exc:
                print(exc)
            
            # DEBUG
            # print(f'No. of records after join : {master_df.count()}')
        
        return master_df
    
    def _apply_column_expressions(self, col_expressions: List[str]) -> DynamicFrame:
        """Invokes join_tables() function and applies column transformation which are
        provided as SQL column expressions
        :param col_expressions: List of SQL column expressions
        """
        joined_df = self.join_tables()
        transformed_df = self._get_target_only_columns(joined_df.selectExpr(*col_expressions))

        return DynamicFrame.fromDF(transformed_df, self.gc, "self.target_dyf")

    def _get_target_only_columns(self, df: DataFrame) -> DataFrame:
        """Get only columns that are available in target and joined dataframe"""
        target_table_columns = self.target_table.get_columns()
        
        # if mutation of incoming df is desired, make a deepcopy here
        filtered_df = df
        for column in filtered_df.columns:
            if column not in target_table_columns:
                print(f'dropping unused column "{column}"')
                filtered_df = filtered_df.drop(column)
        
        return filtered_df

    def _resolve_target_dtypes(self, dyf: DynamicFrame) -> DynamicFrame:
        """Retrieve target column datatypes to derive cast logic for transformed columns"""
        resolve_choice_specs = [
            (col, f"cast:{col_type}") for col, col_type in self.target_table.get_dyf().toDF().dtypes
        ]

        return dyf.resolveChoice(resolve_choice_specs)

    def _write_to_glue_catalog(self, dyf: DynamicFrame):
        table_def = self.target_table.table_def
        self.gc.write_dynamic_frame_from_catalog(
            dyf, table_def.db_name, table_def.table_name,
        )

    def get_unique_records(self, dyf: DynamicFrame, key_column: str) -> DynamicFrame:
        df = dyf.toDF().selectExpr(
            '*', 
            f'row_number() over(partition by {key_column} order by {key_column}) as row_selector',
        )
        unique_records_df = df.filter('row_selector = 1').drop('row_selector')
        return DynamicFrame.fromDF(unique_records_df, self.gc, "self.target_dyf")
        
    
    def write_to_glue_catalog(self, col_expressions):
        dyf = self._resolve_target_dtypes(
            self._apply_column_expressions(col_expressions)
        )
        
        return self._write_to_glue_catalog(dyf)        

    def write_to_parquet(self,col_expressions,parquet_location):
        """Write target dynamic frame to S3 location as parquet file"""
        dyf = self._resolve_target_dtypes(
            self._apply_column_expressions(col_expressions)
        )

        self.gc.write_dynamic_frame_from_options(
            dyf,
            connection_type="s3",
            format=GlueTable.FORMAT_PARQUET,
            connection_options={"path": parquet_location},
        )


            

class MergeActionResult:
    def __init__(
        self,
        expression: Optional[str] = None,
        join: Optional[JoinKeyPair] = None,
        table_specs: Optional[Set[str]] = None,
    ):
        self.expression = expression
        self.join = join
        self.table_specs = table_specs


merge_action_map = {}
       
        
def register_rule(cls, **kwargs):
    print(f"registering rule '{cls.name()}'")
    merge_action_map[cls.name()] = cls
    print(f"complete.")
    return cls


class MergeAction:
    def __init__(self, job_spec: JobSpec):
        self.job_spec = job_spec

    def apply(self, params: List[str]) -> MergeActionResult:
        raise NotImplementedError()
    
    @classmethod
    def name(cls):
        raise NotImplementedError()
        

@register_rule
class FromMergeAction(MergeAction):
    """Create `nvl` chain from parameters
    Example:
       if params == [X, Y, Z], then produce the expression
           nvl(X, nvl(Y, Z))
       if params == [X], then produce the expression
           X
    """

    @classmethod
    def name(cls):
        return "FROM"
    
    def apply(self, params: List[str]) -> MergeActionResult:
        table_specs = set()
        
        expression = params[0]
        source = JoinKey.from_dotted(params[0])
        table_specs.add(source.table_spec)

        for param in reversed(params[1:]):
            expression = f"nvl({param}, {expression})"
                        
            source = JoinKey.from_dotted(param)
            table_specs.add(source.table_spec)

        return MergeActionResult(
            expression=expression,
            table_specs=table_specs,
        )
    

@register_rule
class LookupRefMergeAction(MergeAction):
    @classmethod
    def name(cls):
        return "LOOKUPREF"
    
    def apply(self, params: List[str]) -> MergeActionResult:
        dotted_source = params[0]
        reference_table_name = params[1]

        source = JoinKey.from_dotted(dotted_source)
        
        reference_df = spark.read.csv(
            self.job_spec.get_reference_document_path(reference_table_name),
            header='true',
        )
        
        expression = "case "
        for row in reference_df.rdd.collect():
            expression += f"when {dotted_source} == '{row[0]}' then '{row[1]}' " 
        expression += "else 'UNK' end"
        
        table_specs = {source.table_spec}

        return MergeActionResult(
            expression=expression,
            table_specs=table_specs,
        )


@register_rule
class LookupMergeAction(MergeAction):
    @classmethod
    def name(cls):
        return "LOOKUP"
    
    def apply(self, params: List[str]) -> MergeActionResult:
        """Example mapping document row:
        CW.PATIENT.ETHNIC_COUNTRY CW.COUNTRY_TYPE COUNTRY_TYPE_ID NAME
        
        | CW.COUNTRY_TYPE.NAME |
        """
        dotted_source, dotted_ref_table_spec, ref_join_id, ref_value_id = params
        
        source = JoinKey.from_dotted(dotted_source)
        
        ref_table_spec = TableSpec.from_dotted(dotted_ref_table_spec)
        ref = JoinKey(ref_table_spec.db_alias, ref_table_spec.table_alias, ref_join_id)

        expression = GlueTable.translate_alias(
            ref.db_alias, ref.table_alias, ref_value_id,
        )
        
        table_specs = set()
        table_specs.add(source.table_spec)
        table_specs.add(ref_table_spec)
        
        return MergeActionResult(
            expression=expression,
            join=LeftJoinSpec(source, ref),
            table_specs=table_specs,
        )

@register_rule
class DateFilterAction(MergeAction):
    
    @classmethod
    def name(cls):
        return "DATE"
    
    

@register_rule
class JoinMergeAction(MergeAction):
    """Extracts a join directive from the mapping document

    Example:
        params = ["CW.PATIENT.PERSON_ID", "REMIS.PATIENTS.SIMS_ID"]
          db ------^^
          table ------^^^^^^^
          column -------------^^^^^^^^^
        these parameters indicate that tables
            1. CW.PATIENT
            2. REMIS.PATIENTS
        should be joined on
            CW.PATIENT.PERSON_ID = REMIS.PATIENTS.SIMS_ID
    """

    @classmethod
    def name(cls):
        return "JOIN"

    def apply(self, params: List[str]) -> MergeActionResult:
        print(f"processing JOIN: {params}")
        assert len(params) == 2
        
        sources = []
        table_specs = set()
        for param in params:
            source = JoinKey.from_dotted(param)

            sources.append(source)
            table_specs.add(source.table_spec)
            
        print(f"sources:     {[str(s) for s in sources]}")
        print(f"table_specs: {[str(t) for t in table_specs]}")

        return MergeActionResult(
            join=OuterJoinSpec(*sources),
            table_specs=table_specs,
        )

@register_rule
class LeftJoinMergeAction(MergeAction):
    """Extracts a join directive from the mapping document

    Example:
        params = ["CW.PATIENT.PERSON_ID", "REMIS.PATIENTS.SIMS_ID"]
          db ------^^
          table ------^^^^^^^
          column -------------^^^^^^^^^
        these parameters indicate that tables
            1. CW.PATIENT
            2. REMIS.PATIENTS
        should be joined on
            CW.PATIENT.PERSON_ID = REMIS.PATIENTS.SIMS_ID
    """

    @classmethod
    def name(cls):
        return "LEFTJOIN"

    def apply(self, params: List[str]) -> MergeActionResult:
        print(f"processing LEFTJOIN: {params}")
        assert len(params) == 2
        
        sources = []
        table_specs = set()
        for param in params:
            source = JoinKey.from_dotted(param)

            sources.append(source)
            table_specs.add(source.table_spec)
            
        print(f"sources:     {[str(s) for s in sources]}")
        print(f"table_specs: {[str(t) for t in table_specs]}")

        return MergeActionResult(
            join=LeftJoinSpec(*sources),
            table_specs=table_specs,
        )

@register_rule
class CustomMergeAction(MergeAction):
    """Extracts a custom directive from the mapping document
    Format:
                params = ["GENERATE_SEQ", "PARAMETER1", "PARAMETER2", ...]
          function name ---^^^^^^^^^^^^
          parameter1 ----------------------^^^^^^^^^^
          parameter2 ------------------------------------^^^^^^^^^^
    Example:
                params = ["GENERATE_SEQ", "START_VALUE", "CW.PATIENT.PERSON_ID", "REMIS.PATIENTS.SIMS_ID"]
    """

    @classmethod
    def name(cls):
        return "CUSTOM"

    @staticmethod
    def generate_seq(params: List[str]) -> str:
        """
        Custom sequence generator function taking in list as parameter
        param[0] : Starting value for sequence generator
        param[1..n] : Columns which will be used for ordering the window partition
        :return:
        """
        params = list(params)
        start_index = params.pop(0)
        if not params:
            return f"{start_index} + row_number() over(order by 1)"
        expression = f"{start_index} + row_number() over(order by {','.join(params)})"
               
        for param in reversed(params):
            expression = f"nvl({param}, {expression})"

        return expression

    @staticmethod
    def concatenate_columns(params: List[str]) -> str:
        """Custom function to concatenate list of columns provided
        :param params: a list of columns which will be concatenated as string values
        """
        convert_columns_to_string = [f'string({col})' for col in params]

        return f"concat({','.join(convert_columns_to_string)})"

    @staticmethod
    def timestamp(params: List[str]) -> str:
        expression = 'CURRENT_TIMESTAMP'

        for param in reversed(params):
            expression = f"nvl({param}, {expression})"

        return expression
    
    
    @staticmethod
    def text(params: List[str]) -> str:
        params = list(params)
        text_param = params.pop()
        expression = f"'{text_param}'"

        for param in reversed(params):
            expression = f"nvl({param}, {expression})"
        return expression
       
    def apply(self, params: List[str]) -> MergeActionResult:
        """Apply CUSTOM MergeAction
        params[0]: Custom function name defined as a static method in CustomMergeAction
        params[1..n]: Parameters to custom function
        :param params: a list of parameters
        """
        print(f"processing CUSTOM: {params}")
        custom_function_name = params.pop(0).lower()

        try:
            expression = getattr(self, custom_function_name)(params)
        except AttributeError as e:
            raise AttributeError(
                f"Invalid custom function provided {custom_function_name}"
            )

        table_specs = set()
        for param in params:
            try:
                table_specs.add(JoinKey.from_dotted(param).table_spec)
            except:
                print(f"excluding '{param}' from table_specs in {params}")

        return MergeActionResult(expression=expression, table_specs=table_specs,)
    
merge_action_map

class MergeRuleExtractor:
    """Manages extraction of merge rules from mapping document"""
    
    KEY_TARGET_TABLE = "TARGET TABLE"
    KEY_TARGET_COLUMN = "TARGET COLUMN"
    KEY_MERGE_RULE = "MERGE RULE"
    
    def __init__(self, job_spec: JobSpec):
        """Read the mapping file as Pandas DataFrame and
        add a derived column containing target columns in lower case
        :param merge_rule_location: File location where mapping document is available
        """
        self.job_spec = job_spec
        
        self.merge_rules = spark.read.csv(job_spec.mapping_document_path, header='true').toPandas()

        self.join_map = JoinMap()
        
        # defined externally
        self.merge_action_map = merge_action_map
        
    @staticmethod
    def cleanse_merge_rule(merge_rule: str) -> str:
        return merge_rule.strip().upper()
    
    def _decode_merge_rule(self, merge_rule: str) -> Tuple[str, List[str]]:
        merge_rule = MergeRuleExtractor.cleanse_merge_rule(merge_rule)

        # extract the command from the parameters
        # example: "FROM X Y Z" becomes ("FROM", ["X Y Z"])        
        command, param_string = merge_rule.split(" ", 1)
        params = param_string.split(" ")
        return command, params

    def _process_merge_rule(
        self,
        merge_rule: str,
        target_table: str,
        target_column: str,
    ) -> MergeActionResult:
        command, params = self._decode_merge_rule(merge_rule)
       
        try:
            merge_action_cls = self.merge_action_map[command]
            merge_action = merge_action_cls(self.job_spec)
            
        except Exception as exc:
            raise ValueError(f'Unrecognized merge rule: "{merge_rule}": {exc}')

        return merge_action.apply(params)

    @classmethod
    def extract_from_row(cls, row) -> Tuple[str, str, str]:
        merge_rule = row[cls.KEY_MERGE_RULE]
        target_table = row[cls.KEY_TARGET_TABLE]
        target_column = row[cls.KEY_TARGET_COLUMN]
        return merge_rule, target_table, target_column

    def col_expr_builder(self) -> Tuple[List[str], List[TableSpec], List[TableSpec]]:
        """For each column in provided list, retrieve equivalent SQL select expression
        based on merge rule available in mapping document
        :param target_column_names: List of columns for which column expression to be derived
        :return: a list of expressions for extracting values
        """
        error_counter = 0
        target_db_alias = self.job_spec.target_db

        # SQL expressions (one for each target colum) that will form the SELECT clause of Glue job
        expressions = []
        source_table_specs: List[TableSpec] = set()
        target_table_specs: List[TableSpec] = set()
            
        for index, row in self.merge_rules.iterrows():
            merge_rule, target_table_alias, target_column = self.extract_from_row(row)
            
            if target_table_alias:
                target_table_spec = TableSpec(target_db_alias, target_table_alias)
                target_table_specs.add(target_table_spec)
            
            try:
                result = self._process_merge_rule(merge_rule, target_table_alias, target_column)
                
                if result.expression:
                    translated_expression = GlueTable.translate_alias(result.expression)
                    expressions.append(f"{translated_expression} as {target_column.lower()}") 

                if result.join:
                    join_spec = result.join
                    print(f"join_spec: {join_spec}")
                    
                    self.join_map.add_spec(join_spec)
                    
                if result.table_specs:
                    source_table_specs.update(result.table_specs)
                    
            except Exception as exc:
                # DEBUG
                print(row)
                print(exc)
                error_counter += 1

        if error_counter:
            print(f"Errors encountered: {error_counter}")
            
        return (
            expressions,
            source_table_specs,
            target_table_specs,
        )


glue_context = GlueContext(sc)

try:
    if spark:
        print("spark already defined")
    else:
        raise ValueError()
except:
    spark = SparkSession.builder.getOrCreate()
    
job = Job(glue_context)
sql_context = SQLContext(sc)


def test():
    target_db = 'EQRS'
    target_table = 'PTNT_CLNCL'
    env = 'sbx2-1'
    
    mapping_dir = 's3://ado1-eqrs-sbx2-glue-data-storage/sbx2-1/clinical/mappings'
    reference_dir = 's3://ado1-eqrs-sbx2-glue-data-storage/sbx2-1/clinical/mappings/csv_maps'

    return JobSpec(
        name=f'test_{target_db}_{target_table}',
        env=env,
        target_db=target_db,
        target_table=target_table,
        mapping_dir=mapping_dir,
        reference_dir=reference_dir,
        db_resolver_path=None,
    )


def process_glue_args():
    # get Glue job parameters when executed as AWS Glue job
    name = 'name'
    env = 'env'
    target_db = 'target_db'
    target_table = 'target_table'
    mapping_dir = 'mapping_dir'
    reference_dir = 'reference_dir'
    db_resolver_path = 'db_resolver_path'

    args = getResolvedOptions(
        sys.argv,
        [
            name,
            env,
            target_db,
            target_table,
            mapping_dir,
            reference_dir,
            db_resolver_path,
        ],
    )

    return JobSpec(
        name=args[name],
        env=args[env],
        target_db=args[target_db],
        target_table=args[target_table],
        mapping_dir=args[mapping_dir],
        reference_dir=args[reference_dir],
        db_resolver_path=args[db_resolver_path],
    )
    
do_test = False

if do_test:
    job_spec = test()
else:
    job_spec = process_glue_args()

mapping_doc_uri = f"{job_spec.mapping_dir}/{job_spec.target_table}.csv"

print(f"mapping_doc_uri: {mapping_doc_uri}")



# Step 1: Initialize job
job.init(job_spec.name)


# instantiate a TableResolver to map between database and table names
# ex. (database="CW", env="sbx1") -> "ado1-eqrs-sbx2-1-data-migration-database"
resolver = TableResolver(job_spec.env)


# Step 2: Get transformation rules from mapping document
merge_rule_extractor = MergeRuleExtractor(job_spec)


expressions, sources, targets = merge_rule_extractor.col_expr_builder()

# Step 3: Define sources
source_glue_tables = {}
for db_name, table_name in sources:
    table_spec = db_name, table_name
    resolved_db_name, resolved_table_name = resolver.from_alias(db_name, table_name)

    table_def = DatabaseGlueTableDef(
        db_name=resolved_db_name,
        table_name=resolved_table_name,
    )

    source_table = GlueTable(
        table_def=table_def,
        gc=glue_context,
        resolver=resolver,
    )

    if table_spec in source_glue_tables:
        raise ValueError(f'Error building source tables: "{table_spec}" already exists')

    source_glue_tables[table_spec] = source_table


# Step 4: Define target
assert len(targets) == 1
target = list(targets)[0]

print(f"target: {str(target)}")

resolved_target = resolver.from_alias(target.db_alias, target.table_alias)
resolved_target_db_name, resolved_target_table_name = resolved_target

target_table_def = DatabaseGlueTableDef(
    db_name=resolved_target_db_name,
    table_name=resolved_target_table_name,
)

target_glue_table = GlueTable(
    table_def=target_table_def,
    gc=glue_context,
    resolver=resolver,
)


# Step 6: Create GlueTransformer object defining Sources and Targets
glue_transformer = GlueTransformer(
    source_glue_tables,
    target_glue_table,
    merge_rule_extractor.join_map,
    resolver=resolver,
    gc=glue_context,    
)




do_debug_path = True
do_filter_unique = True
unique_id = 'ptnt_id'

if not do_debug_path:
    glue_transformer.write_to_glue_catalog(expressions)
    
    job.commit()




if do_debug_path:
    df = glue_transformer.join_tables()

if do_debug_path:
    dyf = glue_transformer._apply_column_expressions(expressions)

if do_debug_path:
    dyf = glue_transformer._resolve_target_dtypes(dyf)

if do_debug_path:
    glue_transformer._write_to_glue_catalog(dyf)


if do_debug_path:
    job.commit()

