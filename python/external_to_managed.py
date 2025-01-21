# Databricks notebook source
from delta.tables import *
import time
from pyspark.sql.functions import *
from dataclasses import dataclass

@dataclass(frozen=True)
class metaTags:
    name: str
    value: str


@dataclass(frozen=True)
class metaColums:
    name: str
    comment: str
    tags: []


@dataclass(frozen=True)
class metaTable:
    catalog: str
    schema: str
    name: str
    uc_table: str
    owner: str
    comment: str
    partitions: str
    clustering: str
    location: str
    columns: []
    tags: []


class ExternalToManaged:

    def __init__(self, catalog, schema=None, table=None, like=None, owner=None):
        self.catalog = catalog
        self.schema = schema
        self.table = table
        self.owner = owner
        self.like = like
        self.region = dbutils.secrets.get('frontline-force', 'region').lower()
        self.uc_table = f"{self.catalog}.{self.schema}.{self.table}"
        self.uc_column = spark.read.table(f"{self.catalog}.information_schema.columns")
        self.uc_table_tags= spark.read.table(f"{self.catalog}.information_schema.table_tags")
        self.uc_column_tags= spark.read.table(f"{self.catalog}.information_schema.column_tags")
        self.information_tables = self.__information_tables()



    def __information_tables(self):
        df = spark.read.table(f"{self.catalog}.information_schema.tables").filter("table_type = 'EXTERNAL'")

        if self.schema is None:
            pass
        else:
            df = df.filter(f"table_schema = '{self.schema}'")

        if self.table is None:
            pass
            if  self.like is None:
                pass
            else:
                df = df.filter(f"table_name like '%{self.like}%'")
        else:
            df = df.filter(f"table_name = '{self.table}'")

        return df


    def __load_meta_data(self, table):
        arr = spark.sql(f"DESCRIBE DETAIL {table}") \
                .select("location", "partitionColumns", "clusteringColumns") \
                .withColumn('partitionColumns', concat_ws(',', 'partitionColumns')) \
                .withColumn('clusteringColumns', concat_ws(',', 'clusteringColumns')) \
                .collect()[0]
        return arr[0], arr[1], arr[2]


    def __get_table_tags(self, catalog, schema, table):
        tags = self.uc_table_tags.filter(f"catalog_name = '{catalog}' and schema_name = '{schema}' and table_name = '{table}'") \
            .select("tag_name", "tag_value") \
            .collect()
        arr = []
        for tag in tags:
            arr.append(metaTags(tag.tag_name, tag.tag_value))
        return arr


    def __get_tags(self, catalog, schema, table, column):
        tags = self.uc_column_tags.filter(f"catalog_name = '{catalog}' and schema_name = '{schema}' and table_name = '{table}' and column_name = '{column}'") \
            .select("tag_name", "tag_value") \
            .collect()
        arr = []
        for tag in tags:
            arr.append(metaTags(tag.tag_name, tag.tag_value))
        return arr


    def __get_columns(self, catalog, schema, table):
        arr = []
        cols = self.uc_column.filter(f"table_catalog = '{catalog}' and table_schema = '{schema}' and table_name = '{table}'") \
                    .select("column_name", "comment") \
                    .collect()
        for s in cols:
            tags = self.__get_tags(catalog, schema, table, s.column_name)
            arr.append(metaColums(s.column_name, s.comment, tags))

        return arr


    def __get_tables(self):
        arr = []
        for f in self.information_tables.collect():
            table = f"{f.table_catalog}.{f.table_schema}.{f.table_name}"
            columns = self.__get_columns(f.table_catalog, f.table_schema, f.table_name)
            tags = self.__get_table_tags(f.table_catalog, f.table_schema, f.table_name)
            location, partitionColumns, clusteringColumns = self.__load_meta_data(table)
            arr.append(metaTable(f.table_name,
                                 f.table_schema,
                                 f.table_catalog,
                                 table,
                                 f.table_owner,
                                 f.comment,
                                 partitionColumns,
                                 clusteringColumns,
                                 location,
                                 columns,
                                 tags))
        return arr


    def print_msg(self, msg="", space=""):
        print(space + msg)


    def if_table_exists(self, uc_table):
        uc = uc_table.split(".")
        spark.catalog.setCurrentCatalog(uc[0])
        spark.catalog.setCurrentDatabase(uc[1])
        return uc[2] in sqlContext.tableNames()


    def pretty(self, full=True):
        tables = self.__get_tables()
        self.print_msg("tables")
        for table in tables:
            col_stt = ""
            for column in table.columns:
                tag_col_stt = ""
                for tag in column.tags:
                    tag_col_stt += f"{tag.name} | {tag.value} "


                col_stt += f"""
                                column: {column.name} | comment: {column.comment} | tags: {tag_col_stt}"""

            tag_stt = ""
            for tag in table.tags:
                tag_stt += """{tag.name} | {tag.value} """

            stt = f"""
                      table : {table.uc_table}
                      owner : {table.owner}
                      comment : {table.comment}
                      partions : {table.partitions}
                      clustering : {table.clustering}
                      location : {table.location}
                      columns : {col_stt}
                      tags : {tag_stt}
                   """
            self.print_msg(stt)


    def save(self, table):
        espace_msg =  "  "
        self.print_msg(f"Init - {table.uc_table} ",espace_msg)
        uc_table = table.uc_table
        location = table.location
        partitions = table.partitions
        clustering = table.clustering
        location = table.location

        self.print_msg(f"location: {location}", espace_msg)
        dataframe = spark.read.load(location)
        dataframe.persist()
        count_start = dataframe.count()
        self.print_msg(f"before count: {count_start}", espace_msg)

        df = dataframe \
            .write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true")

        if len(partitions) > 0:
            self.print_msg(f"partitions:{partitions}", espace_msg)
            df = df.partitionBy(partitions.split(","))

        if len(clustering) > 0:
            self.print_msg(f"clustering:{clustering}", espace_msg)
            df = df.clusterBy(clustering.split(","))

        try:
            self.print_msg(f"droping table {uc_table}", espace_msg)
            spark.sql(f"DROP TABLE IF EXISTS {uc_table}", espace_msg)

            self.print_msg(f"table exists: {self.if_table_exists(uc_table)}", espace_msg)
            self.print_msg(f"saveAsTable table {uc_table}", espace_msg)
            spark.conf.set("spark.databricks.delta.commitValidation.enabled", False)
            spark.conf.set("spark.databricks.delta.stateReconstructionValidation.enabled", False)
            df.saveAsTable(uc_table)
            time.sleep(1)

            self.print_msg(f"table exists: {self.if_table_exists(uc_table)}", espace_msg)
            count_end = spark.read.table(uc_table).count()
            self.print_msg(f"after count table {count_end}", espace_msg)
            self.print_msg(f"ownert to {self.owner}", espace_msg)
            cmd = f"ALTER TABLE {uc_table} OWNER TO {self.owner};"
            spark.sql(cmd)

            self.print_msg(f"counts are equal -> {count_start == count_end}", espace_msg)

            self.print_msg("update metadata table", espace_msg)
            spark.sql(f"COMMENT ON TABLE {uc_table} IS '{table.comment}'")
            if len(table.tags) > 0:
                for tag in table.tags:
                    spark.sql(f"ALTER TABLE {uc_table} SET TAGS ('{tag.name}' = '{tag.value}')")

            self.print_msg("update metadata columns", espace_msg)
            if len(table.columns) > 0:
                for column in table.columns:
                    if column.comment is not None:
                        spark.sql(f"ALTER TABLE {uc_table} ALTER COLUMN {column.name} COMMENT '{column.comment}'")
                    if len(column.tags) > 0:
                        for tag in column.tags:
                            spark.sql(f"ALTER TABLE {uc_table} ALTER COLUMN {column.name} SET TAGS ('{tag.name}' = '{tag.value}')")

            self.print_msg("SUCESS")
        except Exception as e:
            self.print_msg(f"FAILED {uc_table} - {e}")
            recreate_table = f"""CREATE TABLE IF NOT EXISTS {uc_table}
                                    USING DELTA
                                    LOCATION '{location}'
                              """
            spark.sql(recreate_table)
            self.print_msg(f"recreate table {uc_table}", espace_msg)
            self.print_msg(f"table exists: {self.if_table_exists(uc_table)}", espace_msg)
            raise e
        self.print_msg("Done")


    def run(self):
        tables = self.__get_tables()
        index  = 1
        self.print_msg(f"total tables: {len(tables)}")
        for table in tables:
            self.print_msg(f"  [{index}] - table {table.uc_table}")
            self.save(table)
            index += 1
        self.information_tables.show()