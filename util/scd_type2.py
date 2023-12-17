from pyspark.sql.functions import current_date
from util.config_reader import config_reader
from util.spark_session import get_spark
from pyspark.sql.window import Window as W
import pyspark.sql.functions as F
import time
class scd_type2:

    def __init__(self):
        self.config_reader = config_reader()
        self.get_spark = get_spark()
        self.config_pipeline = self.config_reader.get_config_pipeline()
        self.source_file_path = self.config_pipeline.get('CONFIG_DETAIL', 'SOURCE_PATH')
        #self.source_file_path_new_load = self.config_pipeline.get('CONFIG_DETAIL', 'SOURCE_PATH_NEW_LOAD')
        self.end_date = self.config_pipeline.get('CONFIG_DETAIL', 'EOW_DATE')
        self.dest_path = self.config_pipeline.get('CONFIG_DETAIL', 'DEST_PATH')
        self.history_path = self.config_pipeline.get('CONFIG_DETAIL', 'DEST_PATH')
        self.temp_path = self.config_pipeline.get('CONFIG_DETAIL', 'TEMP_PATH')
        self.key_list = self.config_pipeline.get('CONFIG_DETAIL', 'KEY_LIST')
        self.type2_cols = self.config_pipeline.get('CONFIG_DETAIL', 'TYPE2_COLS')
        self.scd2_cols = self.config_pipeline.get('CONFIG_DETAIL', 'SCD2_COLS')
        self.stg_path = self.config_pipeline.get('CONFIG_DETAIL','STG_PATH')
        self.sel_col = self.config_pipeline.get('CONFIG_DETAIL', 'SEL_COL')
        self.date_format = self.config_pipeline.get('CONFIG_DETAIL', 'DATE_FORMAT')
        sparkconfig = self.config_reader.getSparkConfig()
        self.spark_session = self.get_spark.getSparkSession(sparkconfig)
        pass

    def read_data_source(self,batch,filename=''):
        if batch == 'first':
            df = self.spark_session.read.options(header=True, delimiter=',', inferSchema='True').csv(
                self.source_file_path+filename)
            window_spec = W.orderBy("customerid")
            df = df \
                .withColumn("sk_customer_id", F.row_number().over(window_spec)) \
                .withColumn("effective_date", F.date_format(current_date(), self.date_format)) \
                .withColumn("expiration_date", F.date_format(F.lit(self.end_date), self.date_format)) \
                .withColumn("current_flag", F.lit(True))
            print("First time load:")
            df.show()
            return df
        elif batch == 'hist':
            df = self.spark_session.read.options(header=True, delimiter=',', inferSchema='True') \
                .csv(self.history_path) \
                .withColumn("expiration_date",F.date_format(F.col('expiration_date'), self.date_format)).\
                withColumn("effective_date", F.date_format(F.col('effective_date'), self.date_format))
            print("Historical data read from target::")
            df.show(100)
            return df
        else:
            df = self.spark_session.read.options(header=True, delimiter=',', inferSchema='True') \
            .csv(self.source_file_path+filename)
            print("Current Incoming Load:")
            df.show(100)
            return df

    def write_data_target(self,df):
        df.coalesce(1).write.mode('overwrite') \
            .option("header", True) \
            .option("delimiter", ",") \
            .csv(self.stg_path)
        print("Writing results:")
        df.show()
        self.spark_session.read.options(header=True, delimiter=',', inferSchema='True').csv(self.stg_path).coalesce(1).write.mode('overwrite') \
            .option("header", True) \
            .option("delimiter", ",") \
            .csv(self.dest_path)
        return 1

    def get_hash(self,df,type2_cols):
        """
        input:
            df: dataframe
            key_list: list of columns to be hashed
        output:
            df: df with hashed column
        """
        columns = [F.col(column) for column in type2_cols]
        if columns:
            return df.withColumn("hash_md5", F.md5(F.concat_ws("", *columns)))
        else:
            return df.withColumn("hash_md5", F.md5(F.lit(1)))

    def column_renamer(self,df, suffix, append):
        """
        input:
            df: dataframe
            suffix: suffix to be appended to column name
            append: boolean value
                    if true append suffix else remove suffix

        output:
            df: df with renamed column
        """
        if append:
            new_column_names = list(map(lambda x: x + suffix, df.columns))
        else:
            new_column_names = list(map(lambda x: x.replace(suffix, ""), df.columns))
        return df.toDF(*new_column_names)

    def scd_type2_implementation(self,batch,filename):
        if batch == "first":
            employee_detail_first_load = self.read_data_source('first',filename)
            return self.write_data_target(employee_detail_first_load)
        else:
            employee_detail_incoming = self.read_data_source('incoming',filename)
            employee_detail_hist = self.read_data_source('hist')
            """
                We are segregating the history data on basis of open and closed records.
                We are not going to do any calculation/manipulation on closed record.
            """
            employee_detail_hist_open = employee_detail_hist.where(F.col('current_flag')=='true')
            employee_detail_hist_closed = employee_detail_hist.where(F.col('current_flag')=='false')
            """
                We will rename the columns of two dataframe for clear understanding purpose only, also
                to resolve the column ambiguity problem in pyspark.
            """
            employee_detail_hist_open_hash = self.column_renamer(self.get_hash(employee_detail_hist_open,eval(self.type2_cols)),"_history",True)
            employee_detail_incoming_hash = self.column_renamer(self.get_hash(employee_detail_incoming,eval(self.type2_cols)),"_current",True)

            """
                Join the two data frame and check whether we have insert,delete,upadate and no change records
                Apply full_outer_join
            """

            result = employee_detail_hist_open_hash.join(employee_detail_incoming_hash,F.col('customerid_current')==F.col('customerid_history'),"full_outer") \
                .withColumn("Action",F.when(F.col("hash_md5_history") == F.col("hash_md5_current"),F.lit("NoChange")) \
                            .when(F.col("customerid_current").isNull(),F.lit("Delete")).when(F.col("customerid_history").isNull(),F.lit("Insert")) \
                            .otherwise(F.lit("Update")))

            df_nochange = self.column_renamer(result.where(F.col("action") == 'NoChange'),"_history",False)
            """
            we will get the max id from history data , so that using this we can add new records or insert new updated record its like 
            autoincrement
            max_sk = employee_detail_hist.agg({"sk_customer_id": "max"}).collect()[0][0]
            """
            max_sk = employee_detail_hist.agg({"sk_customer_id":"max"}).collect()[0][0]
            winspec = W.orderBy("customerId")
            df_insert = self.column_renamer(result.where(F.col("action")=='Insert'),"_current",False) \
            .select(employee_detail_incoming.columns).withColumn("effective_date",F.date_format(current_date(),self.date_format)) \
                .withColumn("expiration_date",F.date_format(F.lit(self.end_date),self.date_format)) \
                .withColumn("current_flag",F.lit(True)).withColumn("rw",F.row_number().over(winspec)) \
                .withColumn("sk_customer_id",F.col('rw') + max_sk)
            print("Records to be added:")
            df_insert.show(100)

            df_delete = self.column_renamer(result.where(F.col("action")=='Delete'),"_history",False) \
            .select(employee_detail_hist.columns).withColumn("expiration_date",F.date_format(current_date(),self.date_format)) \
                .withColumn("current_flag",F.lit(False))
            print("Records to be deleted:")
            df_delete.show(100)

            max_sk_insert = df_insert.agg({"sk_customer_id": "max"}).collect()[0][0]
            max_sk_hist = employee_detail_hist.agg({"sk_customer_id": "max"}).collect()[0][0]
            max_sk = max_sk_insert if (0 if max_sk_insert == None else max_sk_insert) > (0 if max_sk_hist == None else max_sk_hist) else max_sk_hist
            df_update = self.column_renamer(result.where(F.col("action") == 'Update'), "_history", False) \
                .select(employee_detail_hist.columns).withColumn("expiration_date",F.date_format(current_date(), self.date_format)) \
                .withColumn("current_flag", F.lit(False)).unionByName(
                self.column_renamer(result.where(F.col("action") == 'Update'), "_current", False) \
                    .select(employee_detail_incoming.columns) \
                    .withColumn("expiration_date",F.date_format(F.lit(self.end_date), self.date_format)) \
                    .withColumn("effective_date", F.date_format(current_date(), self.date_format)) \
                    .withColumn("current_flag", F.lit(True)) \
                    .withColumn("rw",F.row_number().over(winspec))
                    .withColumn("sk_customer_id",F.col('rw')+max_sk).drop("rw")
            )
            print("Records to be updated:")
            df_update.show()

            final_df = df_insert.select(eval(self.sel_col)).unionByName(df_update.select(eval(self.sel_col))) \
            .unionByName(df_nochange.select(eval(self.sel_col))).unionByName(df_delete.select(eval(self.sel_col))) \
            .unionByName(employee_detail_hist_closed.select(eval(self.sel_col)))
            return self.write_data_target(final_df)
        return 0
