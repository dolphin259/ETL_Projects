from airflow.hooks.mysql_hook import MySqlHook
from support_processing import TemplateOperatorDB
import logging
from contextlib import closing
import os

class MySQLOperators:
    def __init__(self, conn_id="mysql"):
        try:
            self.mysqlhook = MySqlHook(mysql_conn_id=conn_id)
            self.mysql_conn = self.mysqlhook.get_conn()
        except:
            logging.error(f"Can't connect to {conn_id} database")
    def get_data_to_pd(self, query=None):
        return self.mysqlhook.get_pandas_df(query)
    
    def get_records(self, query):
        return self.mysqlhook.get_records(query)
    
    def execute_query(self, query):
        try:
            cur = self.mysql_conn.cursor()
            cur.execute(query)
            self.mysql_conn.commit()
        except:
            logging.ERROR(f"cant execute query: {query}")
    
    def insert_dataframe_into_table(self, table_name, dataframe, data, chunk_size=100000):
        query = TemplateOperatorDB(table_name).create_query_insert_into(dataframe)
        try:
            with closing(self.mysql_conn) as conn:
                if self.mysqlhook.supports_autocommit:
                    self.mysqlhook.set_autocommit(conn, False)
                conn.commit()
                with closing(conn.cursor()) as cur:
                    for i in range(0, len(data), chunk_size):
                        partitioned_data = data[i:i+chunk_size]
                        lst = []
                        for row in partitioned_data:
                            sub_lst = []
                            for cell in row:
                                sub_lst.append(self.mysqlhook._serialize_cell(cell, conn))
                            lst.append(sub_lst)
                        values = tuple(lst)
                        num_records = len(values)
                        cur.executemany(query, values)
                        print(f"merged or updated {num_records} records")
                        conn.commit()
                conn.commit()
            
        except:
            logging.ERROR(f"cant execute {query}")
    
    def delete_records_in_table(self, table_name, key_field, values):
        query = TemplateOperatorDB(table_name).create_delete_query(key_field, values)
        
        try:
            with closing(self.mysql_conn) as conn:
                if self.mysqlhook.supports_autocommit:
                    self.mysqlhook.set_autocommit(conn, False)
                conn.commit()
                with closing(conn.cursor()) as cur:
                    lst = []
                    for cell in values:
                       lst.append(self.mysqlhook._serialize_cell(cell, conn))
                    del_values = tuple(lst)
                    cur.execute(query, del_values)
                    num_records = len(values)
                    print(f"deleted {num_records} records")
                    conn.commit()
                conn.commit()
        except:
            logging.ERROR(f"cant execute {query}")
        
    def insert_data_into_table(self, table_name, data, create_table_like=""):
        if create_table_like != "":
            create_tbl_query = f"CREATE TABLE IF NOT EXISTS {table_name} LIKE {create_table_like};"
            conn = self.mysql_conn
            cur = conn.cursor()
            cur.execute(create_tbl_query)
            conn.commit()
        try:
            self.mysqlhook.insert_rows(table_name, data)
        except:
            logging.ERROR(f"cant insert data into {table_name}")
    
    def remove_table_if_exists(self, table_name):
        try:
            remove_table = f"DROP TABLE IF EXISTS {table_name};"
            cur = self.mysql_conn.cursor()
            cur.execute(remove_table)
            self.mysql_conn.commit()
        except:
            logging.ERROR(f"cant remove table: {table_name}")
    
    def truncate_all_data_from_table(self, table_name):
        try:
            truncate_table = f"TRUNCATE TABLE {table_name};"
            cur = self.mysql_conn.cursor()
            cur.execute(truncate_table)
            self.mysql_conn.commit()
        except:
            logging.ERROR(f"cant truncate data from: {table_name}")
    
    def dump_table_into_path(self, table_name):
        try:
            priv = self.mysqlhook.get_first("SELECT @@global.secure_file_priv")
            if priv and priv[0]:
                tbl_name = list(table_name)[0]
                file_name = tbl_name.replace(".", "__")
                self.mysqlhook.bulk_dump(tbl_name, os.path.join(priv[0], f"{file_name}.txt"))
            else:
                logging.ERROR("missing priviledge")
        except:
            logging.ERROR(f"cant dump {table_name}")
    def load_data_into_table(self, table_name):
        try:
            priv = self.mysqlhook.get_first("SELECT @@global.secure_file_priv")
            if priv and priv[0]:
                file_path = os.path.join(priv[0], "TABLES.txt")
                load_data_into_tbl = f"LOAD DATA INFILE '{file_path}' INTO TABLE {table_name};"
                cur = self.mysql_conn.cursor()
                cur.execute(load_data_into_tbl)
                self.mysql_conn.commit()
            else:
                logging.ERROR("missing priviledge")
        except:
            logging.ERROR(f"cant load {table_name}")