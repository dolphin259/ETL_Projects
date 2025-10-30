import pandas as pd

class TemplateOperatorDB:
    def __init__(self, table_name):
        self.table_name = table_name
    
    def create_query_insert_into(self, dataframe):
        self.columns = ""
        self.values = ""
        self.odku = ""

        end_col = dataframe.columns[-1]
        for col in dataframe.columns:
            if col == end_col:
                self.columns += col
                self.values += "%s"
                self.odku += col + "=" + "VALUES(" + col + ")"
            else:
                self.columns += col + ", "
                self.values += "%s, "
                self.odku += col + "=" + "VALUES(" + col + "), "

        create_query = \
            f"INSERT INTO {self.table_name}" + \
            f" ({self.columns}) " + \
            f"VALUES ({self.values}) " + \
            f"ON DUPLICATE KEY UPDATE {self.odku}"
        return create_query
    
    def create_delete_query(self, key_field, values):
        self.key_field = key_field
        self.values = values
        self.place_holder = ""
        i = 0
        while i < len(self.values):
            if i == len(self.values) - 1:
                self.place_holder += "%s"
            else:
                self.place_holder += "%s, "
            i += 1
                
        create_query = f"""
            DELETE FROM {self.table_name}
            WHERE {self.key_field} IN ({self.place_holder})
        """
        return create_query
