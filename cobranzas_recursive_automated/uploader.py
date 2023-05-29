import datetime
import logging
import psycopg2
from main import df_concats_new_executive
import pandas as pd
import numpy as np

logging.basicConfig()
logger = logging.getLogger(__name__)

print(df_concats_new_executive.columns)
print(df_concats_new_executive.shape)
df_concats_new_executive.replace({pd.NaT: None}, inplace=True)
class PGAuroraRepository:
    def __init__(self, c_str, df ,  *args, **kwargs ):
        self.connection = None
        self.c_str = c_str
        self.__clean_c_str()
        self.df = df
        self.sort_fields_map = self.__map_sort_fields()
        self.sort_direction_map = self.__map_sort_directions()


    def __clean_c_str(self):
        if self.c_str is None:
            return
        self.c_str = self.c_str.replace("Host", "host")
        self.c_str = self.c_str.replace("Database", "dbname")
        self.c_str = self.c_str.replace("Username", "user")
        self.c_str = self.c_str.replace("Password", "password")
        self.c_str = self.c_str.replace(";", " ")

    def connect(self):
        self.connection = psycopg2.connect(self.c_str)

    def register_ruts(self):
        df = self.df
        values = []
        date = datetime.datetime.utcnow()
        day = str(date.date())

        # Delete existing rows
        delete_query = "DELETE FROM cobranzas_recursive_docs_with_executive;"
        cursor = None

        try:
            self.connect() if (self.connection is None) else None
            cursor = self.connection.cursor()

            # Execute the delete query
            cursor.execute(delete_query)
            self.connection.commit()

            for index, row in df.iterrows():
                values.append(tuple(row))
            
            placeholders = ','.join(['%s'] * len(df.columns))
            query = """
            INSERT INTO cobranzas_recursive_docs_with_executive ({}) VALUES ({});""".format(','.join(df.columns), placeholders)
            
            # Execute the insert query with new data
            cursor.executemany(query, values)
            self.connection.commit()
            
            count = cursor.rowcount
        except Exception as e:
            print(e)
        finally:
            cursor.close() if cursor is not None else None
        return count > 0

    
    def __map_sort_fields(self):
        df = self.df
        fields = {}
        for column in df.columns:
            # Map the column name to the desired display name
            fields[column] = f'"{column}"'  # Assuming the column name itself is the desired display name
        return fields
    
    def __map_sort_directions(self):
        directions = {
            "asc": "asc",
            "desc": "desc"
        }
        return directions

    
#aurora_repo = PGAuroraRepository(c_str=connection_string , df = df_concats_new_executive)
aurora_repo = PGAuroraRepository(c_str='postgresql://postgres:9QqnUdZvr6zWz4W@pg-aurora-serverless.cluster-cyzwrcs8gffc.us-east-1.rds.amazonaws.com/FIVANA_DB', df = df_concats_new_executive)

#aurora_repo.sort_fields_map = aurora_repo.__map_sort_fields(df_concats_new_executive)
result = aurora_repo.register_ruts()
#print(result)
