import pandas as pd
import numpy as np
import pymysql
from models.scripts import recursive, branches, cartera_and_original, branches, branches_to_side, branches_down, reassign_normalization_executive

from queryes.tables import cartera_query, rec_query, data_query

conn=pymysql.connect(host='54.175.78.29',port=int(3306),user='fivreaduser',passwd='0Q4W3@pE^pb5Nu',db='dbFactorClickProd')

print("hi fivana")
#import data needed
car = pd.read_sql(cartera_query, conn)

rec = pd.read_sql(rec_query, conn)

data = pd.read_sql(data_query, conn)


#start executing scripts

recurs = recursive(car, rec).iterations()

complete_branches = branches(recurs)

cartera_original = cartera_and_original(complete_branches)

complete_branches_with_data = branches_to_side(complete_branches, data)

df_concats = branches_down(complete_branches, data)

executive_with_document = reassign_normalization_executive(complete_branches_with_data)

df_concats_new_executive = pd.merge(df_concats, executive_with_document, left_on = 'document_id', right_on = 'document_id')

c_o1 = pd.merge(cartera_original, data.add_prefix('car_'), left_on = 'document_id_1', right_on = 'car_document_id', how = 'left')
c_19 = pd.merge(c_o1, data.add_prefix('ori_'), left_on = 'document_id_9', right_on = 'ori_document_id', how = 'left')

print(df_concats_new_executive.columns)
print(df_concats_new_executive.shape)