import pandas as pd
import numpy as np


class recursive:
  def __init__(self, car, rec):
    self.car = car
    self.rec = rec
  
  def firstIteration(self):
    """Interescta lo que está en cartera y pagado con documentos, 
    relacionando el documento pasado que fue pagado con el que está en cartera"""
    car = self.car
    rec = self.rec
    start = pd.merge(car, rec, left_on = 'document_id', right_on = 'document_id')
    start.rename(columns = {'document_id':'document_id_1', 'past_document_id': 'document_id_2'}, inplace = True)
    start['n_iteration'] = 1
    return start

  def iterations(self):
    """Itera entre el documento pasado y el anterior, 
    hasta llegar al original para cada doc"""
    car = self.car
    rec = self.rec
    n = 2
    start_to_merge = recursive(car, rec).firstIteration()
    second = recursive(car, rec).firstIteration()
    while len(second['document_id_2'].unique())>0:

          second['n_iteration'] = n 
          second = pd.merge(second, rec[['document_id', 'past_document_id']], left_on = 'document_id_' + str(n), right_on = 'document_id')
          
          second.rename(columns = {'past_document_id': 'document_id_' + str(n+1)}, inplace = True)
          second = second.drop_duplicates().drop(columns = ['document_id'])
          start_to_merge = pd.concat([start_to_merge, second], axis = 0, ignore_index=True)
          n = n + 1
    return start_to_merge



def branches(df):
    """Elige solo las ramas completas"""
    #máxima iteración por documento
    df1 = df.groupby(['document_id_1', 'n_iteration']).count().reset_index().groupby('document_id_1')['n_iteration'].max().to_frame().reset_index() 
    #elige las ramas que están con la máxima iteración por doc
    onlys = pd.merge(df, df1, left_on = ['document_id_1', 'n_iteration'], right_on = ['document_id_1', 'n_iteration'], how = 'inner')
    return onlys.filter(regex = 'document|iteration')


def cartera_and_original(onlys):
  #first document per row, so the cartera document
  cartera_doc = onlys['document_id_1']
  #last document per row, so the original document
  original_doc = onlys.filter(regex='document').ffill(axis=1).iloc[:, -1] 
  #unimos documento original con el de la cartera para un buen trackeo
  cartera_original = pd.concat([cartera_doc, original_doc], axis = 1).drop_duplicates()
  return cartera_original


#ramas hacia el lado con data
def branches_to_side(complete_branches, data): 
  """Poner las ramas hacia el lado
  permite comprender de mejor manera 
  la historia del cobro"""
  d = {}
  for i in complete_branches.filter(regex='document').columns:
    d[i] = data[data['document_id'].isin(complete_branches[i].tolist())]
    d[i] = d[i].add_suffix(str(i[-2:]))
    complete_branches = pd.merge(complete_branches, d[i], left_on = i, right_on = i, how = 'left')
  return complete_branches

#ramas hacia abajo con data 
def branches_down(complete_branches, data): 
  """Poner las ramas hacia abajo
  permite un mejor filtrado de los documentos"""
  e = {}
  df_concats = pd.DataFrame()
  #itera en los document_id
  for i in complete_branches.filter(regex='document').columns:
    #toma las filas de data que estén en el document_id_i
    e[i] = data[data['document_id'].isin(complete_branches[i].tolist())]
    #une las filas de data del document id con la fila de complete branches i_document_id
    complete_branches_toconcat_data = pd.merge(complete_branches[[i]], e[i], left_on = i, right_on = i[:-2], how = 'left')
    #renombra i_document_id a document_id
    complete_branches_toconcat_data.rename(columns = {i:'document_id'})
    #va concatenando los resultados hacia abajo
    df_concats = pd.concat([complete_branches_toconcat_data, df_concats])
  #en caso de que se repita un doc en dos iteraciones, lo elimina
  return df_concats.dropna(subset = ['document_id']).drop_duplicates()

def reassign_normalization_executive(complete_branches_with_data):
  """reasignamos ejecutivo de normalización acorde a 
  el primer documento de la rama, es decir, acorde al 
  documento que está en cartera se le asignan todos los 
  documentos que estan asociados a este en el pasado"""
  #buscamos datos de ejecutivos de normalización
  executives = complete_branches_with_data.filter(regex='normalization_executive_name')
  #buscamos los documentos
  complete_branches = complete_branches_with_data.filter(regex = 'document_id')
  ex_rama = executives['normalization_executive_name_1']
  #asignamos a todos los documentos el ejecutivo del primero en la rama 1_document_id
  for column in executives.columns:
    executives[column] = ex_rama
  #unimos ejecutivos con los documentos lado a lado
  complete_executives = complete_branches.join(executives)
  #para cada documento tomamos su ejecutivo reasignado y lo ponemos hacia abajo
  assigned_executives = pd.DataFrame()
  for i in range(1,len(complete_branches.columns)):
    par = complete_executives.filter(regex = str(i))
    par.rename(columns = {par.columns[0]:'document_id', par.columns[1]: 'normalization_executive_name_pbi'}, inplace = True)
    assigned_executives = pd.concat([par, assigned_executives])
    #por la naturaleza de las ramas, se duplican los documentos, al ponerlos hacia abajo eliminamos duplicados
  return assigned_executives.dropna(subset = ['document_id']).drop_duplicates()