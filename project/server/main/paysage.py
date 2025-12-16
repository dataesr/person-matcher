import requests
import os
from project.server.main.ods import get_ods_data
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def get_paysage_data():
    df_paysage_id = get_ods_data('fr-esr-paysage_structures_identifiants')
    df_paysage_struct = get_ods_data('structures-de-paysage-v2')
    df_siren = df_paysage_id[df_paysage_id.id_type=='siret']
    df_siren['siren'] = df_siren.id_value.apply(lambda x:x[0:9])
    df_ror = df_paysage_id[df_paysage_id.id_type=='ror']
    return df_paysage_struct, df_siren, df_ror

def get_paysage_id(siren, df_siren, df_ror):
    paysage_id = None
    potential_paysage_ids = df_siren[df_siren.siren==siren].id_paysage.to_list()
    if len(potential_paysage_ids) == 1:
        paysage_id = potential_paysage_ids[0]
    elif len(potential_paysage_ids) > 1:
        potential_paysage_ids_with_ror = df_ror[df_ror['id_paysage'].apply(lambda x: x in set(potential_paysage_ids))].id_paysage.to_list()
        if len(potential_paysage_ids_with_ror) == 1:
            paysage_id = potential_paysage_ids_with_ror[0]
    return paysage_id

def get_status_from_paysage(paysage_id, df_paysage_struct):
    records = df_paysage_struct[df_paysage_struct['id']==paysage_id].to_dict(orient='records')
    assert(len(records)==1)
    status = records[0]['structurestatus']
    ans = {}
    if status == 'inactive':
        ans['status'] = 'old'
    elif status == 'active':
        ans['status'] = 'active'
    closuredate = records[0].get('closuredate')
    if closuredate and closuredate==closuredate and status=='inactive':
        ans['endDate'] = closuredate+'T00:00:00'
    return ans

def get_status_from_siren(siren, df_paysage_struct, df_siren, df_ror):
    paysage_id = get_paysage_id(siren, df_siren, df_ror)
    ans = {}
    if paysage_id:
        ans = get_status_from_paysage(paysage_id, df_paysage_struct)
    return ans
