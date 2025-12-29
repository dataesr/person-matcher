import requests
import os
from project.server.main.ods import get_ods_data
from project.server.main.utils import chunks, to_jsonl, to_json
from project.server.main.s3 import upload_object
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def get_paysage_data():
    df_paysage_id = get_ods_data('fr-esr-paysage_structures_identifiants')
    df_paysage_struct = get_ods_data('structures-de-paysage-v2')
    df_siren = df_paysage_id[df_paysage_id.id_type=='siret']
    df_siren['siren'] = df_siren.id_value.apply(lambda x:x[0:9])
    df_ror = df_paysage_id[df_paysage_id.id_type=='ror']
    return df_paysage_struct, df_siren, df_ror

def dump_paysage_data():
    df_paysage_id = get_ods_data('fr-esr-paysage_structures_identifiants')
    df_paysage_struct = get_ods_data('structures-de-paysage-v2')
    df_web = get_ods_data('fr-esr-paysage_structures_websites')
    id_map, web_map = {}, {}, {}
    for e in df_paysage_id.to_dict(orient='records'):
        current_paysage = e['id_paysage']
        if current_paysage not in id_map:
            id_map[current_paysage] = []
        new_elt, new_elt_siren = {}, {}
        if e['id_type'] in ['ror', 'rnsr', 'siret']:
            for f in ['id_value', 'id_type', 'active', 'id_startdate', 'id_enddate']:
                if e.get(f):
                    new_elt[f] = e[f]
        id_map[current_paysage].append(new_elt)
        if e['id_type'] == 'siret':
            new_elt_siren = new_elt.copy()
            new_elt_siren['id_type'] = 'siren'
            new_elt_siren['id_value'] = new_elt['id_value'][0:9]
            id_map[current_paysage].append(new_elt)
    for e in df_web.to_dict(orient='records'):
        current_paysage = e['id_structure_paysage']
        if current_paysage not in web_map:
            web_map[current_paysage] = []
        web_map[current_paysage].append({'url': e['url'], 'type': e['type']})
    data = []
    for e in df_paysage_struct.to_dict(orient='records'):
        current_paysage = e['id']
        external_ids = [{'id_type': 'paysage', 'id_value': current_paysage}]
        if current_paysage in id_map:
            external_ids += id_map[current_paysage]
        e['external_ids'] = external_ids
        if current_paysage in web_map:
            e['websites'] = web_map[current_paysage]
        data.append(e)
    os.system(f'rm -rf /upw_data/scanr/orga_ref/paysage.jsonl')
    to_jsonl(data, f'/upw_data/scanr/orga_ref/paysage.jsonl')
    os.system(f'cd /upw_data/scanr/orga_ref && rm -rf paysage.jsonl.gz && gzip -k paysage.jsonl')
    upload_object(container='scanr-data', source = f'/upw_data/scanr/orga_ref/paysage.jsonl.gz', destination=f'production/paysage.jsonl.gz')

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


def format_paysage():
    paysage_formatted = []
    for e in df_paysage.to_dict(orient='records'):
        new_elt = {'id': e['id']}
        new_elt['externalIds'] = [{'id': e['id'], 'type': 'paysage'}]
        for k in e['external_ids']:
            new_k = {'id': k['id_value'], 'type': k['id_type']}
            if new_k not in new_elt['externalIds']:
                new_elt['externalIds'].append(new_k)
        # startDate
        if isinstance(e.get('creationdate'), str):
            if len(e['creationdate'])==4:
                new_elt['startDate'] = e['creationdate']+'-01-01T00:00:00'
            elif len(e['creationdate'])==10:
                new_elt['startDate'] = e['creationdate']+'T00:00:00'
            elif len(e['creationdate'])==7:
                new_elt['startDate'] = e['creationdate']+'-01T00:00:00'
            else:
                print(e['creationdate'])
        if new_elt.get('startDate'):
            new_elt['creationYear'] = int(new_elt['startDate'][0:4])
        # status
        if e.get('structurestatus')=='inactive':
            new_elt['status'] = 'old'
        else:
            new_elt['status']='active'
        # endDate
        if isinstance(e.get('closuredate'), str):
            if len(e['closuredate'])==4:
                new_elt['endDate'] = e['closuredate']+'-01-01T00:00:00'
            elif len(e['closuredate'])==10:
                new_elt['endDate'] = e['closuredate']+'T00:00:00'
            elif len(e['closuredate'])==7:
                new_elt['endDate'] = e['closuredate']+'-01T00:00:00'
            else:
                print(e['closuredate'])
        # name
        currentname = json.loads(e.get('currentname', '{}'))
        if isinstance(e.get('officialname'), str):
            new_elt['label'] = {'default': e['officialname']}
        if isinstance(e.get('usualname'), str):
            new_elt['label'] = {'default': e['usualname']}
        if isinstance(e.get('nameen'), str):
            new_elt['label']['en'] = e['nameen']
        new_elt['acronym'] = {}  
        if isinstance(currentname.get('acronymEn'), str):
            new_elt['acronym']['en'] = currentname['acronymEn']
            new_elt['acronym']['default'] = currentname['acronymEn']
        if isinstance(currentname.get('acronymFr'), str):
            new_elt['acronym']['fr'] = currentname['acronymFr']
            new_elt['acronym']['default'] = currentname['acronymFr']
        # kind ??
        # level ??
        new_elt['description'] = {}
        if isinstance(e.get('descriptionfr'), str):
            new_elt['description']['fr'] = e.get('descriptionfr')
        if isinstance(e.get('descriptionen'), str):
            new_elt['description']['en'] = e.get('descriptionen')
        #address
        address = {'main': True}
        for f in ['country', 'city', 'address', 'iso3']:
            if isinstance(e.get(f), str):
                address[f] = e[f]
        if address.get('city') is None and isinstance(e.get('locality'), str):
            address['city'] = e['locality']
        if isinstance(e.get('gps'), str):
            lat = e['gps'].split(',')[0]
            lon = e['gps'].split(',')[1]
            address['gps'] = {'lat': lat, 'lon': lon}
        new_elt['address'] = [address]
        if e.get('websites'):
            new_elt['links'] = e['websites']
        paysage_formatted.append(new_elt)
    return paysage_formatted


