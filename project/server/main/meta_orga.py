import pandas as pd
import requests
import os
import pickle
from project.server.main.export_data_without_tunnel import dump_rnsr_data
from project.server.main.siren import format_siren
from project.server.main.paysage import format_paysage, dump_paysage_data, get_uai2siren
from project.server.main.ror import format_ror, dump_ror_data, get_grid2ror
from project.server.main.ods import get_ods_data, get_agreements, get_awards
from project.server.main.logger import get_logger
from project.server.main.utils_swift import download_object
from project.server.main.s3 import upload_object
from project.server.main.utils import chunks, to_jsonl, to_json, EXCLUDED_ID, build_ed_map, identifier_type, remove_duplicates

logger = get_logger(__name__)

MOUNTED_VOLUME = '/upw_data/'

def get_lists(uai2siren, grid2ror):
    logger.debug('getting list of ids to incorporate ...')
    logger.debug('from pcri projects')
    df_horizon_part = get_ods_data('fr-esr-horizon-projects-entities')
    df_h2020_part = get_ods_data('fr-esr-h2020-projects-entities')
    cols = ['entities_id_source', 'entities_id']
    df = pd.concat([df_horizon_part, df_h2020_part])[cols]

    sirens = df[df.entities_id_source=='siren'].entities_id.unique().tolist()
    sirets = df[df.entities_id_source=='siret'].entities_id.unique().tolist()
    rors = df[df.entities_id_source=='ror'].entities_id.unique().tolist()
    paysages = df[df.entities_id_source=='paysage'].entities_id.unique().tolist()

    logger.debug('from patents')
    download_object(container='patstat', filename=f'fam_final_json.jsonl', out=f'{MOUNTED_VOLUME}/fam_final_json.jsonl')
    df = pd.read_json(f'{MOUNTED_VOLUME}/fam_final_json.jsonl', lines=True, chunksize=10000)
    for c in df:
        pats = c.to_dict(orient='records')
        for p in pats:
            if isinstance(p.get('applicants'), list):
                for a in p['applicants']:
                    if isinstance(a.get('ids'), list):
                        for cur_id in a['ids']:
                            if cur_id.get('type') == 'siren' and len(cur_id['id']) == 9:
                                sirens.append(cur_id['id'])
                            if cur_id.get('type') == 'siret' and len(cur_id['id']) == 14:
                                sirets.append(cur_id['id'])
                            if cur_id.get('type') == 'id_paysage':
                                paysages.append(cur_id['id'])
                            if cur_id.get('type') == 'ror':
                                rors.append(cur_id['id'])

    # data from rnsr
    logger.debug('from labs institutions')
    df = pd.read_json('/upw_data/scanr/orga_ref/rnsr.jsonl.gz', lines=True)
    for e in df.to_dict(orient='records'):
        if isinstance(e.get('institutions'), list):
            for inst in e['institutions']:
                if isinstance(inst.get('structure'), str):
                    if len(inst['structure']) == 9:
                        sirens.append(inst['structure'])
                    elif len(inst['structure']) == 14:
                        sirets.append(inst['structure'])
                    else:
                        logger.debug(f'inst from rnsr {inst}')

    #awards
    awards = get_awards(corresp = {}) # with no correspondance, siren are returned
    sirens += [k for k in awards if len(k)==9]
    
    #agreements
    agreements = get_agreements(corresp = {})
    sirens += [k for k in agreements if len(k)==9]

    # projects
    df = pd.read_json('https://scanr-data.s3.gra.io.cloud.ovh.net/production/projects-v2.jsonl.gz', lines=True)
    for e in df.to_dict(orient='records'):
        if isinstance(e.get('participants'), list):
            for p in e['participants']:
                if isinstance(p.get('structure'), str):
                    id_type = identifier_type(p.get('structure'))
                    if id_type=='siren':
                        sirens.append(p['structure'])
                    if id_type=='siret':
                        sirets.append(p['structure'])
                    if id_type=='ror':
                        rors.append(p['structure'])
                    if p['structure'] in uai2siren:
                        sirens.append(uai2siren[p['structure']])
                    if p['structure'] in grid2ror:
                        rors.append(grid2ror[p['structure']])

    sirens = list(set(sirens))
    sirets = list(set(sirets))
    rors = list(set(rors))
    paysages = list(set(paysages))
    ans = {'siren': sirens, 'siret':sirets, 'ror': rors, 'paysage': paysages}
    for k in ans:
        logger.debug(f'{k}: {len(ans[k])} elts')
    pickle.dump(ans, open('/upw_data/lists.pkl', 'wb'))
    return ans

def get_meta_orga():
    full_data = []
    
    #paysage
    dump_paysage_data()
    dump_ror_data()
    uai2siren = get_uai2siren()
    dump_rnsr_data(500, uai2siren)

    grid2ror = get_grid2ror()

    #rnsr
    rnsr_data = pd.read_json('/upw_data/scanr/orga_ref/rnsr.jsonl.gz', lines=True).to_dict(orient='records')
    logger.debug(f'{len(rnsr_data)} elts from rnsr')
    full_data += rnsr_data
        
    lists = get_lists(uai2siren, grid2ror)
        
    try:
        lists = pickle.load(open('/upw_data/lists.pkl', 'rb'))
        logger.debug('read pkl list')
    except:
        lists = get_lists(uai2siren, grid2ror)
    
    paysage_data = format_paysage(lists['paysage'], lists['siren'])
    to_jsonl(paysage_data, '/upw_data/scanr/orga_ref/paysage_data_formatted.jsonl')
    logger.debug(f'{len(paysage_data)} elts from paysage')
    full_data += paysage_data
    existing_siren, existing_ror = [], []
    for e in paysage_data:
        for k in e.get('externalIds', []):
            if k.get('type', '').startswith('sire'):
                existing_siren.append(k['id'][0:9])
            if k.get('type', '').startswith('ror'):
                existing_ror.append(k['id'])
    
    #siren
    siren_data = format_siren(lists['siren'], lists['siret'], existing_siren)
    to_jsonl(siren_data, '/upw_data/scanr/orga_ref/siren_data_formatted.jsonl')
    logger.debug(f'{len(siren_data)} elts from siren')
    full_data += siren_data
    
    # ror
    ror_data = format_ror(lists['ror'], existing_ror)
    logger.debug(f'{len(ror_data)} elts from ror')
    full_data += ror_data

    # ed (already in paysage)
    #ed_map = build_ed_map()
    #ed_data = list(ed_map.values())
    #to_jsonl(ed_data, '/upw_data/scanr/orga_ref/ed_data_formatted.jsonl')
    #logger.debug(f'{len(ed_data)} elts from ed')
    #full_data+= ed_data

    for org in full_data:
        for f in ['institutions', 'parents', 'leaders']:
            if org.get(f):
                org[f] = remove_duplicates(org[f], org['id'])

    to_jsonl(full_data, '/upw_data/scanr/orga_ref/organizations-v2.jsonl')
    logger.debug(f'{len(full_data)} elts from total')
    os.system('cd /upw_data/scanr/orga_ref/ && rm -rf organizations-v2.jsonl.gz && gzip organizations-v2.jsonl')
    upload_object(container='scanr-data', source = f'/upw_data/scanr/orga_ref/organizations-v2.jsonl.gz', destination='production/organizations-v2.jsonl.gz')
