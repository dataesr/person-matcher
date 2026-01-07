import pandas as pd
import requests
import pickle
from project.server.main.export_data_without_tunnel import dump_rnsr_data
from project.server.main.siren import format_siren
from project.server.main.paysage import format_paysage, dump_paysage_data
from project.server.main.ods import get_ods_data
from project.server.main.logger import get_logger
from project.server.main.utils_swift import download_object

logger = get_logger(__name__)

MOUNTED_VOLUME = '/upw_data/'

def get_lists():
    df_horizon_part = get_ods_data('fr-esr-horizon-projects-entities')
    df_h2020_part = get_ods_data('fr-esr-h2020-projects-entities')
    cols = ['entities_id_source', 'entities_id']
    df = pd.concat([df_horizon_part, df_h2020_part])[cols]

    sirens = df[df.entities_id_source=='siren'].entities_id.unique().tolist()
    sirets = df[df.entities_id_source=='siret'].entities_id.unique().tolist()
    rors = df[df.entities_id_source=='ror'].entities_id.unique().tolist()
    paysages = df[df.entities_id_source=='paysage'].entities_id.unique().tolist()

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
    try:
        lists = pickle.load(open('/upw_data/lists.pkl', 'rb'))
    except:
        lists = get_lists()
    #siren
    siren_data = format_siren(lists['siren'], lists['siret'])
    #paysage
    dump_paysage_data()
    paysage_data = format_paysage()
    #rnsr
    dump_rnsr_data()
