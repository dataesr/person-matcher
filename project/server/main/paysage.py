import requests
import os
import pandas as pd
import json
import gzip
import json
import requests
from retry import retry

from project.server.main.ods import get_ods_data
from project.server.main.utils import chunks, to_jsonl, to_json, identifier_type
from project.server.main.s3 import upload_object
from project.server.main.siren import format_siren
from project.server.main.logger import get_logger

logger = get_logger(__name__)
PAYSAGE_URL = os.getenv('PAYSAGE_URL')
PAYSAGE_API_KEY = os.getenv('PAYSAGE_API_KEY')

PAYSAGE_CAT_TO_KEEP = set(pd.read_csv('categories_for_scanr.csv')['id'].to_list())

def get_typologie(elt):
    CAT_UNIV = [
            {'id': 'LzIYN', 'name': 'Université scientifique et/ou médicale'},
            {'id': 'tu7fX', 'name': 'Université pluridisciplinaire avec santé'},
            {'id': 'UvC9F', 'name': 'Université pluridisciplinaire hors santé'},
            {'id': '5bRja', 'name': 'Université tertiaire - droit et économie'},
            {'id': 'yX0Xs', 'name': 'Université tertiaire - lettres et sciences Humaines'},
            {'id': 'nw20k', 'name': 'Université de technologie'},
            {'id': 'mCpLW', 'name': 'Université'}
            ]
    CAT_ORGANISME = [{'id': '2ZdzP', 'name': 'Organisme de recherche'}, 
            {'id': 'B67Dl', 'name': "Organisme lié à la recherche"}]
    CAT_INFRA = [
        {'id': '4Ak5K', 'name': 'Infrastructure de recherche'},
        {'id': 'bf4uj', 'name': 'Infrastructure de recherche candidate à la feuille de route 2026'}
            ]
    CAT_ENTREPRISE = [{'id': 's79DJ', 'name': 'Entreprise'},
            {'id': 'rh13d', 'name': 'Start-Up'},
            {'id': 'uZ2E2', 'name': "Entreprise étrangère"}]
    CAT_ETRANGER = [{'id': 'NsMkU', 'name': "Établissement d'enseignement supérieur étranger"},
            {'id': 'P3XZB', 'name': "Institution étrangère active en matière de recherche et d'innovation"},
            {'id': 'E61CB', 'name':"Institution étrangère en charge du financement de l'enseignement supérieur, de la recherche et de l'innovation"},
            {'id': 'C9nJr', 'name': "Institution étrangère d'expertise et d'aide à la décision"},
            {'id': 'XQE8E', 'name': "Institution étrangère en charge de la définition des politiques d'enseignement supérieur, de recherche et d'innovation"}]
    CAT_INST_ECOLE = [
                {'id': 'hg3v5', 'name': "École d'ingénieurs"},
                {'id': 'gnP1F', 'name': "École normale supérieure"},
                {'id': 'YHTyQ', 'name': "Ecole ou site relevant d'une école accréditée à délivrer un titre d'ingénieur diplômé"},
                {'id': 'bXxJY', 'name': "École de commerce et de management"},
                {'id': 'HDR9x', 'name': "Institut d'étude politique"},
                {'id': '93BR1', 'name': "Établissement supérieur d'architecture"},
                {'id': '7OtgP', 'name': 'Institut national polytechnique'},
                {'id': '9BZal', 'name': "École de formation artistique"},
                {'id': '3u0A2', 'name': "École française à l'étranger"},
                {'id': 'vt1Z4', 'name': "Institut ou école extérieur aux universités"},
                {'id': 'ss6r4', 'name': "Institut d’administration des entreprises"},
                {'id': 'AVhzK', 'name': "Institut de recherche technologique"},
                {'id': 's8Vm3', 'name': "Institut pour la transition énergétique"},
                {'id': 'lHssW', 'name': 'Établissement participant au COMP'},
                {'id': '95ius', 'name': 'Établissement de la 3eme vague de COMP'},
                {'id': 'dmk6m', 'name': "Établissement de la 2eme vague de COMP"},
                {'id': 'FAFNi', 'name': 'Établissement de la 1ère vague de COMP'},
                {'id': '2SZlU', 'name': "Principaux établissements d'enseignement supérieur"}
            ]
    CAT_SANTE = [
            {'id': 'bf4i6', 'name': 'Cancéropôle'},
            {'id': 'Jb7uE', 'name': 'Centre de lutte contre le cancer'},
            {'id': '1uQLb', 'name': 'Centre hospitalier'},
            {'id': 'habHA', 'name': 'Institut hospitalo-universitaire'}
            ]
    CAT_STRUCT = [{'id': 'z367d', 'name': 'Structure de recherche'}]
    CAT_OTHER = [
            {'id': '3Tv86', 'name': 'Organisation internationale'},
            {'id': 'mNJ1Z', 'name': 'Incubateur public'},
            {'id': 'xvai7', 'name': "Observatoire des sciences de l'Univers"},
            {'id': 'i4O23', 'name': "Observatoire"},
            {'id': '7vn7r', 'name': "Fondation de coopération scientifique"},
            {'id': '46iKf', 'name': "Administration publique"},
            {'id': 'rslqh', 'name': 'Institution publique (ROR)'},
            {'id': 'yrmd0', 'name': "École doctorale"},
            {'id': 'shaKH', 'name': "Institut universitaire de formation des maîtres"},
            {'id': '1dx1a', 'name': "Société d'accélération du transfert de technologies Expérimentale"},
            {'id': 'svJ2v', 'name': "Pôle de compétitivité"},
            {'id': '7w3QE', 'name': "Autre structure"}
            ]
    if not isinstance(elt.get('categories'), list):
        return {}
    for c in elt.get('categories', []):
        if c in [k['name'] for k in CAT_ETRANGER]:
            return {'typologie_1': 'Etablissements étrangers', 'typologie_2': c}
    for c in elt.get('categories', []):
        if c in [k['name'] for k in CAT_UNIV]:
            return {'typologie_1': 'Universités et assimilés', 'typologie_2': c}
    for c in elt.get('categories', []):
        if c in [k['name'] for k in CAT_ORGANISME]:
            return {'typologie_1': 'Organismes de recherche', 'typologie_2': c}
    for c in elt.get('categories', []):
        if c in [k['name'] for k in CAT_STRUCT]:
            return {'typologie_1': 'Structures de recherche', 'typologie_2': c}
    for c in elt.get('categories', []):
        if c in [k['name'] for k in CAT_ENTREPRISE]:
            return {'typologie_1': 'Entreprises', 'typologie_2': c}
    for c in elt.get('categories', []):
        if c in [k['name'] for k in CAT_SANTE]:
            return {'typologie_1': 'Etablissements de santé', 'typologie_2': c}
    for c in elt.get('categories', []):
        if c in [k['name'] for k in CAT_INFRA]:
            return {'typologie_1': 'Infrastructures de recherche', 'typologie_2': c}
    for c in elt.get('categories', []):
        if c in [k['name'] for k in CAT_INST_ECOLE]:
            return {'typologie_1': 'Ecoles, instituts et assimilés', 'typologie_2': c}
    for c in elt.get('categories', []):
        if c in [k['name'] for k in CAT_OTHER]:
            return {'typologie_1': 'Autres', 'typologie_2': c}
        return {'typologie_1': 'Autres', 'typologie_2': c}
    #logger.debug('no typo for')
    #logger.debug(elt['id'])
    #logger.debug(elt['categories'])

def test_typo():
    df = pd.read_json('/upw_data/scanr/orga_ref/paysage_formatted.jsonl', lines=True)
    data = df.to_dict(orient='records')
    for d in data:
        if d.get('is_main_parent'):
            get_typologie(d)

@retry(delay=100, tries=3, logger=logger)
def dump_full_paysage():
    logger.debug('### DUMP FULL Paysage data')
    response = requests.get(
        f'{PAYSAGE_URL}/dump/structures',
        headers={'x-api-key': PAYSAGE_API_KEY},
        stream=True
    )
    docs = []
    with gzip.open(response.raw, 'rt') as f:
        for line in f:
            doc = json.loads(line)
            docs.append(doc)
    logger.debug(f'{len(docs)} paysage elts retrieved')
    os.system(f'rm -rf /upw_data/scanr/orga_ref/paysage_dump.jsonl')
    to_jsonl(docs, f'/upw_data/scanr/orga_ref/paysage_dump.jsonl')
    os.system(f'cd /upw_data/scanr/orga_ref && rm -rf paysage_dump.jsonl.gz && gzip -k paysage_dump.jsonl')
    upload_object(container='scanr-data', source = f'/upw_data/scanr/orga_ref/paysage_dump.jsonl.gz', destination=f'production/paysage_dump.jsonl.gz')
    return docs

def get_paysage_data():
    df_paysage_id = get_ods_data('fr-esr-paysage_structures_identifiants')
    df_paysage_struct = get_ods_data('structures-de-paysage-v2')
    df_siren = df_paysage_id[df_paysage_id.id_type=='siret']
    df_siren['siren'] = df_siren.id_value.apply(lambda x:x[0:9])
    df_ror = df_paysage_id[df_paysage_id.id_type=='ror']
    return df_paysage_struct, df_siren, df_ror

def dump_paysage_data():
    logger.debug('### DUMP Paysage data')
    df_paysage_id = get_ods_data('fr-esr-paysage_structures_identifiants')
    df_paysage_struct = get_ods_data('structures-de-paysage-v2')
    df_web = get_ods_data('fr-esr-paysage_structures_websites')
    id_map, web_map = {}, {}
    sirens, sirets = [], []
    for e in df_paysage_id.to_dict(orient='records'):
        current_paysage = e['id_paysage']
        if current_paysage not in id_map:
            id_map[current_paysage] = []
        new_elt, new_elt_siren = {}, {}
        if e['id_type'] in ['ror', 'rnsr', 'siret', 'uai', 'ed', 'grid']:
            for f in ['id_value', 'id_type', 'active', 'id_startdate', 'id_enddate']:
                if e.get(f):
                    new_elt[f] = e[f]
                    if e['id_type'] == 'ed' and f=='id_value':
                        new_elt[f] = 'ED' + str(e[f])
        if new_elt and new_elt not in id_map[current_paysage]:
            id_map[current_paysage].append(new_elt)
            if e['id_type'] == 'siret':
                sirets.append(e['id_value'])
                sirens.append(e['id_value'][0:9])
                new_elt_siren = {'id_type': 'siren', 'id_value': new_elt['id_value'][0:9]}    
                id_map[current_paysage].append(new_elt_siren)
    for e in df_web.to_dict(orient='records'):
        current_paysage = e['id_structure_paysage']
        if current_paysage not in web_map:
            web_map[current_paysage] = []
        if e['type'] in ['website', 'hceres', 'Hal', 'BSO']:
            web_elt = {'url': e['url'], 'type': e['type']}
            if web_elt not in web_map[current_paysage]:
                web_map[current_paysage].append(web_elt)
    try:
        siren_info = pd.read_json('/upw_data/scanr/orga_ref/siren_info_for_paysage.jsonl', lines=True).to_dict(orient='records')
        logger.debug(f'{len(siren_info)} sirens info loaded')
    except:
        siren_info = format_siren(siren_list=sirens, siret_list=sirets, existing_siren=[])
        logger.debug(f'{len(siren_info)} sirens info downloaded')
        to_jsonl(siren_info, f'/upw_data/scanr/orga_ref/siren_info_for_paysage.jsonl')
    siret_map = {}
    for e in siren_info:
        for k in e.get('externalIds', []):
            if k.get('type')=='siret':
                siret_map[k['id']] = e
    data = []
    for e in df_paysage_struct.to_dict(orient='records'):
        current_paysage = e['id']
        external_ids = [{'id_type': 'paysage', 'id_value': current_paysage}]
        if current_paysage in id_map:
            external_ids += id_map[current_paysage]
        e['external_ids'] = external_ids
        for k in external_ids:
            if k.get('id_type')=="siret":
                current_siret = k['id_value']
                if current_siret in siret_map:
                    e['etablissementSiege'] = siret_map[current_siret]['etablissementSiege']
                else:
                    logger.debug(f'{current_siret} not in siret_map?')
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

def get_main_candidate_deprecated(candidates):
    if len(candidates)==1:
        return candidates[0]
    sieges = [c for c in candidates if c.get('etablissementSiege')==1.0]
    if len(sieges)==1:
        return sieges[0]
    new_candidates1 = sieges
    if len(sieges)==0:
        new_candidates1 = candidates
    actives = [c for c in new_candidates1 if c.get('structurestatus')=='active']
    if len(actives)==1:
        return actives[0]
    new_candidates2 = actives
    if len(new_candidates2)==0:
        new_candidates2 = new_candidates1 
    no_camp = [c for c in new_candidates2 if 'campus' not in c.get('usualname').lower()]
    if len(no_camp)==1:
        return no_camp[0]
    custom = [c for c in new_candidates2 if c.get('usualname').lower()in ['cesi', 'centrale lyon']]
    if len(custom)==1:
        return custom[0]
    return None

def get_main_candidate(candidates):
    if len(candidates)==1:
        return candidates[0]['main_id']
    # 1
    parents = [c for c in candidates if c.get('is_main_parent') is True]
    candidate_ids = list(set([c['main_id'] for c in parents]))
    if len(candidate_ids)==1:
        return candidate_ids[0]
    new_candidates1 = parents
    if len(parents)==0:
        new_candidates1 = candidates
    # 2
    sieges = [c for c in new_candidates1 if c.get('etablissementSiege')==1.0]
    candidate_ids = list(set([c['main_id'] for c in sieges]))
    if len(candidate_ids)==1:
        return candidate_ids[0]
    new_candidates2 = sieges
    if len(sieges)==0:
        new_candidates2 = new_candidates1
    # 3
    filtered = [c for c in new_candidates2 if c.get('category').get('id') in PAYSAGE_CAT_TO_KEEP]
    candidate_ids = list(set([c['main_id'] for c in filtered]))
    if len(candidate_ids)==1:
        return candidate_ids[0]
    new_candidates3 = filtered
    if len(filtered)==0:
        new_candidates3 = new_candidates2
    # 4
    actives = [c for c in new_candidates3 if c.get('status')=='active']
    candidate_ids = list(set([c['main_id'] for c in actives]))
    if len(candidate_ids)==1:
        return candidate_ids[0]
    return None

def get_main_id_paysage(current_id, corresp):
    current_id = current_id.strip()
    HARD_CODED = {'267500452': 'Py0K5'}
    if current_id in HARD_CODED:
        return HARD_CODED[current_id]
    if identifier_type(current_id) in ['rnsr']:
        return current_id
    if current_id not in corresp:
        #logger.debug(f"{current_id} not in corresp paysage !!")
        return current_id
    candidate = get_main_candidate(corresp[current_id])
    if candidate:
        return candidate
    if identifier_type(current_id) in ['ror']:
        return current_id
    logger.debug('no main id for :')
    logger.debug(current_id)
    logger.debug(corresp[current_id])
    return current_id

def get_main_id_paysage_deprecated(current_id, corresp):
    if identifier_type(current_id) in ['rnsr']:
        return current_id
    if current_id not in corresp:
        #logger.debug(f"{current_id} not in corresp paysage !!")
        return current_id
    candidates = get_main_candidate(corresp[current_id])
    if candidates:
        return candidates['main_id']
    for cat in ["Grand établissement relevant d'un autre département ministériel", "Grand établissement", "Communauté d'universités et établissements", 
            "Établissement public expérimental",
            'Université', "Organisme de recherche", 
            "Institut national polytechnique", "Institut ou école extérieur aux universités",
            "École normale supérieure", "Établissement d'enseignement supérieur privé d'intérêt général",
            "École d'ingénieurs",  "École de commerce et de management", "École nationale supérieure d'ingénieurs",
            "Institut d'étude politique",
            "Centre de lutte contre le cancer", "Institut hospitalo-universitaire", "Centre hospitalier",
            "Établissement privé d'enseignement universitaire", "Établissement d'enseignement supérieur privé rattachés à un EPSCP", 
            "Établissement d'enseignement supérieur étranger", "Institution étrangère active en matière de recherche et d'innovation"]:
        candidates = [k for k in corresp[current_id] if k.get('category_usualnamefr')==cat]
        candidates_filt = get_main_candidate(candidates)
        if candidates_filt:
            return candidates_filt['main_id']
    if identifier_type(current_id) in ['ror']:
        return current_id
    logger.debug('no main id for :')
    logger.debug(current_id)
    logger.debug(corresp[current_id])
    return current_id

def is_main_parent(elt):
    relations = elt.get('relations', [])
    for r in relations:
        if r.get('relationTag')=='structure-interne':
            if r.get('resourceId') != elt['id']:
                return False
    return True

def get_label(e):
    label = ''
    currentname = e.get('currentName')
    if isinstance(currentname.get('officialName'), str):
        label = currentname['officialName']
    if isinstance(currentname.get('usualName'), str):
        label = currentname['usualName']
    return label

def get_parent(elt_id, paysage_map):
    elt = paysage_map[elt_id]
    parents = []
    relations = elt.get('relations', [])
    for r in relations:
        if r.get('relationTag')=='structure-interne':
            if r.get('resourceId') != elt['id']:
                parents.append(r.get('resourceId')) 
    parents = list(set(parents))
    main_parents = []
    for p in parents:
        if p not in paysage_map:
            logger.debug(f"Data ISSUE in paysage as {p} not main_id - pb arise for {elt_id}")
            continue
        if is_main_parent(paysage_map[p]):
            main_parents.append(p)
        else:
            main_parents += get_parent(p, paysage_map)
    main_parents = list(set(main_parents))
    return main_parents


def get_correspondance_paysage():
    corresp = {}
    siret_map = get_siret_map()
    df_paysage = pd.read_json('/upw_data/scanr/orga_ref/paysage_dump.jsonl', lines=True)
    for e in df_paysage.to_dict(orient='records'):
        main_id = e['id']
        for k in e['identifiers']:
            if isinstance(k.get('value'), str) and k['type']=='siret' and k['value'] in siret_map:
                e['etablissementSiege'] = siret_map[k['value']]['etablissementSiege']
        for k in e['identifiers']:
            new_elt = {'main_id': main_id}
            for g in ['status', 'category', 'etablissementSiege']:
                new_elt[g] = e.get(g)
            new_elt['is_main_parent'] = is_main_parent(e)
            if isinstance(k.get('value'), str):
                current_ids = [k['value']]
                if k['type'] == 'ed':
                    current_ids.append('ED' + str(k['value']))
                if k['type'] == 'siret':
                    current_ids.append(k['value'][0:9])
                for current_id in current_ids:
                    elt_to_add = new_elt.copy()
                    elt_to_add['type'] = k['type']
                    elt_to_add['value'] = current_id
                    if current_id not in corresp:
                        corresp[current_id] = []
                    if elt_to_add not in corresp[current_id]:
                        corresp[current_id].append(elt_to_add)
    logger.debug(f'{len(corresp)} elts from paysage')
    return corresp

def get_correspondance_paysage_deprecated():
    corresp = {}
    df_paysage = pd.read_json('/upw_data/scanr/orga_ref/paysage.jsonl', lines=True)
    for e in df_paysage.to_dict(orient='records'):
        main_id = e['id']
        for k in e['external_ids']:
            k['main_id'] = main_id
            for g in ['category_usualnamefr', 'structurestatus', 'usualname']:
                if isinstance(e.get(g), str):
                    k[g] = e[g]
                if e.get('etablissementSiege') == e.get('etablissementSiege'):
                    k['etablissementSiege'] = e.get('etablissementSiege')
            if isinstance(k.get('id_value'), str):
                current_id = k['id_value']
                if current_id not in corresp:
                    corresp[current_id] = []
                if k not in corresp[current_id]:
                    corresp[current_id].append(k)
    logger.debug(f'{len(corresp)} elts from paysage')
    return corresp

def format_paysage(paysage_ids, sirens):
    logger.debug('formatting paysage data')
    df_paysage = pd.read_json('/upw_data/scanr/orga_ref/paysage_dump.jsonl', lines=True)
    paysage_data = df_paysage.to_dict(orient='records')
    paysage_map = {}
    for p in paysage_data:
        p_id = p['id']
        #p_id = p['id'].lower()
        if p_id in paysage_map:
            logger.debug(f'SHOULD NOT HAPPEN {p_id} already in paysage_map')
        paysage_map[p_id] = p
    extra_ids_to_extract = []
    for e in paysage_data:
        if e.get('category').get('id') in PAYSAGE_CAT_TO_KEEP:
            extra_ids_to_extract.append(e['id'])
    input_id_set = set(paysage_ids + extra_ids_to_extract)
    paysage_formatted = []
    for e in paysage_data:
        new_elt = {'id': e['id']}
        to_keep = False
        if e['id'] in input_id_set:
            to_keep = True
        new_elt['externalIds'] = [{'id': e['id'], 'type': 'paysage'}]
        for k in e['identifiers']:
            if isinstance(k.get('value'), str) and k['type'] in ['siret', 'uai', 'grid', 'ror', 'ed', 'rnsr', 'wikidata']:
                new_k = {'id': k['value'], 'type': k['type']}
                if k['type'] == 'ed':
                    new_k['id'] = 'ED'+str(k['value'])
                if k['id'][0:9] in sirens:
                    to_keep = True
            if new_k not in new_elt['externalIds']:
                new_elt['externalIds'].append(new_k)
        if to_keep is False:
            continue
        # startDate
        if isinstance(e.get('creationDate'), str):
            if len(e['creationDate'])==4:
                new_elt['startDate'] = e['creationDate']+'-01-01T00:00:00'
            elif len(e['creationDate'])==10:
                new_elt['startDate'] = e['creationDate']+'T00:00:00'
            elif len(e['creationDate'])==7:
                new_elt['startDate'] = e['creationDate']+'-01T00:00:00'
            else:
                print(e['creationDate'])
        if new_elt.get('startDate'):
            new_elt['creationYear'] = int(new_elt['startDate'][0:4])
        # status
        if e.get('status')=='active':
            new_elt['status'] = 'active'
        else:
            new_elt['status']='old'
        # endDate
        if isinstance(e.get('closureDate'), str):
            if len(e['closureDate'])==4:
                new_elt['endDate'] = e['closureDate']+'-01-01T00:00:00'
            elif len(e['closureDate'])==10:
                new_elt['endDate'] = e['closureDate']+'T00:00:00'
            elif len(e['closureDate'])==7:
                new_elt['endDate'] = e['closureDate']+'-01T00:00:00'
            else:
                print(e['closureDate'])
        # name
        currentname = e.get('currentName')
        if isinstance(currentname.get('officialName'), str):
            new_elt['label'] = {'default': currentname['officialName']}
        if isinstance(currentname.get('usualName'), str):
            new_elt['label'] = {'default': currentname['usualName']}
            new_elt['label'] = {'fr': currentname['usualName']}
        if isinstance(e.get('nameEn'), str):
            new_elt['label']['en'] = e['nameEn']
        new_elt['acronym'] = {}  
        if isinstance(currentname.get('shortName'), str):
            new_elt['acronym']['fr'] = currentname['shortName']
            new_elt['acronym']['default'] = currentname['shortName']
        if isinstance(currentname.get('acronymEn'), str):
            new_elt['acronym']['en'] = currentname['acronymEn']
            new_elt['acronym']['default'] = currentname['acronymEn']
        if isinstance(currentname.get('acronymFr'), str):
            new_elt['acronym']['fr'] = currentname['acronymFr']
            new_elt['acronym']['default'] = currentname['acronymFr']
        # kind
        if e.get('legalcategory').get('sector') == 'privé':
            new_elt['kind'] = ['Secteur privé'] 
        else:
            new_elt['kind'] = ['Secteur public'] 
        # level
        if e.get('legalcategory').get('longNameFr'):
            new_elt['level'] = e.get('legalcategory').get('longNameFr')
        if e.get('legalcategory').get('shortNameFr'):
            new_elt['level'] = e.get('legalcategory').get('shortNameFr')
        new_elt['description'] = {}
        if isinstance(e.get('descriptionFr'), str):
            new_elt['description']['fr'] = e.get('descriptionFr')
        if isinstance(e.get('descriptionEn'), str):
            new_elt['description']['en'] = e.get('descriptionEn')
        #address
        address = {'main': True}
        currentLoc = e.get('currentLocalisation')
        for f in ['country', 'city', 'address', 'iso3']:
            if isinstance(currentLoc.get(f), str):
                address[f] = currentLoc[f]
        if isinstance(currentLoc.get('postalCode'), str):
            address['postcode'] = currentLoc['postalCode']
        if address.get('city') is None and isinstance(currentLoc.get('locality'), str):
            address['city'] = currentLoc['locality']
        geom = currentLoc.get('geometry')
        if isinstance(geom, dict) and isinstance(geom.get('coordinates'), list):
            lat = currentLoc.get('geometry').get('coordinates')[1]
            lon = currentLoc.get('geometry').get('coordinates')[0]
            address['gps'] = {'lat': lat, 'lon': lon}
        else:
            pass
            #logger.debug(f"no gps for {e['id']}")
        new_elt['isFrench'] = False
        if currentLoc.get('iso3') == 'FRA':
            new_elt['isFrench'] = True
        new_elt['address'] = [address]
        links = []
        link_types = []
        if isinstance(e.get('websites'), list):
            for w in e['websites']:
                current_link = {'url': w['url'], 'type': w['type']}
                if w.get('language') == 'fr':
                    links.append(current_link)
                    link_types.append(w['type'])
            for w in e['websites']:
                current_link = {'url': w['url'], 'type': w['type']}
                if w['type'] not in link_types:
                    links.append(current_link)
                    link_types.append(w['type'])
        if isinstance(e.get('socialmedias'), list):
            for w in e['socialmedias']:
                current_link = {'url': w['account'], 'type': w['type']}
                links.append(current_link)
        new_elt['links'] = links
        new_elt['main_category'] = e.get('category', {}).get('usualNameFr')
        new_elt['categories'] = [c.get('usualNameFr') for c in e.get('categories', []) if c.get('usualNameFr')]
        # TODO institutions etc
        parents = get_parent(new_elt['id'], paysage_map)
        current_inst = []
        if parents:
            for p in parents:
                if p not in paysage_map:
                    logger.debug(f"Data ISSUE in paysage as {p} not main_id - pb arise for {new_elt['id']}")
                    continue
                current_p = paysage_map[p]
                label = get_label(current_p)
                new_rel = {'structure': p, 'label': label, "relationType": "établissement tutelle"}
                if new_rel not in current_inst:
                    current_inst.append(new_rel)
        new_elt['institutions'] = current_inst
        new_elt['is_main_parent'] = is_main_parent(e)
        paysage_formatted.append(new_elt)
    os.system(f'rm -rf /upw_data/scanr/orga_ref/paysage_formatted.jsonl')
    to_jsonl(paysage_formatted, f'/upw_data/scanr/orga_ref/paysage_formatted.jsonl')
    return paysage_formatted

def get_siret_map():
    try:
        siren_info = pd.read_json('/upw_data/scanr/orga_ref/siren_info_for_paysage.jsonl', lines=True).to_dict(orient='records')
        logger.debug(f'{len(siren_info)} sirens info loaded')
    except:
        siren_info = format_siren(siren_list=sirens, siret_list=sirets, existing_siren=[])
        logger.debug(f'{len(siren_info)} sirens info downloaded')
        os.system('rm -rf /upw_data/scanr/orga_ref/siren_info_for_paysage.jsonl')
        to_jsonl(siren_info, f'/upw_data/scanr/orga_ref/siren_info_for_paysage.jsonl')
    siret_map = {}
    for e in siren_info:
        for k in e.get('externalIds', []):
            if k.get('type')=='siret':
                siret_map[k['id']] = e
    return siret_map
