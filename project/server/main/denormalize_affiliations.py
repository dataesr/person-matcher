import pandas as pd
import requests
from project.server.main.utils import chunks, to_jsonl, to_json, orga_with_ed
from project.server.main.logger import get_logger

logger = get_logger(__name__)


def get_correspondance():
    url = 'https://scanr-data.s3.gra.io.cloud.ovh.net/production/organizations.jsonl.gz'
    df = pd.read_json(url, lines=True)
    #df = df.set_index('id')
    data = df.to_dict(orient='records')
    correspondance = {}
    raw_rnsrs = data
    for r in raw_rnsrs:
        current_id = None
        externalIdsToKeep = [e for e in r.get('externalIds', []) if e['type'] in ['rnsr',  'ror', 'grid', 'bce', 'sirene', 'siren', 'siret'] ]
        for e in externalIdsToKeep:
            current_id = e['id']
            if current_id not in correspondance:
                correspondance[current_id] = []
        if current_id is None:
            continue

        correspondance[current_id] += [k for k in externalIdsToKeep if k['id'] != current_id]
        for e in r.get('externalIds', []):
            if e['type'] in ['siren', 'siret', 'sirene', 'bce']:
                new_id = e['id']
                correspondance[new_id] += [k for k in externalIdsToKeep if k['id'] != new_id]

        for e in r.get('institutions'):
            if e.get('structure'):
                if isinstance(e.get('relationType'), str) and 'tutelle' in e['relationType'].lower():
                    elt = {'id': e['structure'], 'type': 'siren'}
                    if elt not in correspondance[current_id]:
                        correspondance[current_id].append(elt)
    logger.debug(f'{len(correspondance)} ids loaded with equivalent ids')
    return correspondance

def get_main_address(address):
    main_add = None
    if not isinstance(address, list):
        return main_add
    for add in address:
        if add.get('main', '') is True:
            main_add = add.copy()
            break
    if main_add:
        for f in ['main', 'citycode', 'urbanUnitCode', 'urbanUnitLabel', 'provider', 'score']:
            if main_add.get(f):
                del main_add[f]
    return main_add

def get_name_by_lang(e, lang):
    assert(lang in ['fr', 'en'])
    if not isinstance(e, dict):
        return None
    if isinstance(e.get(lang), str):
        return e[lang]
    return None

def get_default_name(e):
    if not isinstance(e, dict):
        return None
    for f in ['default', 'en', 'fr']:
        if isinstance(e.get(f), str):
            return e[f]
    return None

def compute_is_french(elt_id, mainAddress):
    isFrench = True
    if 'grid' in elt_id or 'ror' in elt_id:
        isFrench = False
        if isinstance(mainAddress, dict) and isinstance(mainAddress.get('country'), str) and mainAddress['country'].lower().strip() == 'france':
            isFrench = True
    return isFrench

def get_orga_data():
    data = orga_with_ed()
    orga_map = {}
    for elt in data:
        res = {}
        #for e in ['id', 'kind', 'label', 'acronym', 'nature', 'status', 'isFrench', 'address']:
        for e in ['id', 'kind', 'label', 'acronym', 'status']:
            if elt.get(e):
                res[e] = elt[e]
            if isinstance(elt.get('address'), list):
                res['mainAddress'] = get_main_address(elt['address'])
        res['isFrench'] = compute_is_french(elt['id'], res.get('mainAddress'))
        if 'label' in res:
            fr_label = get_name_by_lang(res['label'], 'fr')
            en_label = get_name_by_lang(res['label'], 'en')
            default_label = get_default_name(res['label'])
            encoded_labels = []
            if fr_label:
                encoded_labels.append('FR_'+fr_label)
            if en_label:
                encoded_labels.append('EN_'+en_label)
            encoded_label = '|||'.join(encoded_labels)
            if len(encoded_labels)==0 and default_label:
                encoded_label = 'DEFAULT_' + default_label
            res['id_name'] = f"{elt['id']}###{encoded_label}"
        orga_map[elt['id']] = res
    return orga_map

def get_orga(orga_map, orga_id):
    if orga_id in orga_map:
        return orga_map[orga_id]
    return {'id': orga_id}

def get_projects_data():
    url = 'https://scanr-data.s3.gra.io.cloud.ovh.net/production/projects.jsonl.gz'
    df = pd.read_json(url, lines=True)
    data = df.to_dict(orient='records')
    proj_map = {}
    for elt in data:
        res = {}
        for e in ['id', 'label', 'acronym', 'type', 'year']:
            if elt.get(e):
                res[e] = elt[e]
        proj_map[elt['id']] = res
    return proj_map

def get_link_orga_projects():
    url = 'https://scanr-data.s3.gra.io.cloud.ovh.net/production/projects.jsonl.gz'
    df = pd.read_json(url, lines=True)
    data = df.to_dict(orient='records')
    proj_map = {}
    for elt in data:
        res = {}
        for e in ['id', 'label', 'acronym', 'type', 'year']:
            if elt.get(e):
                res[e] = elt[e]
        proj_map[elt['id']] = res
    map_orga_proj = {}
    for proj in data:
        proj_id = proj['id']
        for part in proj.get('participants'):
            if part.get('structure'):
                orga_id = part['structure']
                if orga_id not in map_orga_proj:
                    map_orga_proj[orga_id] = []
                current_proj = proj_map[proj_id]
                map_orga_proj[orga_id].append(current_proj)
    return map_orga_proj

def get_project_from_orga(map_orga_proj, orga_id):
    if orga_id in map_orga_proj:
        return map_orga_proj[orga_id]
    return []

def get_project(proj_map, proj_id):
    if proj_id in proj_map:
        return proj_map[proj_id]
    return {'id': proj_id}
