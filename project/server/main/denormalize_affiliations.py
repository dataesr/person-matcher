import pandas as pd
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def get_main_address(address):
    main_add = None
    for add in address:
        if add.get('main', '') is True:
            main_add = add.copy()
            break
    if main_add:
        for f in ['main', 'citycode', 'urbanUnitCode', 'urbanUnitLabel', 'localisationSuggestions', 'provider', 'score']:
            if main_add.get(f):
                del main_add[f]
    return main_add

def get_orga_data():
    url = 'https://scanr-data.s3.gra.io.cloud.ovh.net/production/organizations.jsonl.gz'
    df = pd.read_json(url, lines=True)
    #df = df.set_index('id')
    data = df.to_dict(orient='records')
    orga_map = {}
    for elt in data:
        res = {}
        #for e in ['id', 'kind', 'label', 'acronym', 'nature', 'status', 'isFrench', 'address']:
        for e in ['id', 'kind', 'label', 'acronym', 'isFrench', 'status']:
            if elt.get(e):
                res[e] = elt[e]
            if isinstance(elt.get('address'), list):
                res['mainAddress'] = get_main_address(elt['address'])
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
