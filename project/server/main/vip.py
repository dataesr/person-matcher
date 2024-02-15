import pandas as pd
import pickle
from SPARQLWrapper import SPARQLWrapper, JSON
import requests
import os
from urllib.parse import quote_plus
from retry import retry

from project.server.main.utils import to_jsonl
from project.server.main.logger import get_logger

logger = get_logger(__name__)

sparql = SPARQLWrapper("https://data.idref.fr/sparql")

ODS_API_KEY = os.getenv('ODS_API_KEY')

QUERY_START = """
SELECT ?idref ?firstName ?lastName ?ext_id
WHERE {?idref a foaf:Person ; foaf:familyName ?lastName. 
  ?idref a foaf:Person ; foaf:givenName ?firstName.
  ?idref owl:sameAs ?ext_id.
FILTER (STRSTARTS(STR(?ext_id),
"""

def get_matches(uri_prefix):
    QUERY_END = f"'{uri_prefix}'))" + "}"
    query = QUERY_START+QUERY_END
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    result = sparql.query().convert()
    return result['results']['bindings']

@retry(delay=100, tries=10, logger=logger)
def get_data_from_idref():
    data_orcid = []
    orcid_matches = get_matches('https://orcid.org')
    for r in orcid_matches:
        idref = r['idref']['value'].split('/')[3]
        orcid = r['ext_id']['value'].split('/')[3].split('#')[0]
        firstName = r['firstName']['value']
        lastName = r['lastName']['value']
        if orcid[0:2] != '00':
            continue
        data_orcid.append({'idref': idref, 'orcid': orcid, 'firstName': firstName, 'lastName': lastName})
    #logger.debug(f'correspondance idref - orcid : {len(data_orcid)}')

    data_id_hal = []
    id_hal_matches = get_matches('https://data.archives-ouvertes.fr')
    for r in id_hal_matches:
        idref = r['idref']['value'].split('/')[3]
        id_hal_s = r['ext_id']['value'].split('/')[4].split('#')[0]
        firstName = r['firstName']['value']
        lastName = r['lastName']['value']
        data_id_hal.append({'idref': idref, 'id_hal_s': id_hal_s, 'firstName': firstName, 'lastName': lastName})
    #logger.debug(f'correspondance idref - id_hal : {len(data_id_hal)}')
    df = pd.merge(pd.DataFrame(data_orcid), pd.DataFrame(data_id_hal), on=['idref', 'firstName', 'lastName'], how='outer')
    
    df['idref_abes'] = df['idref']
    df['id_hal_abes'] = df['id_hal_s']
    idref_dict = {}
    for row in df.itertuples():
        idref_dict[str(row.idref)] = {'idref': row.idref, 'lastName':row.lastName, 'firstName':row.firstName}
        if row.orcid==row.orcid:
            idref_dict[str(row.idref)]['orcid'] = str(row.orcid)
        if row.id_hal_s == row.id_hal_s:
            idref_dict[str(row.idref)]['id_hal'] = str(row.id_hal_s)
    return idref_dict

@retry(delay=100, tries=10, logger=logger)
def get_aurehal(aurehal_type):
    #logger.debug(f'start {aurehal_type} aurehal')
    nb_rows = 10000
    cursor='*'
    data = []
    while True:
        url = f'https://api.archives-ouvertes.fr/ref/{aurehal_type}/?q=orcidId_s:*&wt=json&fl=orcidId_s,idrefId_s,idHal_s&sort=docid asc&rows={nb_rows}&cursorMark={cursor}'
        r = requests.get(url)
        res = r.json()
        new_cursor = quote_plus(res['nextCursorMark'])
        data += res['response']['docs']
        if new_cursor == cursor:
            break
        cursor = new_cursor
    #logger.debug(f'end {aurehal_type} aurehal')
    return data

def get_vip():
    idref_dict = get_data_from_idref()
    aurehal_data = get_aurehal('author')
    for d in aurehal_data:
        if not isinstance(d.get('idrefId_s'), list):
            continue
        if len(d['idrefId_s'])!=1:
            continue
        idref = d['idrefId_s'][0].replace('https://www.idref.fr/', '').strip()
        if idref not in idref_dict:
            idref_dict[idref] = {'idref': idref}
        if d.get('idHal_s'):
            current_id_hal = d['idHal_s'].strip()
            if 'id_hal' not in idref_dict[idref]:
                idref_dict[idref]['id_hal'] = current_id_hal
            if 'id_hal' in idref_dict[idref] and idref_dict[idref]['id_hal'] != current_id_hal:
                print(f"mismatch;id_hal;{idref};{idref_dict[idref]['id_hal']};{current_id_hal}")
                idref_dict[idref]['id_hal'] = current_id_hal
        if isinstance(d.get('orcidId_s'), list) and len(d['orcidId_s']) == 1:
            current_orcid = d['orcidId_s'][0].replace('https://orcid.org/', '').strip()
            if 'orcid' not in idref_dict[idref]:
                idref_dict[idref]['orcid'] = current_orcid
            if 'orcid' in idref_dict[idref] and idref_dict[idref]['orcid'] != current_orcid:
                print(f"mismatch;orcid;{idref};{idref_dict[idref]['orcid']};{current_orcid}")
                del idref_dict[idref]['orcid']

    awardsr = pd.read_csv(f'https://data.enseignementsup-recherche.gouv.fr/explore/dataset/fr_esr_paysage_laureats_all/download/?format=csv&apikey={ODS_API_KEY}', sep=';')
    #iphdr = pd.read_csv(f'https://data.enseignementsup-recherche.gouv.fr/explore/dataset/fr-esr-laureats-concours-i-phd/download/?format=csv&apikey={ODS_API_KEY}', sep=';')
    for row in awardsr.itertuples():
        idref= row.laureat_identifiant_idref
        if idref==idref:
            idref=idref
        else:
            continue
        current_prize = {'prize_name': row.prix_libelle}
        if row.prix_porteurs_libelle==row.prix_porteurs_libelle:
            current_prize['prize_structure'] = row.prix_porteurs_libelle
        if row.prix_site_internet==row.prix_site_internet:
            current_prize['prize_url'] = row.prix_site_internet
        if len(str(row.prix_annee))==4:
            current_prize['prize_date'] = str(row.prix_annee)+'-01-01T00:00:00'
        if idref not in idref_dict:
            idref_dict[idref] = {'idref': idref, 'lastName':row.laureat_personne_nom, 'firstName':row.laureat_personne_prenom}
        if 'prizes' not in idref_dict[idref]:
            idref_dict[idref]['prizes'] = []
        if current_prize not in idref_dict[idref]['prizes']:
            idref_dict[idref]['prizes'].append(current_prize)
    to_jsonl(list(idref_dict.values()), '/upw_data/vip.jsonl', 'w')
    return idref_dict
