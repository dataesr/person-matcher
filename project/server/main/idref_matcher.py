import requests
from bs4 import BeautifulSoup

from project.server.main.strings import normalize, remove_parenthesis, str_footprint
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def name_idref_match(first_name, last_name, full_name):
    
    first_name_normalized_filtered, last_name_normalized, full_name_normalized = None, None, None
    
    if first_name and last_name:
        first_name_normalized_filtered = normalize(first_name, remove_space=False, min_length=3)
        first_name_normalized = normalize(first_name, remove_space=False, min_length=0)
        last_name_normalized = normalize(last_name, remove_space=False, min_length=0)
    elif full_name:
        full_name_normalized = normalize(full_name, remove_space=False, min_length=0)
    
    url = None
    
    if first_name_normalized_filtered and last_name_normalized:
        name_normalized = f'{first_name_normalized} {last_name_normalized}'
        searched_name = ' AND '.join(name_normalized.split(' '))
        # attention persname_t
        url = f'https://www.idref.fr/Sru/Solr?q=persname_t:({searched_name})%20AND%20recordtype_z:a&rows=100'
    elif last_name_normalized:
        name_normalized = f'{last_name_normalized}'
        searched_name = ' AND '.join(name_normalized.split(' '))
        # attention nom_t
        url = f'https://www.idref.fr/Sru/Solr?q=nom_t:({searched_name})%20AND%20recordtype_z:a&rows=100'
    elif full_name_normalized:
        name_normalized = full_name_normalized
        searched_name = ' AND '.join(name_normalized.split(' '))
        # attention persname_t
        url = f'https://www.idref.fr/Sru/Solr?q=persname_t:({searched_name})%20AND%20recordtype_z:a&rows=100'
    
    if url:
        xml_response = requests.get(url).text
        matching_idrefs = filter_xml_response(xml_response, first_name, last_name, full_name)
        if len(matching_idrefs) == 1:
            return matching_idrefs[0]
        else:
            print(f'plusieurs / pas de correspondances dans idref : {matching_idrefs}')

def analyze_doc(doc, first_name_normalized, last_name_normalized, full_name_normalized):
    idref = None
     
    ppn_elt = doc.find('str', {'name': 'ppn_z'})
    if ppn_elt:
        idref = ppn_elt.get_text()
    if idref is None:
        return
    
    bestprenom_long = False
    bestprenoms = []
    bestprenoms_elt = doc.find('arr', {'name': 'bestprenom_s'})
    if bestprenoms_elt:
        bestprenoms = [normalize(p.get_text(), remove_space=False) for p in bestprenoms_elt.find_all('str')]
    for p in bestprenoms:
        if len(p)>3:
            bestprenom_long = True
    if bestprenom_long is False:
        return 
    
    match_fullname = False
    if full_name_normalized:
        fullnames = []
        fullname_elt = doc.find('arr', {'name': 'persname_s'})
        if fullname_elt:
            fullnames = [str_footprint(normalize(remove_parenthesis(p.get_text()), remove_space=False)) for p in fullname_elt.find_all('str')]
        for p in fullnames:
            if p == str_footprint(full_name_normalized):
                match_fullname = True
                
    match_prenom = False
    if first_name_normalized:
        match_prenom_initiale = False
        prenoms = []
        prenoms_elt = doc.find('arr', {'name': 'prenom_s'})
        if prenoms_elt:
            prenoms = [normalize(p.get_text(), remove_space=False) for p in prenoms_elt.find_all('str')]
        for p in prenoms:
            if p and first_name_normalized and isinstance(p, str) and isinstance(first_name_normalized, str):
                if p[0] == first_name_normalized[0]:
                    match_prenom_initiale = True
                if p == first_name_normalized:
                    match_prenom = True

    match_nom = False  
    if last_name_normalized:
        noms = []
        noms_elt = doc.find('arr', {'name': 'nom_s'})
        if noms_elt:
            noms = [normalize(p.get_text(), remove_space=False) for p in noms_elt.find_all('str')]
        for p in noms:
            if p == last_name_normalized:
                match_nom = True
                break

    # ok par défaut (notamment si vide), mais rejet si date de naissance avant 1900
    match_naissance = True
    try:
        siecle_naissance = 0
        for f in ['datenaissance_dt', 'datemort_dt', 'anneemort_dt', 'anneenaissance_dt']:
            naissance_elt = doc.find('date', {'name': f})
            if naissance_elt:
                siecle_naissance = str(naissance_elt.get_text()[0:2])
                if siecle_naissance not in ['19', '20']:
                    match_naissance = False
    except:
        pass
    
    if first_name_normalized and last_name_normalized:
        if bestprenom_long and match_prenom and match_nom and match_naissance:
            return {'id': f'idref{idref}', 'method': 'last_name/first_name/birth_date'}
        if bestprenom_long and len(first_name_normalized) < 3 and match_prenom_initiale and match_nom and match_naissance:
            return {'id': f'idref{idref}', 'method': 'last_name/initial/birth_date'}
    elif full_name_normalized:
        if bestprenom_long and match_fullname and match_naissance:
            return {'id': f'idref{idref}', 'method': 'full_name/birth_date'}
        
    return 
    

def filter_xml_response(xml_response, first_name, last_name, full_name):
    first_name_normalized = normalize(first_name, remove_space=False)
    last_name_normalized = normalize(last_name, remove_space=False)
    full_name_normalized = normalize(full_name, remove_space=False)
    soup = BeautifulSoup(xml_response, 'lxml')
    num_found = 0
    result = soup.find('result')
    if result:
        num_found = int(result.attrs['numfound'])
    threshold = 50
    if num_found > threshold:
        logger.debug(f"more than {threshold} results for search {first_name_normalized} {last_name_normalized} {full_name_normalized}")
        return []
    elif num_found < 1:
        logger.debug(f"no result for search {first_name_normalized} {last_name_normalized} {full_name_normalized}")
        return []
    
    matching_idrefs = []
    
    for doc in soup.find_all('doc'):
        idref = analyze_doc(doc, first_name_normalized, last_name_normalized, full_name_normalized)
        if idref:
            matching_idrefs.append(idref)
            
    return matching_idrefs
