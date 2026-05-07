import re
import unicodedata
import pandas as pd
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def normalize(text):
    text = text.upper().replace('-', ' ').replace('_', ' ').replace("'", ' ')
    text = text.replace('  ', ' ')
    text = ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )
    return re.sub(r'\s+', ' ', text)

df_city = pd.read_csv('cities_with_postal.csv')
df_city['city_norm'] = df_city.city.apply(lambda x:normalize(x))
cities = df_city['city'].to_list()
cities_norm, postal_city_match = {}, {}
for e in df_city.to_dict(orient='records'):
    city_normalized = normalize(e['city'])
    cities_norm[city_normalized] = e['city']
    try:
        postal_city_match[city_normalized] = str(int(e['postal']))[0:2].zfill(2)
    except:
        logger.debug(f"no code in file for {city_normalized}")
#cities_norm = {normalize(city): city for city in cities}

def get_postal_code(text_norm):
    # Extraction code postal
    candidates = []
    candidate = None
    for match in re.finditer(r'\b\d{5}\b', text_norm):
        start = match.start()
        # Vérifier ce qui précède (ignorer si précédé de lettres collées)
        preceded_by_letters = start > 0 and re.search(r'[A-Za-z]\s{0,2}$', text_norm[:start])
        # Vérifier si c'est un numéro de rue (suivi d'un nom de rue)
        followed_by_street = re.search(r'^\s+(rue|route|avenue|boulevard|allee|impasse|chemin|voie|place|square|passage|domaine|hameau|lieu[- ]dit)\b', text_norm[match.end():], re.IGNORECASE)
        candidate = match
        if not preceded_by_letters and not followed_by_street:
            candidates.append(candidate)
    if candidate and len(candidates)==0:
        candidates.append(candidate)
    if candidates:
        return candidates[-1]
    return None

def parse_address(text):
    text_norm = normalize(text)
    text_norm = text_norm.replace(' MT ST ', ' MONT ST ')
    text_norm = re.sub(r'\bST\b', 'SAINT', text_norm)
    # Normalisation des villes

    # Extraction code postal
    #postcode_match = re.search(r'\b\d{5}\b', text_norm)
    postcode_match = get_postal_code(text_norm)
    postcode = postcode_match.group() if postcode_match else None

    # Zone de recherche prioritaire = après code postal
    search_zone = text_norm[postcode_match.end():] if postcode_match else text_norm

    city_found = None

    # On cherche la plus longue correspondance
    for city_norm in sorted(cities_norm.keys(), key=len, reverse=True):
        if re.search(r'\b' + re.escape(city_norm) + r'\b', search_zone):
        #if city_norm in search_zone:
            city_found = cities_norm[city_norm]
            break
            
    if city_found is None:
        for city_norm in sorted(cities_norm.keys(), key=len, reverse=True):
            if re.search(r'\b' + re.escape(city_norm) + r'\b', text_norm):
            #if city_norm in text_norm:
                city_found = cities_norm[city_norm]
                break

    # Nettoyage adresse
    address = text_norm
    if city_found:
        address = address.replace(normalize(city_found), '')
        if normalize(city_found) in postal_city_match:
            postal_start = postal_city_match[normalize(city_found)]
            if postcode is None:
                logger.debug(f"new postcode found {postcode} for city {city_found} | expect {postal_start} in address {text}")
                pass
            elif postal_start != postcode[0:2]:
                #logger.debug(f"erreur postcode found {postcode} for city {city_found} | expect {postal_start} in address {text}")
                pass
            # using the new postcode ('default 000 suffix)
            postcode = postal_start+'000'
    if postcode:
        address = address.replace(postcode, '')

    address = re.sub(r'\bCEDEX\b.*', '', address)  # retire CEDEX
    address = re.sub(r'[ ,]+', ' ', address).strip()

    return {
        'main': True,
        'address': text,
        'address_detected': address.title(),
        'city': city_found,
        'postcode': postcode
    }
