import re
import unicodedata
import pandas as pd

cities = pd.read_csv('cities.csv')['city'].to_list()
def normalize(text):
    text = text.upper().replace('-', ' ').replace('_', ' ')
    text = ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )
    return re.sub(r'\s+', ' ', text)

def parse_address(text):
    text_norm = normalize(text)

    # Normalisation des villes
    cities_norm = {normalize(city): city for city in cities}

    # Extraction code postal
    postcode_match = re.search(r'\b\d{5}\b', text_norm)
    postcode = postcode_match.group() if postcode_match else None

    # Zone de recherche prioritaire = après code postal
    search_zone = text_norm[postcode_match.end():] if postcode_match else text_norm

    city_found = None

    # On cherche la plus longue correspondance
    for city_norm in sorted(cities_norm.keys(), key=len, reverse=True):
        if city_norm in search_zone:
            city_found = cities_norm[city_norm]
            break
            
    if city_found is None:
        for city_norm in sorted(cities_norm.keys(), key=len, reverse=True):
            if city_norm in text_norm:
                city_found = cities_norm[city_norm]
                break

    # Nettoyage adresse
    address = text_norm
    if postcode:
        address = address.replace(postcode, '')
    if city_found:
        address = address.replace(normalize(city_found), '')

    address = re.sub(r'\bCEDEX\b.*', '', address)  # retire CEDEX
    address = re.sub(r'[ ,]+', ' ', address).strip()

    return {
        'main': True,
        'address': text,
        'address_detected': address.title(),
        'city': city_found,
        'postcode': postcode
    }
