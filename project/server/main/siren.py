import pandas as pd
import requests
import numpy as np
import datetime
from io import BytesIO
from pyproj import Transformer
from project.server.main.ods import get_ods_data
from project.server.main.logger import get_logger

logger = get_logger(__name__)

MOUNTED_VOLUME = '/upw_data/'

# URLs Parquet officiels
UNITE_LEGALE_URL = "https://www.data.gouv.fr/api/1/datasets/r/350182c9-148a-46e0-8389-76c2ec1374a3"
ETABLISSEMENT_URL = "https://www.data.gouv.fr/api/1/datasets/r/a29c1297-1f92-4e2a-8f6b-8c902ce96c5f"
UNITE_LEGALE_HISTO_URL = "https://www.data.gouv.fr/api/1/datasets/r/1b9290ed-d0bc-461f-ba31-0250a99cc140"

# Colonnes utiles après correction
UL_COLS = [
    "siren",
    "denominationUniteLegale",
    "categorieJuridiqueUniteLegale",
    "dateDebut",
    "dateCreationUniteLegale",
    "etatAdministratifUniteLegale",
    "activitePrincipaleUniteLegale",
    "trancheEffectifsUniteLegale"
]

ET_COLS = [
    "siret",
    "siren",
    "etablissementSiege",
    "denominationUsuelleEtablissement",
    "dateDebut",
    "numeroVoieEtablissement",
    "typeVoieEtablissement",
    "libelleVoieEtablissement",
    "codePostalEtablissement",
    "libelleCommuneEtablissement",
    "coordonneeLambertAbscisseEtablissement",
    "coordonneeLambertOrdonneeEtablissement"
]

UL_HISTO_COLS = [
        "siren", "dateDebut", "dateFin", "denominationUniteLegale"]

# Fonction pour lire parquet depuis URL
def read_parquet_from_url(url, columns=None):
    print(f"Téléchargement depuis {url} ...")
    r = requests.get(url, stream=True)
    r.raise_for_status()
    return pd.read_parquet(BytesIO(r.content), columns=columns)

def get_ul_histo():
    df_ul = read_parquet_from_url(UNITE_LEGALE_HISTO_URL, columns=UL_HISTO_COLS)
    return df_ul

def get_etab():
    df_et = read_parquet_from_url(ETABLISSEMENT_URL, columns=ET_COLS)
    return df_et
  
def get_ul():
    df_ul = read_parquet_from_url(UNITE_LEGALE_URL, columns=UL_COLS)
    return df_ul

transformer = Transformer.from_crs(
    "EPSG:2154",  # Lambert-93
    "EPSG:4326",  # WGS84
    always_xy=True
)

def get_lat_lon(df):
    # Conversion en numérique, valeurs invalides -> NaN
    x = pd.to_numeric(
        df["coordonneeLambertAbscisseEtablissement"],
        errors="coerce"
    )
    y = pd.to_numeric(
        df["coordonneeLambertOrdonneeEtablissement"],
        errors="coerce"
    )

    # Initialisation des colonnes résultat
    df["lon"] = np.nan
    df["lat"] = np.nan

    # Masque des lignes exploitables
    mask = x.notna() & y.notna()

    # Transformation uniquement sur les lignes valides
    df.loc[mask, "lon"], df.loc[mask, "lat"] = transformer.transform(
        x[mask].values,
        y[mask].values
    )

    return df

def format_siren(siren_list, siret_list, existing_siren):
    existing_siren_set = set(existing_siren)
    sirens = siren_list + [a[0:9] for a in siret_list]
    sirens = list(set(sirens))

    df_ul = get_ul()
    df_et = get_etab()
    df_ul_filtered = df_ul[(df_ul["siren"].isin(sirens))]
    df_et_filtered_siege = df_et[(df_et["siren"].isin(sirens)) & (df_et['etablissementSiege'])]
    df_et_filtered_not_siege = df_et[(df_et["siret"].isin(siret_list)) & (df_et['etablissementSiege']==False)]

    all_et = pd.concat([df_et_filtered_siege, df_et_filtered_not_siege])
    all_et = get_lat_lon(all_et)
    all_et = pd.merge(all_et, df_ul_filtered, on='siren')
    sirene_formatted = []
    for e in all_et.to_dict(orient='records'):
        main_id = e['siren']
        if main_id in existing_siren_set:
            continue
        if e['etablissementSiege'] is False:
            main_id = e['siret']
        new_elt = {'id': main_id}
        new_elt['externalIds'] = [{'id': e['siren'], 'type': 'siren'}]
        if e['etablissementSiege'] is False:
            new_elt['externalIds'].append({'id': e['siret'], 'type': 'siret'})
        # startDate
        if isinstance(e.get('dateCreationUniteLegale'), datetime.date):
            new_elt['startDate'] = str(e['dateCreationUniteLegale'])+'T00:00:00'
        if new_elt.get('startDate'):
            new_elt['creationYear'] = int(new_elt['startDate'][0:4])
        # status
        new_elt['status']='active'
        # name
        if isinstance(e.get('denominationUniteLegale'), str):
            new_elt['label'] = {'default': e['denominationUniteLegale']}
            if e['etablissementSiege'] is False:
                new_elt['label'] = {'default': e['denominationUsuelleEtablissement']}
        #address
        address = {'main': True}
        if isinstance(e.get('libelleCommuneEtablissement'), str):
            address['city'] = e['libelleCommuneEtablissement']
        full_add = f"{e['numeroVoieEtablissement']} {e['typeVoieEtablissement']} {e['libelleVoieEtablissement']}".strip()
        if full_add:
            address['address'] = full_add
        if isinstance(e.get('lat'), float) and isinstance(e.get('lon'), float):
            address['gps'] = {'lat': e['lat'], 'lon': e['lon']}
        new_elt['address'] = [address]
        sirene_formatted.append(new_elt)
    return sirene_formatted
