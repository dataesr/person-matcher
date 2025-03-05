import requests
import pandas as pd
import os
from project.server.main.logger import get_logger

logger = get_logger(__name__)

ODS_API_KEY = os.getenv('ODS_API_KEY')

def get_ods_data(key):
    logger.debug(f'getting ods data {key}')
    current_df = pd.read_csv(f'https://data.enseignementsup-recherche.gouv.fr/explore/dataset/{key}/download/?format=csv&apikey={ODS_API_KEY}', sep=';')
    return current_df

def get_agreements():
    agreements_dict = {}
    for f in ['organismes-prives-agrees-credit-dimpot-innovation-cii',
              'fr-esr-cico-organismes-publics-agrees-ci-collaboration-de-recherche',
              'organismes-experts-bureaux-2-style-stylistes-agrees-credit-d-impot-recherche-cir']:


        current_df = get_ods_data(f)
        for row in current_df.itertuples():
            if row.siren==row.siren:
                siren = str(int(row.siren))
                if siren not in agreements_dict:
                    agreements_dict[siren] = []
                agreements_dict[siren].append({'type': row.dispositif,
                                              'start': int(row.debut), 'end':int(row.fin),
                                               'years': [int(y) for y in row.annees.split(';')],
                                             'label': row.type_lib})
    return agreements_dict

def get_awards():
    awards_dict = {}
    df_ilab = get_ods_data('fr-esr-laureats-concours-national-i-lab')

    for row in df_ilab.itertuples():
        if row.ndeg_siren == row.ndeg_siren:
            siren = str(int(row.ndeg_siren))
            if siren not in awards_dict:
                awards_dict[siren] = []
            awards_dict[siren].append({
                'label': 'Lauréats I-LAB',
                'year': int(row.annee_de_concours),
                'summary': row.resume,
                'domain': {'id': row.domainetechnoid , 'label': row.domaine_technologique}
            })
    df_prix = get_ods_data('fr_esr_paysage_laureats_all')
    for row in df_prix[df_prix.laureat_type=='Structure'].itertuples():
        current_ids = []
        if row.laureat_identifiant_rnsr==row.laureat_identifiant_rnsr:
            current_ids = str(row.laureat_identifiant_rnsr).split(';')
        if row.laureat_identifiant_siret == row.laureat_identifiant_siret:
            current_ids = [c[0:9] for c in str(row.laureat_identifiant_siret).split(';')]
            #current_id = str(int(row.laureat_identifiant_siret))[0:9]
        year = None
        try:
            year = int(str(row.prix_annee)[0:4])
        except:
            logger.debug(f'prix_annee parsing failed for {row.prix_annee}')
        for current_id in current_ids:
            if current_id not in awards_dict:
               awards_dict[current_id] = []
            awards_dict[current_id].append({
                'label': row.prix_libelle,
                'year': year
            })
    return awards_dict

