from project.server.main.logger import get_logger

logger = get_logger(__name__)

def get_region(code_postal):
    """Retourne la région française à partir du code postal (France métropolitaine et DROM)"""
    
    # Dictionnaire des départements vers régions
    dept_vers_region = {
        # Auvergne-Rhône-Alpes
        '01': 'Auvergne-Rhône-Alpes', '03': 'Auvergne-Rhône-Alpes', '07': 'Auvergne-Rhône-Alpes',
        '15': 'Auvergne-Rhône-Alpes', '26': 'Auvergne-Rhône-Alpes', '38': 'Auvergne-Rhône-Alpes',
        '42': 'Auvergne-Rhône-Alpes', '43': 'Auvergne-Rhône-Alpes', '63': 'Auvergne-Rhône-Alpes',
        '69': 'Auvergne-Rhône-Alpes', '73': 'Auvergne-Rhône-Alpes', '74': 'Auvergne-Rhône-Alpes',
        
        # Bourgogne-Franche-Comté
        '21': 'Bourgogne-Franche-Comté', '25': 'Bourgogne-Franche-Comté', '39': 'Bourgogne-Franche-Comté',
        '58': 'Bourgogne-Franche-Comté', '70': 'Bourgogne-Franche-Comté', '71': 'Bourgogne-Franche-Comté',
        '89': 'Bourgogne-Franche-Comté', '90': 'Bourgogne-Franche-Comté',
        
        # Bretagne
        '22': 'Bretagne', '29': 'Bretagne', '35': 'Bretagne', '56': 'Bretagne',
        
        # Centre-Val de Loire
        '18': 'Centre-Val de Loire', '28': 'Centre-Val de Loire', '36': 'Centre-Val de Loire',
        '37': 'Centre-Val de Loire', '41': 'Centre-Val de Loire', '45': 'Centre-Val de Loire',
        
        # Corse
        '20': 'Corse', '2A': 'Corse', '2B': 'Corse',
        
        # Grand Est
        '08': 'Grand Est', '10': 'Grand Est', '51': 'Grand Est', '52': 'Grand Est',
        '54': 'Grand Est', '55': 'Grand Est', '57': 'Grand Est', '67': 'Grand Est',
        '68': 'Grand Est', '88': 'Grand Est',
        
        # Hauts-de-France
        '02': 'Hauts-de-France', '59': 'Hauts-de-France', '60': 'Hauts-de-France',
        '62': 'Hauts-de-France', '80': 'Hauts-de-France',
        
        # Ile-de-France
        '75': 'Ile-de-France', '77': 'Ile-de-France', '78': 'Ile-de-France',
        '91': 'Ile-de-France', '92': 'Ile-de-France', '93': 'Ile-de-France',
        '94': 'Ile-de-France', '95': 'Ile-de-France',
        
        # Normandie
        '14': 'Normandie', '27': 'Normandie', '50': 'Normandie', '61': 'Normandie', '76': 'Normandie',
        
        # Nouvelle-Aquitaine
        '16': 'Nouvelle-Aquitaine', '17': 'Nouvelle-Aquitaine', '19': 'Nouvelle-Aquitaine',
        '23': 'Nouvelle-Aquitaine', '24': 'Nouvelle-Aquitaine', '33': 'Nouvelle-Aquitaine',
        '40': 'Nouvelle-Aquitaine', '47': 'Nouvelle-Aquitaine', '64': 'Nouvelle-Aquitaine',
        '79': 'Nouvelle-Aquitaine', '86': 'Nouvelle-Aquitaine', '87': 'Nouvelle-Aquitaine',
        
        # Occitanie
        '09': 'Occitanie', '11': 'Occitanie', '12': 'Occitanie', '30': 'Occitanie',
        '31': 'Occitanie', '32': 'Occitanie', '34': 'Occitanie', '46': 'Occitanie',
        '48': 'Occitanie', '65': 'Occitanie', '66': 'Occitanie', '81': 'Occitanie', '82': 'Occitanie',
        
        # Pays de la Loire
        '44': 'Pays de la Loire', '49': 'Pays de la Loire', '53': 'Pays de la Loire',
        '72': 'Pays de la Loire', '85': 'Pays de la Loire',
        
        # Provence-Alpes-Côte d'Azur
        '04': "Provence-Alpes-Côte d'Azur", '05': "Provence-Alpes-Côte d'Azur",
        '06': "Provence-Alpes-Côte d'Azur", '13': "Provence-Alpes-Côte d'Azur",
        '83': "Provence-Alpes-Côte d'Azur", '84': "Provence-Alpes-Côte d'Azur",
        
        # DROM (Départements et Régions d'Outre-Mer)
        '97': 'DROM',  # Gestion par les 3 premiers chiffres
        '971': 'Guadeloupe',
        '972': 'Martinique',
        '973': 'Guyane',
        '974': 'La Réunion',
        '975': 'Saint-Pierre-et-Miquelon',
        '976': 'Mayotte',
        '977': 'Saint-Barthélemy',
        '978': 'Saint-Martin',
        '98': 'COM et territoires d’outre-mer',
        '984': 'Terres australes et antarctiques françaises',
        '986': 'Wallis-et-Futuna',
        '987': 'Polynésie française',
        '988': 'Nouvelle-Calédonie',
        '989': 'Île de Clipperton'
    }
    
    # Convertir en string si nécessaire
    code_postal = str(code_postal)
    
    # Pour les DROM, vérifier d'abord les 3 premiers chiffres
    if code_postal.startswith('97') or code_postal.startswith('98'):
        dept_3 = code_postal[:3]
        if dept_3 in dept_vers_region:
            return dept_vers_region[dept_3]
    
    # Sinon, extraire les 2 premiers chiffres
    dept = code_postal[:2]
    
    return dept_vers_region.get(dept, "Région inconnue")
