# Table de passage codePaysEtrangerEtablissement (COG INSEE, millesime 2025) -> ISO 3166-1 alpha-3
# Source : COG INSEE (via github.com/sfaut/data-cog). None = pas de code ISO officiel.
# Cle = valeur brute du champ Sirene (chaine "99XXX").

COG_TO_ISO3 = {
    "99100": "FRA",           # France
    "99101": "DNK",           # Danemark
    "99102": "ISL",           # Islande
    "99103": "NOR",           # Norvège
    "99104": "SWE",           # Suède
    "99105": "FIN",           # Finlande
    "99106": "EST",           # Estonie
    "99107": "LVA",           # Lettonie
    "99108": "LTU",           # Lituanie
    "99109": "DEU",           # Allemagne
    "99110": "AUT",           # Autriche
    "99111": "BGR",           # Bulgarie
    "99112": "HUN",           # Hongrie
    "99113": "LIE",           # Liechtenstein
    "99114": "ROU",           # Roumanie
    "99116": "CZE",           # Tchéquie
    "99117": "SVK",           # Slovaquie
    "99118": "BIH",           # Bosnie-Herzégovine
    "99119": "HRV",           # Croatie
    "99120": "MNE",           # Monténégro
    "99121": "SRB",           # Serbie
    "99122": "POL",           # Pologne
    "99123": "RUS",           # Russie
    "99125": "ALB",           # Albanie
    "99126": "GRC",           # Grèce
    "99127": "ITA",           # Italie
    "99128": "SMR",           # Saint-Marin
    "99129": "VAT",           # Vatican
    "99130": "AND",           # Andorre
    "99131": "BEL",           # Belgique
    "99132": "GBR",           # Royaume-Uni
    "99133": "GIB",           # Territoires britanniques en Méditerranée
    "99134": "ESP",           # Espagne
    "99135": "NLD",           # Pays-Bas
    "99136": "IRL",           # Irlande
    "99137": "LUX",           # Luxembourg
    "99138": "MCO",           # Monaco
    "99139": "PRT",           # Portugal
    "99140": "CHE",           # Suisse
    "99144": "MLT",           # Malte
    "99145": "SVN",           # Slovénie
    "99148": "BLR",           # Biélorussie
    "99151": "MDA",           # Moldavie
    "99155": "UKR",           # Ukraine
    "99156": "MKD",           # Macédoine du Nord
    "99157": "XKX",            # Kosovo
    "99158": "FRO",           # Féroé
    "99159": None,            # Territoires norvégiens en Atlantique Nord
    "99160": "ALA",           # Åland
    "99161": None,            # Dépendances de la Couronne britannique
    "99162": None,            # Île des Faisans
    "99201": "SAU",           # Arabie saoudite
    "99203": "IRQ",           # Irak
    "99204": "IRN",           # Iran
    "99205": "LBN",           # Liban
    "99206": "SYR",           # Syrie
    "99207": "ISR",           # Israël
    "99208": "TUR",           # Turquie
    "99212": "AFG",           # Afghanistan
    "99213": "PAK",           # Pakistan
    "99214": "BTN",           # Bhoutan
    "99215": "NPL",           # Népal
    "99216": "CHN",           # Chine
    "99217": "JPN",           # Japon
    "99219": "THA",           # Thaïlande
    "99220": "PHL",           # Philippines
    "99222": "JOR",           # Jordanie
    "99223": "IND",           # Inde
    "99224": "MMR",           # Birmanie
    "99225": "BRN",           # Brunei
    "99226": "SGP",           # Singapour
    "99227": "MYS",           # Malaisie
    "99229": "MDV",           # Maldives
    "99230": "HKG",           # Hong Kong
    "99231": "IDN",           # Indonésie
    "99232": "MAC",           # Macao
    "99234": "KHM",           # Cambodge
    "99235": "LKA",           # Sri Lanka
    "99236": "TWN",           # Taïwan
    "99238": "PRK",           # Corée du Nord
    "99239": "KOR",           # Corée du Sud
    "99240": "KWT",           # Koweït
    "99241": "LAO",           # Laos
    "99242": "MNG",           # Mongolie
    "99243": "VNM",           # Vietnam
    "99246": "BGD",           # Bangladesh
    "99247": "ARE",           # Émirats arabes unis
    "99248": "QAT",           # Qatar
    "99249": "BHR",           # Bahreïn
    "99250": "OMN",           # Oman
    "99251": "YEM",           # Yémen
    "99252": "ARM",           # Arménie
    "99253": "AZE",           # Azerbaïdjan
    "99254": "CYP",           # Chypre
    "99255": "GEO",           # Géorgie
    "99256": "KAZ",           # Kazakhstan
    "99257": "KGZ",           # Kirghizstan
    "99258": "UZB",           # Ouzbékistan
    "99259": "TJK",           # Tadjikistan
    "99260": "TKM",           # Turkménistan
    "99261": "PSE",           # Palestine
    "99262": "TLS",           # Timor oriental
    "99265": None,            # Territoires australiens de l’océan Indien
    "99301": "EGY",           # Égypte
    "99302": "LBR",           # Libéria
    "99303": "ZAF",           # Afrique du Sud
    "99304": "GMB",           # Gambie
    "99306": "SHN",           # Territoires britanniques de l’océan Atlantique
    "99308": "IOT",           # Territoire britannique de l’océan Indien
    "99309": "TZA",           # Tanzanie
    "99310": "ZWE",           # Zimbabwe
    "99311": "NAM",           # Namibie
    "99312": "COD",           # République démocratique du Congo
    "99313": "ESP",           # Territoires espagnols en Afrique du Nord
    "99314": "GNQ",           # Guinée équatoriale
    "99315": "ETH",           # Éthiopie
    "99316": "LBY",           # Libye
    "99317": "ERI",           # Érythrée
    "99318": "SOM",           # Somalie
    "99319": "PRT",           # Territoires portugais de l’océan Atlantique
    "99321": "BDI",           # Burundi
    "99322": "CMR",           # Cameroun
    "99323": "CAF",           # Centrafrique
    "99324": "COG",           # Congo
    "99326": "CIV",           # Côte d’Ivoire
    "99327": "BEN",           # Bénin
    "99328": "GAB",           # Gabon
    "99329": "GHA",           # Ghana
    "99330": "GIN",           # Guinée
    "99331": "BFA",           # Burkina
    "99332": "KEN",           # Kenya
    "99333": "MDG",           # Madagascar
    "99334": "MWI",           # Malawi
    "99335": "MLI",           # Mali
    "99336": "MRT",           # Mauritanie
    "99337": "NER",           # Niger
    "99338": "NGA",           # Nigéria
    "99339": "UGA",           # Ouganda
    "99340": "RWA",           # Rwanda
    "99341": "SEN",           # Sénégal
    "99342": "SLE",           # Sierra Leone
    "99343": "SDN",           # Soudan
    "99344": "TCD",           # Tchad
    "99345": "TGO",           # Togo
    "99346": "ZMB",           # Zambie
    "99347": "BWA",           # Botswana
    "99348": "LSO",           # Lesotho
    "99349": "SSD",           # Soudan du Sud
    "99350": "MAR",           # Maroc
    "99351": "TUN",           # Tunisie
    "99352": "DZA",           # Algérie
    "99363": None,            # Terres australes norvégiennes
    "99389": "ESH",           # Sahara occidental
    "99390": "MUS",           # Maurice
    "99391": "SWZ",           # Swaziland
    "99392": "GNB",           # Guinée-Bissao
    "99393": "MOZ",           # Mozambique
    "99394": "STP",           # Sao Tomé-et-Principe
    "99395": "AGO",           # Angola
    "99396": "CPV",           # Cap-Vert
    "99397": "COM",           # Comores
    "99398": "SYC",           # Seychelles
    "99399": "DJI",           # Djibouti
    "99401": "CAN",           # Canada
    "99404": "USA",           # États-Unis
    "99405": "MEX",           # Mexique
    "99406": "CRI",           # Costa Rica
    "99407": "CUB",           # Cuba
    "99408": "DOM",           # République dominicaine
    "99409": "GTM",           # Guatémala
    "99410": "HTI",           # Haïti
    "99411": "HND",           # Honduras
    "99412": "NIC",           # Nicaragua
    "99413": "PAN",           # Panama
    "99414": "SLV",           # Salvador
    "99415": "ARG",           # Argentine
    "99416": "BRA",           # Brésil
    "99417": "CHL",           # Chili
    "99418": "BOL",           # Bolivie
    "99419": "COL",           # Colombie
    "99420": "ECU",           # Équateur
    "99421": "PRY",           # Paraguay
    "99422": "PER",           # Pérou
    "99423": "URY",           # Uruguay
    "99424": "VEN",           # Vénézuéla
    "99425": None,            # Territoires britanniques dans les Antilles et Bermudes
    "99426": "JAM",           # Jamaïque
    "99427": None,            # Territoires britanniques dans l’océan Atlantique Sud
    "99428": "GUY",           # Guyana
    "99429": "BLZ",           # Bélize
    "99430": "GRL",           # Groenland
    "99431": "ABW",           # Aruba
    "99432": None,            # Territoires des États-Unis en Amérique
    "99433": "TTO",           # Trinité-et-Tobago
    "99434": "BRB",           # Barbade
    "99435": "GRD",           # Grenade
    "99436": "BHS",           # Bahamas
    "99437": "SUR",           # Suriname
    "99438": "DMA",           # Dominique
    "99439": "LCA",           # Sainte-Lucie
    "99440": "VCT",           # Saint-Vincent-et-les-Grenadines
    "99441": "ATG",           # Antigua-et-Barbuda
    "99442": "KNA",           # Saint-Christophe-et-Niévès
    "99443": "BES",           # Bonaire, Saint-Eustache, Saba
    "99444": "CUW",           # Curaçao
    "99445": "SXM",           # Saint-Martin (partie néerlandaise)
    "99446": None,            # Territoire antarctique argentin
    "99447": None,            # Territoire antarctique chilien
    "99501": "AUS",           # Australie
    "99502": "NZL",           # Nouvelle-Zélande
    "99503": "PCN",           # Pitcairn
    "99505": None,            # Territoires des États-Unis en Océanie
    "99506": "WSM",           # Samoa
    "99507": "NRU",           # Nauru
    "99508": "FJI",           # Fidji
    "99509": "TON",           # Tonga
    "99510": "PNG",           # Papouasie-Nouvelle-Guinée
    "99511": "TUV",           # Tuvalu
    "99512": "SLB",           # Salomon
    "99513": "KIR",           # Kiribati
    "99514": "VUT",           # Vanuatu
    "99515": "MHL",           # Marshall
    "99516": "FSM",           # Micronésie
    "99517": "PLW",           # Palaos
    "99519": None,            # Territoires de la Nouvelle-Zélande
    "99520": "COK",           # Cook
    "99521": "NIU",           # Niue
    "99522": None,            # Territoires de l’Australie en Océanie
    "99699": "ATA",           # Antarctique
}
