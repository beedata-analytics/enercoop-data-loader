# encoding: utf-8

# Beedata Api settings
BEEDATA_LOGIN_USER = ''
BEEDATA_LOGIN_PASSWORD = ''

BEEDATA_CRT_PATH = ''
BEEDATA_KEY_PATH = ''
BEEDATA_COMPANYID = 0
BEEDATA_BASE_URL = 'https://api.beedataanalytics.com/'

BEEDATA_ENDPOINTS = {
    'contracts': 'v1/contracts',
    'measures': 'v1/amon_measures'
}

# Enedis Webservice settings 
ENEDIS_LOGIN_USER = ''
ENEDIS_LOGIN_PASSWORD = ''
WSDL_PATH = 'Enercoop/ConsultationMesuresDetaillees-v1.0.wsdl'


# Enedis required request fields
ENEDIS_INIT_LOGIN_MAIL = ''
ENEDIS_CONTRAT_ID = ''


# Local database settings
MONGO_HOST = ''
MONGO_DBNAME = ''
MONGO_USERNAME = ''
MONGO_PASSWORD = ''

# PRM/PDL security 
ANONYMIZE_KEY = ''


# File CSV delimiters
CONTRACTS_DELIMITER = ';'
CONTRACTS_DATETIME_FORMAT = '%d/%m/%Y'
AUTHORIZATIONS_DELIMITER = ','
AUTHORIZATIONS_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
HOURS_DELIMITER = ';'
HOURS_DATETIME_FORMAT = '%d/%m/%Y'



# Other
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
CONTRACT_COLUMNS = {
    'contractId': 'Contrat',
    'dateStart': 'Date mise en service',
    'dateEnd': 'Date fin',
    'activityCode': 'Code NAF',
    'tariffId': 'Tarif',
    'power': 'pce souscrite',
    'postalCode': 'Code postal du PDL',
    'contract_type': 'Nature titulaire',
    'meteringPointId': 'PDL' # will be anonymized        
}

MODIFICATIONS = 9
CONTRACTS_HISTORY_COLUMNS = {
    'changes': 'Ancien av. n°',
    'dateStart': 'Date début ancien av. n°',
    'dateEnd': 'Date fin ancien av. n°',
    'tariffId': 'Ancien tarif n°',
    'tariff': 'Anc. opt. tarifaire n°',
    'power': 'Anc. pce n°',
    'reason': 'Motif ancien av. n°'
}

DISCRIMINATION_TARIFFS = []

AUTHORIZATIONS_COLUMNS = {
    'meteringPointId': 'PDL',
    'auth30': 'typeData_30min',
    'dateStart30': 'dateStart',
    'dateEnd30': 'dateEnd',
    'authDay': 'typeData_Day',
    'dateStartDay': 'dateStartDay',
    'dateEndDay': 'dateEndDay',
} 

HOURS_COLUMNS = {
    'meteringPointId': 'PDL',
    'currentHours': 'Plages heures creuses',
    'futureHours': 'Futures plages heures creuses',
    'modification': 'Date de derniere modification de FTA'
} 



