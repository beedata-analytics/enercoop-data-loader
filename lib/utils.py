# encoding: utf-8

import csv
import logging
import hashlib
from pymongo import MongoClient
from datetime import datetime, timedelta
from json import dumps
from copy import deepcopy

#custom imports
import settings 
from lib.transformations import date_converter, str2bool
from lib.security import encode
from lib.enedis_connector import get_data, init_webservice_client
from lib.beedata_connector import BaseClient


#init clients to reuse them through all services calls
ws_client = init_webservice_client()
beedata_client = BaseClient()
logger = logging.getLogger("app")

def connect_mongo():
    """ Return a connector to DataBase defined at settings """
    logger.debug('Connecting to MongoDB...')
    mongo = MongoClient(settings.MONGO_HOST)
    db = mongo[settings.MONGO_DBNAME]
    if settings.MONGO_USERNAME:
        logger.debug('Authenticating to MongoDB...')
        db.authenticate(settings.MONGO_USERNAME, settings.MONGO_PASSWORD)
        logger.debug('Successfully authenticated to MongoDB.')
        
    logger.info('Successfully connected to MongoDB.')
    
    return db
    

def read_csv_file(path, delimiter=','):
    """ Parse CSV and returns a list of row dicts
    
    :param path: Path where file is
    :param delimiter: Delimiter for CSV file. Default ';'
    """
    with open(path, mode='r', encoding='utf-8') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=delimiter)
        rows = []
        for row in csv_reader:
            rows.append(row)
        
        return rows


def get_modification(row, number):
    """ Gets the field that genereated the contract modification
    
    :param row: CSV row dict
    :param number: The modification number being processed
    """
    for modification in ['tariffId', 'power']:
        if row[settings.CONTRACT_COLUMNS[modification]] != row[settings.CONTRACTS_HISTORY_COLUMNS[modification]+'%s' % number]:
            return modification
        
    return None

def get_modification_dict(row, number, modification):
    """ Creates a dictionary for the row which will create the tariffHistory or powerHistory item
    
    :param row: CSV row dict
    :param number: The modification number being processed
    :param modification: One of power or tariffId
    """
    try:
        result = {
            modification: int(float(row[settings.CONTRACTS_HISTORY_COLUMNS[modification]+'%s' % number])*1000) if modification == 'power' else row[settings.CONTRACTS_HISTORY_COLUMNS[modification]+'%s' % number],
            'dateStart': date_converter(row[settings.CONTRACTS_HISTORY_COLUMNS['dateStart']+'%s' % number], format=settings.CONTRACTS_DATETIME_FORMAT, str_format=settings.DATETIME_FORMAT),
            'dateEnd': date_converter(row[settings.CONTRACTS_HISTORY_COLUMNS['dateEnd']+'%s' % number], format=settings.CONTRACTS_DATETIME_FORMAT, last_second=True, str_format=settings.DATETIME_FORMAT)
        }
    except ValueError:
        logger.error('Contract row is not defining well the contract modification fields (dates) for contract [%s] on its historic change number [%s]: dateStart = [%s] dateEnd = [%s]' % (row[settings.CONTRACT_COLUMNS['contractId']], number, row[settings.CONTRACTS_HISTORY_COLUMNS['dateStart']+'%s' % number], row[settings.CONTRACTS_HISTORY_COLUMNS['dateEnd']+'%s' % number]))
        raise Exception('Error parsing contract row: %s' % row)

    return result

def get_contracts(paths):
    """ Creates a dictionary containing all contract information from the 3 required CSV
    
    :param paths: paths from argparse (it might have all required arguments)
    """
    logger.debug('Start reading CSV files to merge in a common structure...')
    contracts = read_csv_file(paths.contracts, delimiter=settings.CONTRACTS_DELIMITER)
    authorizations = read_csv_file(paths.authorizations, delimiter=settings.AUTHORIZATIONS_DELIMITER)
    hours = read_csv_file(paths.hours, delimiter=settings.HOURS_DELIMITER)
    logger.debug('Files read successfully. Creating contracts documents and adding needed information...')
    
    logger.debug('Start creating contracts documents...')
    contracts_data = {}
    hours_errors = []
    auth_errors = []
    for contract in contracts:
        # creating document for beedata
        # date end that will be used for history creation
        date_end = datetime(2099,1,1).strftime(settings.DATETIME_FORMAT)
        if contract[settings.CONTRACT_COLUMNS['dateEnd']]: 
            try:
                date_end = date_converter(contract[settings.CONTRACT_COLUMNS['dateEnd']], format='%d/%m/%Y', last_second=True, str_format=settings.DATETIME_FORMAT)
            except ValueError:
                raise Exception('Contract row for contract [%s] has end date in bad format: [%s]' % (contract[settings.CONTRACT_COLUMNS['contractId']], contract[settings.CONTRACT_COLUMNS['dateEnd']]))
            
        contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]] = {
            'document': {
                'contractId': contract[settings.CONTRACT_COLUMNS['contractId']],
                'customer': {
                    'address': {
                        'postalCode': contract[settings.CONTRACT_COLUMNS['postalCode']],
                        'countryCode': 'FR'
                    },
                    'customerId': contract[settings.CONTRACT_COLUMNS['contractId']]
                },
                'dateStart': date_converter(contract[settings.CONTRACT_COLUMNS['dateStart']], format=settings.CONTRACTS_DATETIME_FORMAT, str_format=settings.DATETIME_FORMAT),
                'dateEnd': date_end,
                'power': int(float(contract[settings.CONTRACT_COLUMNS['power']])*1000),
                'tariffCostId': str(int(float(contract[settings.CONTRACT_COLUMNS['power']])*1000)/1000),
                'tariffId': contract[settings.CONTRACT_COLUMNS['tariffId']],
                'meteringPointId': encode(contract[settings.CONTRACT_COLUMNS['meteringPointId']]),
                'activityCode': contract[settings.CONTRACT_COLUMNS['activityCode']],
                'customFields': {},
                'devices': [{
                    'dateStart': date_converter(contract[settings.CONTRACT_COLUMNS['dateStart']], format=settings.CONTRACTS_DATETIME_FORMAT, str_format=settings.DATETIME_FORMAT),
                    'dateEnd': date_end,
                    'deviceId': encode(contract[settings.CONTRACT_COLUMNS['meteringPointId']])
                }]
            },
            'contract_type': 'residential' if contract[settings.CONTRACT_COLUMNS['contract_type']].lower() == 'particulier' else 'tertiary'
        }
        contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['csv'] = contract
        
        # create history fields
        tariff_history = []
        power_history = []
        for i in range(1, settings.MODIFICATIONS+1):
            if contract[settings.CONTRACTS_HISTORY_COLUMNS['dateStart']+'%s' % i]:
                modification =  get_modification(contract, i)
                if modification == 'tariffId':
                    tariff_history.append(get_modification_dict(contract, i, modification))
                elif modification == 'power':
                    power_history.append(get_modification_dict(contract, i, modification))
            else:
                break
        
        if tariff_history:
            # add current tariff with last dateEnd as dateStart
            tariff_history.append({
                'dateStart': tariff_history[-1]['dateEnd'],
                'dateEnd': date_end,
                'tariffId': contract[settings.CONTRACT_COLUMNS['tariffId']]
            })
            # get last (current) tariff for tariff_ field
            contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['tariff_'] = tariff_history[-1]
            contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['tariffHistory'] = tariff_history
        else:
            # no history, just set tariff_ to current and use it as unique tariffHistory item
            contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['tariff_'] = {
                'dateStart': contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['dateStart'],
                'dateEnd': contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['dateEnd'],
                'tariffId': contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['tariffId']
            }
            contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['tariffHistory'] = [contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['tariff_']]
        
        if power_history:
            # add current power with last dateEnd as dateStart
            power_history.append({
                'dateStart': power_history[-1]['dateEnd'],
                'dateEnd': date_end,
                'power': int(float(contract[settings.CONTRACT_COLUMNS['power']])*1000)
            })
            # get last (current) power for power_ field
            contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['power_'] = power_history[-1]
            contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['powerHistory'] = power_history
        else:
            # no history, just set power_ to current and use it as unique powerHistory item
            contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['power_'] = {
                'dateStart': contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['dateStart'],
                'dateEnd': contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['dateEnd'],
                'power': contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['power']
            }
            contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['powerHistory'] = [contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['power_']]
        logger.debug('Created contracts documents successfully.')
        
        
        logger.debug('Adding authorization information...')
        for authorization in authorizations:
            # search on full authorization list
            if authorization[settings.AUTHORIZATIONS_COLUMNS['meteringPointId']] == contract[settings.CONTRACT_COLUMNS['meteringPointId']]:
                try:
                    auth_dict = {
                        'auth30': str2bool(authorization[settings.AUTHORIZATIONS_COLUMNS['auth30']]),
                        'dateStart30': date_converter(authorization[settings.AUTHORIZATIONS_COLUMNS['dateStart30']], format=settings.AUTHORIZATIONS_DATETIME_FORMAT) if authorization[settings.AUTHORIZATIONS_COLUMNS['dateStart30']] else None,
                        'dateEnd30': date_converter(authorization[settings.AUTHORIZATIONS_COLUMNS['dateEnd30']], format=settings.AUTHORIZATIONS_DATETIME_FORMAT) if authorization[settings.AUTHORIZATIONS_COLUMNS['dateEnd30']] else None,
                        'authDay': str2bool(authorization[settings.AUTHORIZATIONS_COLUMNS['authDay']]),
                        'dateStartDay': date_converter(authorization[settings.AUTHORIZATIONS_COLUMNS['dateStartDay']], format=settings.AUTHORIZATIONS_DATETIME_FORMAT) if authorization[settings.AUTHORIZATIONS_COLUMNS['dateStartDay']] else None,
                        'dateEndDay': date_converter(authorization[settings.AUTHORIZATIONS_COLUMNS['dateEndDay']], format=settings.AUTHORIZATIONS_DATETIME_FORMAT) if authorization[settings.AUTHORIZATIONS_COLUMNS['dateEndDay']] else None
                    }
                except ValueError:
                    raise Exception('Authorization row is not well formed due to dates or bad values: [%s]' % authorization)

                contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['auth'] = auth_dict
                contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['customFields']['auth'] = {
                    'auth30': auth_dict['auth30'],
                    'dateStart30': auth_dict['dateStart30'].strftime(settings.DATETIME_FORMAT) if auth_dict['dateStart30'] else '',
                    'dateEnd30': auth_dict['dateEnd30'].strftime(settings.DATETIME_FORMAT) if auth_dict['dateEnd30'] else '',
                    'authDay': auth_dict['authDay'],
                    'dateStartDay': auth_dict['dateStartDay'].strftime(settings.DATETIME_FORMAT) if auth_dict['dateStartDay'] else '',
                    'dateEndDay': auth_dict['dateEndDay'].strftime(settings.DATETIME_FORMAT) if auth_dict['dateEndDay'] else '',
                }
                break
            
        if not 'auth' in contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]:
            # check if auth info is available, if not add to error list
            auth_errors.append(contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['contractId'])
            contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['error'] = {'auth': True}
        
        logger.debug('Authorization information successfully added.')
            
        
        logger.debug('Adding discrimination hours information...')
        for hour in hours:
            # search on full hours list
            if hour[settings.HOURS_COLUMNS['meteringPointId']] == contract[settings.CONTRACT_COLUMNS['meteringPointId']]:
                date_end = date_converter(contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['dateEnd'], format=settings.DATETIME_FORMAT)
                
                try:
                    mod_date = date_converter(hour[settings.HOURS_COLUMNS['modification']], format=settings.HOURS_DATETIME_FORMAT)
                except ValueError:
                    raise Exception('Hour row end date [%s] is not well formed: [%s]' % (hour[settings.HOURS_COLUMNS['modification']], hour))

                if hour[settings.HOURS_COLUMNS['modification']] and date_end > mod_date:
                    if hour[settings.HOURS_COLUMNS['currentHours']]:
                        # add discrimination schedule on tariffHistory, tariffId, and tariff_ fields
                        for t in  contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['tariffHistory']:
                            if t['tariffId'] == contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['tariffId']:
                                t['tariffId'] = contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['tariffId']  + '~' +hour[settings.HOURS_COLUMNS['currentHours']]
                        contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['tariff_']['tariffId'] = contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['tariffId'] + '~' +hour[settings.HOURS_COLUMNS['currentHours']]
                        contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['tariffId'] = contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['tariffId'] + '~' +hour[settings.HOURS_COLUMNS['currentHours']] 
                contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['customFields']['hours'] = hour[settings.HOURS_COLUMNS['currentHours']]
                break
                
        if not 'hours' in contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['customFields']:
            # check if hours info is available, if not add to error list
            hours_errors.append(contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['document']['contractId'])
            if 'error' in contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]:
                contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['error']['hours'] = True
            else: 
                contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]['error'] = {'hours': True}
        
        logger.debug('Discrimination hours information successfully added.')
        
        logger.debug('Final document: %s' % (contracts_data[contract[settings.CONTRACT_COLUMNS['contractId']]]))
        
    if auth_errors:
        logger.error('Authorization information not available for this contracts and won\'t be processed: [%s]' % auth_errors)
    if hours_errors:
        logger.error('Hours information not available for this contracts and won\'t be processed: [%s]' % hours_errors)
        
        
    logger.info('Contracts documents and required information successfully created.')
    logger.info('Contracts read: [%s]' % len(contracts_data.keys()))
    
    return contracts_data
    
    
def upload_contract(mongo_contract, data, current_etag, beedata_client):
    """ Function to decide when contract needs to be POST, PATCH or nothing
     
    :param mongo_contract: document from MongoDB if it was stored
    :param data: contract document with all needed information
    :param current_etag: string to determine if a contract has modifications since last operation
    :param beedata_client: connector to Beedata API
    """
    contract_report = {}
    if not mongo_contract or (mongo_contract and not 'etag' in mongo_contract):
        if beedata_client.get_contract(data['document']['contractId']):
            logger.debug('Contract [%s] already on Beedata API... Proceeding with a PATCH operation' % data['document']['contractId'])
            res = beedata_client.modify_contract(data['document'])
            contract_report['contracts_api_call'] = 'PATCH'
            contract_report['contracts_api_status'] = res.status_code
            if res.status_code != 200:
                contract_report['contracts_api_error'] = res.text
                logger.error('Beedata API response was unexpected on PATCH existing contract [%s]:    %s' % (data['document']['contractId'], res.text))
            else:
                logger.debug('PATCH contract [%s] successfully modified on Beedata API.' % (data['document']['contractId']))
        else:
            logger.debug('POST new contract [%s] to Beedata' % (data['document']['contractId']))
            res = beedata_client.send_data(data['document'], 'contracts')
            contract_report['contracts_api_call'] = 'POST'
            contract_report['contracts_api_status'] = res.status_code
            if res.status_code != 201:
                contract_report['contracts_api_error'] = res.text
                logger.error('Beedata API response was unexpected on POST new contract [%s]:    %s' % (data['document']['contractId'], res.text))
            else:
                logger.info('New contract [%s] successfully created on Beedata API.' % (data['document']['contractId']))
            
    else:
        if mongo_contract['etag'] != current_etag:
            logger.debug('PATCH contract [%s] to Beedata' % (data['document']['contractId']))
            res = beedata_client.modify_contract(data['document'])
            contract_report['contracts_api_call'] = 'PATCH'
            contract_report['contracts_api_status'] = res.status_code
            if res.status_code != 200:
                contract_report['contracts_api_error'] = res.text
                logger.error('Beedata API response was unexpected on PATCH existing contract [%s]:    %s' % (data['document']['contractId'], res.text))
            else:
                logger.debug('PATCH contract [%s] successfully modified on Beedata API.' % (data['document']['contractId']))
        else:
            contract_report['contracts_api_call'] = None
            contract_report['contracts_api_status'] = None
            logger.info('Contract [%s] does not have modifications. No calls to Beedata API needed.' % (data['document']['contractId']))
    
    return contract_report

def get_measures_dates(authorization, date_start, date_end, contract, type, margindays):
    """ Return from_date for given measure type. It has to determined between authorization files, contract dates and last measure stored on MongoDB
    
    :param authorization: contract authorization dict
    :param date_start: contract start date
    :param date_end: contract end date
    :param contract: document stored at mongoDB for this contract or None
    :param type: measures type. One of PMAX, CDC, CONSOGLO
    :param margindays: number of days we leave as margin 
    
    :return result dictionary with backward and forward from and to dates
    """
    result = {}
    
    mongo_contract = deepcopy(contract)
    date_start = date_converter(date_start, format=settings.DATETIME_FORMAT)
    date_end = date_converter(date_end, format=settings.DATETIME_FORMAT)
    if mongo_contract:
        del mongo_contract['prm']
    
    logger.debug('Arguments received for get dates ranges: dateStart [%s], dateEnd [%s], mongo_contract [%s], authorization [%s], type [%s]' % (date_start, date_end, mongo_contract, authorization, type))
    if type == 'PMAX' or type =='CONSOGLO':
        result['min'] = max(date_start, datetime.now() - timedelta(days=1095)) if authorization['authDay'] else None
        result['max'] = min(date_end, datetime.now() - timedelta(days=margindays)) if authorization['authDay'] else None
        if result['max'] and authorization['dateEndDay']:
            result['max'] = min(result['max'], authorization['dateEndDay']) if authorization['dateEndDay'] else result['max']
    elif type == 'CDC':
        result['min'] = max(date_start,  datetime.now() - timedelta(days=1095), authorization['dateStart30']) if authorization['auth30'] else None
        result['max'] = min(date_end, datetime.now() - timedelta(days=margindays)) if authorization['auth30'] else None
        if result['max']:
            result['max'] = min(result['max'], authorization['dateEnd30']) if authorization['dateEnd30'] else result['max']
    
    
    if not result['min'] or not result['max']:
        return None 
    
    
    if mongo_contract and 'ts_min_%s' % type in mongo_contract and mongo_contract['ts_min_%s' % type] and 'ts_max_%s' % type in mongo_contract and mongo_contract['ts_max_%s' % type]:
        if mongo_contract['ts_min_%s' % type] > result['min']:
            result['backward'] = {
                'from_date': result['min'],
                'to_date': mongo_contract['ts_min_%s' % type]
            }
        else: 
            result['backward'] = None
        
        if mongo_contract['ts_max_%s' % type] < result['max']:
            result['forward'] = {
                'from_date': mongo_contract['ts_max_%s' % type],
                'to_date': result['max']
            }
        else:
            result['forward'] = None
    else:
        if result['min']:
            result['backward'] = {
                'from_date': result['min'],
                'to_date': result['max']
            }
            result['forward'] = None
        else:
            result = None 
    
    logger.debug('Dates limits for [%s] measures: %s' % (type, result))
    
    return result

    
def process_contract(id, data, customer_type, margindays, measure_types):
    """ Main function to process a single contract (upload or update contract on Beedata and add its measures too).
    
    :param id: contractId. Main connector between Enercoop and Beedata
    :param data: contract document containing all information created on get_contracts
    :param customer_type: one of residential or tertiary
    :param margindays: number of days we leave as margin
    :param measure_types: measure types to recover from Enedis
    """
    
    if 'error' in data:
        return None
    
    report = {
        'start': datetime.now(),
        'contractId': id
    }
    
    logger.info('Processing contract: [%s]...' % id)
    logger.debug('Getting stored data from MongoDB...')
    mongo_db = connect_mongo()
    mongo_contract = mongo_db['Contracts'].find_one({'contractId': id})
    if mongo_contract:
        for field, value in mongo_contract.items():
            if 'ts_' in field and value and isinstance(value, str):
                mongo_contract[field] = date_converter(value, format=settings.DATETIME_FORMAT)
        logger.debug('Info recovered from MongoDB for contract [%s]: %s' % (id, mongo_contract))
    else:
        mongo_contract = {}
    
    logger.debug('Deciding if contract should be POSTed or PATCHed')
    current_etag = document_etag(data['document'])
    contract_report = upload_contract(mongo_contract, data, current_etag, beedata_client)
    report['contract_report'] = contract_report
        
    
    # getting measures
    ts_min = {}
    ts_max = {}
    report_results = {
        'PMAX': {},
        'CDC': {'iterations': []},
        'CONSOGLO': {}
    }

    # set measure types to recover
    types = ['PMAX', 'CONSOGLO', 'CDC'] if measure_types == 'ALL' else [measure_types]

    # repeat for each measure type
    for i in types:
        result = {}
        dates = get_measures_dates(data['auth'], data['document']['dateStart'], data['document']['dateEnd'], mongo_contract, i, margindays)
        if dates:
            if i == 'CDC':
                if dates['backward']:
                    logger.info('Starting "backward" loop to recover [CDC] measures for contract [%s]...' % id)
                    from_date = dates['backward']['to_date']
                    #from_date = dates['backward']['from_date']
                    to_date = from_date - timedelta(days=7)
                    error = 0
                    #while to_date > dates['backward']['to_date']:
                    while to_date > dates['backward']['from_date']:
                        logger.info('Recovering "backwards" [CDC] measures from Enedis service: from [%s] to [%s]...' % (to_date.strftime('%d/%m/%Y'), from_date.strftime('%d/%m/%Y'))) 
                        result2 = get_data(ws_client, data, i, customer_type, to_date, from_date)
                        recover_report = {
                            'from_date': to_date.strftime('%d/%m/%Y'),
                            'to_date': from_date.strftime('%d/%m/%Y'),
                        }
                        if not 'error' in result2:
                            # Accumulate on 1 single document to POST
                            if not result:
                                result = result2
                            else:
                                # if there is everything created just add measurements we just recovered
                                result['measurements'] = result['measurements'] + result2['measurements']
                                
                            recover_report['measures'] = len(result2['measurements'])
                        else:
                            recover_report['error'] = result2['error']
                            error = 1
                       
                        report_results['CDC']['iterations'].append(recover_report)
                        from_date = from_date - timedelta(days=7)
                        to_date = to_date - timedelta(days=7)
                        
                        if error:
                            break
                        
                    if result and len(result['measurements']) > 1:
                        aux = result['measurements']
                        report_results[i]['measures'] = len(aux)
                        ts_min['CDC'] = sorted(aux, key = lambda j: j['timestamp'])[0]['timestamp']
                        if not dates['forward']:
                            ts_max['CDC'] = sorted(aux, key = lambda j: j['timestamp'])[-1]['timestamp']
                    else:
                        ts_min['CDC'] = mongo_contract['ts_min_CDC'] if 'ts_min_%s' % i in mongo_contract else None
                        ts_max['CDC'] = mongo_contract['ts_max_CDC'] if 'ts_max_%s' % i in mongo_contract else None
                        
                if dates['forward']:
                    from_date = dates['forward']['from_date']
                    to_date = from_date + timedelta(days=7)
                    error = 0
                    logger.info('Starting "forward" loop to recover [CDC] measures for contract [%s]...' % id)
                    while to_date < dates['forward']['to_date']:
                        logger.info('Recovering "forward" [CDC] measures from Enedis service: from [%s] to [%s]...' % (from_date.strftime('%d/%m/%Y'), to_date.strftime('%d/%m/%Y'))) 
                        result2 = get_data(ws_client, data, i, customer_type, from_date, to_date)
                        recover_report = {
                            'from_date': from_date.strftime('%d/%m/%Y'),
                            'to_date': to_date.strftime('%d/%m/%Y'),
                        }
                        if not 'error' in result2:
                            # Accumulate on 1 single document to POST
                            if not result:
                                result = result2
                            else:
                                # if there is everything created just add measurements we just recovered
                                result['measurements'] = result['measurements'] + result2['measurements']
                                
                            recover_report['measures'] = len(result2['measurements'])
                        else:
                            recover_report['error'] = result2['error']
                            error = 1
                       
                        report_results['CDC']['iterations'].append(recover_report)
                        from_date = from_date + timedelta(days=7)
                        to_date = to_date + timedelta(days=7)
                        
                        if error:
                            break
                        
                    if result and len(result['measurements']) > 1:
                        aux = result['measurements']
                        report_results[i]['measures'] = len(aux)
                        ts_max['CDC'] = date_converter(sorted(aux, key = lambda j: j['timestamp'])[-1]['timestamp'], format=settings.DATETIME_FORMAT)
                    else:
                        ts_max['CDC'] = mongo_contract['ts_max_CDC'] if 'ts_min_%s' % i in mongo_contract else None
                    
                           
            else:
                logger.info('Recovering [%s] measurements from Enedis service for contract [%s]...' % (i, id))
                if dates['backward']:
                    from_date = dates['backward']['from_date']
                    to_date = dates['backward']['to_date']
                    
                    logger.info('Recovering "backwards" [%s] measurements from Enedis service for contract [%s]: from [%s] to [%s]...' % (i, id, from_date, to_date))
                    result = get_data(ws_client, data, i, customer_type, from_date, to_date)
                    report_results[i] = {
                        'from_date': from_date.strftime('%d/%m/%Y'),
                        'to_date': to_date.strftime('%d/%m/%Y'),
                    }
                    if not 'error' in result:
                        aux = result['measurements']
                        report_results[i]['measures'] = len(result['measurements'])
                        ts_min[i] = sorted(aux, key = lambda j: j['timestamp'])[0]['timestamp']
                        if not dates['forward']:
                            ts_max[i] = sorted(aux, key = lambda j: j['timestamp'])[-1]['timestamp']
                    else:
                        ts_min[i] = mongo_contract['ts_min_%s' % i] if 'ts_min_%s' % i in mongo_contract else None
                        if not dates['forward']:
                            ts_max[i] = mongo_contract['ts_max_%s' % i] if 'ts_max_%s' % i in mongo_contract else None
                        report_results[i]['error'] = result['error'] 
                
                if dates['forward']:
                    from_date = dates['forward']['from_date']
                    to_date = dates['forward']['to_date']

                    logger.info('Recovering "forward" [%s] measurements from Enedis service for contract [%s]: from [%s] to [%s]...' % (i, id, from_date, to_date))
                    result2 = get_data(ws_client, data, i, customer_type, from_date, to_date)
                    report_results[i] = {
                        'from_date': from_date.strftime('%d/%m/%Y'),
                        'to_date': to_date.strftime('%d/%m/%Y'),
                    }
                    if not 'error' in result2:
                        # Accumulate on 1 single document to POST
                        if not result or not 'measurements' in result:
                            result = result2
                        else:
                            # if there is everything created just add measurements we just recovered
                            result['measurements'] = result['measurements'] + result2['measurements']
                            
                        aux = result['measurements']
                        report_results[i]['measures'] = len(result['measurements'])
                        ts_max[i] = sorted(aux, key = lambda j: j['timestamp'])[-1]['timestamp']
                    else:
                        report_results[i]['error'] = result2['error'] 
                        ts_max[i] = mongo_contract['ts_max_%s' % i] if 'ts_max_%s' % i in mongo_contract else None
                    
        else:
            logger.debug('Contract [%s] does not have authorization for [%s] measures' % (id, i))       
        
        
        logger.debug('Sending [%s] data for contract [%s] to Beedata...' % (i, id))
        if result and 'measurements' in result and len(result['measurements']):
            api_result = beedata_client.send_data(result, 'measures')
            report_results[i]['beedata_call_status'] = api_result.status_code
            if api_result.status_code != 200:
                report_results[i]['beedata_call_error'] = api_result.text 
                logger.error('Error on POST measures to Beedata: %s' % (api_result.text))
                # if there is an error, update ts_min and ts_max to previous values
                logger.debug('Using previous ts_min and ts_max values because we weren\'t able to send data to Beedata API.')
                ts_min[i] = mongo_contract['ts_min_%s' % i] if 'ts_min_%s' % i in mongo_contract else None
                ts_max[i] = mongo_contract['ts_max_%s' % i] if 'ts_max_%s' % i in mongo_contract else None
            else:
                aux = result['measurements']
                logger.info('Measures type [%s] successfully sent to Beedata. Measures loaded: [%s]' % (i, len(aux)))
                logger.debug('Data for type [%s] is between [%s] and [%s]' % (i, sorted(aux, key = lambda j: j['timestamp'])[0]['timestamp'], sorted(aux, key = lambda j: j['timestamp'])[-1]['timestamp'])) 
        else:
            logger.info('No measures type [%s] for send to Beedata API.' % i)

    
    report['measures_report'] = report_results
    logger.info('Updating mongo contract with ts_min values [%s] and ts_max values [%s]' % (ts_min, ts_max))
    update_mongo_contract(mongo_db, id, ts_min, ts_max, current_etag, data['csv'][settings.CONTRACT_COLUMNS['meteringPointId']])
    report['finish'] = datetime.now()
    logger.info('Loop for contract [%s] finished.' % id)
    
    return report
    
    
def document_etag(value):
    """ Creates a value that will be used to know if contract has changed or not since last execution
    
    :param value: entire dict to be calculated
    """
    h = hashlib.sha1()
    h.update(dumps(value, sort_keys=True).encode("utf-8"))
    return h.hexdigest()    
        
    
    
def update_mongo_contract(mongo_db, id, ts_min, ts_max, etag, prm):
    """ Saves to mongo the contract information once it has been processed
    
    :param mongo_db: MongoDB connector
    :param id: contractId
    :param ts_min: dict containing first recovered measure 
    :param ts_max: dict containing last recovered measure 
    :param etag: contract document etag of last CSV
    :param prm: contract prm
    """
    doc = {
        'contractId': id,
        'etag': etag,
        'prm': prm,
        'meteringPointId': encode(prm),
        'last_op': datetime.now()
    }        
    for i in ['PMAX', 'CONSOGLO', 'CDC']:
        if i in ts_min and ts_min[i]:
            doc['ts_min_%s' % i] = ts_min[i]
        if i in ts_max and ts_max[i]:
            doc['ts_max_%s' % i] = ts_max[i]
            
    mongo_db['Contracts'].find_and_modify({'contractId': id}, {'$set': doc}, upsert=True)
    logger.debug('MongoDB contract [%s] successfully saved with last modification dates: [%s]' % (id, doc))
