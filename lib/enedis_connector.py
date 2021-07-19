# encoding: utf-8

# webservice imports
from requests.auth import HTTPBasicAuth  # or HTTPDigestAuth, or OAuth1, etc.
from requests import Session
from zeep import Client, Plugin
from zeep.transports import Transport
from lxml import etree

# time imports
import pytz
from datetime import timedelta, datetime

import logging
from copy import deepcopy
# custom imports
import settings

logger = logging.getLogger("app")


class MyloggerPlugin(Plugin):
    def ingress(self, envelope, http_headers, operation):
        #print('Response:')
        #print(etree.tostring(envelope, pretty_print=False))
        return envelope, http_headers

    def egress(self, envelope, http_headers, operation, binding_options):
        #print('Request:')
        #print(etree.tostring(envelope, pretty_print=False))
        logger.debug('Enedis response ---------')
        logger.debug(etree.tostring(envelope, pretty_print=True))    
        logger.debug('-------------------------')
        return envelope, http_headers




def init_webservice_client():
    """ Creates and initializes Enedis WebService Client"""
    session = Session()
    session.auth = HTTPBasicAuth(settings.ENEDIS_LOGIN_USER, settings.ENEDIS_LOGIN_PASSWORD)
    client = Client(settings.WSDL_PATH, transport=Transport(session=session), plugins=[MyloggerPlugin()])
    
    return client


def get_data(ws_client, customer, measures_type, customer_type, from_date, to_date):
    """ Function to recover measures from Enedis and transform them into Beedata API documents.
    
    :param ws_client: Enedis webservice client
    :param customer: Contract full dictionary from get_contracts function
    :param measures_type: one of PMAX, CDC, CONSOGLO
    :param customer_type: one of residential or tertiary
    :param from_date: where request start
    :param to_date: where request finish
    """
    logger.debug('Preparing body to recover [%s] measures from Enedis service for contract [%s]...' % (measures_type, customer['document']['contractId']))
    body = {
        'demande': {
            'initiateurLogin': settings.ENEDIS_INIT_LOGIN_MAIL,
            'pointId': customer['csv'][settings.CONTRACT_COLUMNS['meteringPointId']],
            'mesuresTypeCode': 'COURBE' if measures_type == 'CDC' else 'ENERGIE' if measures_type == 'CONSOGLO' else measures_type, 
            'dateDebut': from_date.strftime('%Y-%m-%d'),                    
            'dateFin': to_date.strftime('%Y-%m-%d'),
            'mesuresCorrigees': False,
            'soutirage': True, #TBD
            'injection': False, #TBD
            'accordClient': True,
        }
    }

    if measures_type == 'PMAX':
        body['demande']['measuresPas'] == 'P1M'
        body['demande']['grandeurPhysique'] == 'PMA'
    elif measures_type == 'CDC':
        body['demande']['grandeurPhysique'] == 'PA'
    else: # consoglo
        body['demande']['grandeurPhysique'] == 'EA'
    
    
    logger.debug('Body data: %s' % (body))
    data = None
    error = None
    try:
        data = ws_client.service.consulterMesuresDetaillees(**body)
        logger.debug('Measures recovered successfully from Enedis for contract [%s]: %s' % (customer['document']['contractId'], data))
    except Exception as e:
        clean_body = deepcopy(body)
        del clean_body['demande']['pointId']
        del clean_body['demande']['initiateurLogin']
        logger.warning('Cannot recover data from Enedis for contract [%s]: %s. Data sent to Enedis: %s' % (customer['document']['contractId'], e, clean_body))
        error = str(e)
        
        
    
    doc = None
    if data:
        type = None
        if measures_type == 'CDC':
            type = 'electricityConsumption'
        elif measures_type == 'PMAX':
            type = 'power'
        elif measures_type == 'CONSOGLO':
            type = 'dailyElectricityConsumption'
            
        doc = {
            'deviceId': customer['document']['meteringPointId'],
            'meteringPointId': customer['document']['meteringPointId'],
            'readings': [{
                'type': type,
                'period': 'INSTANT',
                'unit': data['body']['grandeur']['unite'] if measures_type != 'CDC' else 'Wh'
            }],
            'measurements': []
        }
        
        ts = None
        n = 1
        for key, measure in data['body']['grandeur']:
            if key == 'mesure':
                doc['measurements'].append({
                    'type': type,
                    'timestamp': measure['d'].astimezone(pytz.utc).strftime(settings.DATETIME_FORMAT),
                    'value': int(int(measure['v']) * 0.5) if measures_type == 'CDC' else int(measure['v']) 
                })
                    
                n += 1
            
    else:
        doc = {'error': error}
        
    return doc
    
    
    
