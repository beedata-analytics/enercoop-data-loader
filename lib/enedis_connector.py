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
    logger.debug('Preparing body to recover [%s] measures from Enedis service...' % (measures_type))
    body = {
        'demande': {
            'initiateurLogin': settings.ENEDIS_INIT_LOGIN_MAIL,
            'pointId': customer['csv'][settings.CONTRACT_COLUMNS['meteringPointId']],
            'contratId': settings.ENEDIS_CONTRAT_ID,
            'mesuresTypeCode': measures_type, 
            'dateDebut': from_date.strftime('%Y-%m-%d'),                    
            'dateFin': to_date.strftime('%Y-%m-%d'),
            'mesuresCorrigees': False,
            'pasCdc': {
                'valeur': 30,
                'unite': 'min'
            },
            'declarationAccordClient': {}
        }
    }
    
    if customer_type == 'residential':
        declaration = {
            'accordClient': True,
            'personnePhysique': {
                'civilite': customer['csv']['Civilité'][0] if customer['csv']['Civilité'] else 'M',
                'nom': customer['csv']['Nom'],
                'prenom': customer['csv']['Prénom']
            }
        }
    else:
        declaration = {
            'accordClient': True,
            'personneMorale': {
                'denominationSociale': customer['csv']['Nom']
            }
        }
        
    body['demande']['declarationAccordClient'] = declaration
    
    logger.debug('Body data: %s' % (body))
    data = None
    error = None
    try:
        data = ws_client.service.consulterMesuresDetaillees(**body)
        logger.debug('Measures recovered successfully from Enedis: %s' % data)
    except Exception as e:
        clean_body = deepcopy(body)
        del clean_body['demande']['declarationAccordClient']
        del clean_body['demande']['pointId']
        del clean_body['demande']['contratId']
        del clean_body['demande']['initiateurLogin']
        logger.warning('Cannot recover data from Enedis for contract %s: %s. Data sent to Enedis: %s' % (customer['document']['contractId'], e, clean_body))
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
                'unit': data['body']['aPasFixe']['unite'] if measures_type != 'CDC' else 'Wh'
            }],
            'measurements': []
        }
        
        ts = None
        n = 1
        for i, measure in enumerate(data['body']['aPasFixe']['m']):
            if not ts:
                ts = data['body']['aPasFixe']['dateHeureDebut']
            
            # timeseries datetime check
            while measure['n'] > n:
                n += 1
                if measures_type == 'CDC':
                    ts = ts + timedelta(minutes=30)
                else:
                    ts = ts + timedelta(days=1)
            
            
            ts_doc = ts
            if measures_type == 'PMAX':
                hour = int(measure['c'].split(':')[0])
                minute = int(measure['c'].split(':')[1])
                ts_doc = ts + timedelta(hours=hour) + timedelta(minutes=minute)
            elif measures_type == 'CONSOGLO':
                ts_doc = ts + timedelta(days=1) - timedelta(hours=1)
                ts_doc = ts_doc.replace(hour=22)
                
            doc['measurements'].append({
                'type': type,
                'timestamp': ts_doc.astimezone(pytz.utc).strftime(settings.DATETIME_FORMAT),
                'value': int(int(measure['v']) * 0.5) if measures_type == 'CDC' else int(measure['v']) 
            })
            
            if measures_type == 'CDC':
                ts = ts + timedelta(minutes=30)
            else:
                ts = ts + timedelta(days=1)
                
            n += 1
            
    else:
        doc = {'error': error}
        
    return doc
    
    
    
