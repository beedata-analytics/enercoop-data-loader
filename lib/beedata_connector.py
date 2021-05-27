# encoding: utf-8
import urllib3
import settings
from requests import request, Session
from json import dumps
import logging
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class BaseClient(object):
    """ Client to hold a single connection and avoid login issues """
    def __init__(self):
        # read required data from settings and set them
        self.cookie = None
        self.http_headers = {
            'Content-type': 'application/json',
            'X-CompanyId': str(settings.BEEDATA_COMPANYID)
        }
        self.certificate = (settings.BEEDATA_CRT_PATH, settings.BEEDATA_KEY_PATH)
        self.base_url = settings.BEEDATA_BASE_URL
        self.username = settings.BEEDATA_LOGIN_USER
        self.password = settings.BEEDATA_LOGIN_PASSWORD

    def do_login(self):
        """ Perform login request to set authentication cookie""" 
        data = {'username': self.username, 'password': self.password}
        # login request
        response = request('POST', self.base_url + '/authn/login',
                           data=dumps(data), headers=self.http_headers, verify=False)
        # setting cookie for response
        self.cookie = {'iPlanetDirectoryPro': response.json()['token']}

        return self.cookie

    def do_logout(self):
        """ Perform logout request"""
        # logout request
        request('GET', self.base_url + '/authn/logout', headers=self.http_headers, verify=False)

    def get_contract(self, contract_id):
        
        response = request('GET', settings.BEEDATA_BASE_URL + settings.BEEDATA_ENDPOINTS['contracts'] + '/%s' % contract_id,
                           cookies=self.cookie or self.do_login(), headers=self.http_headers,
                           cert=self.certificate, verify=False)
        
        contract = None
        if response.status_code == 200:
            contract = response.json()
        
        return contract
    
    
    def send_data(self, data, type):
        """ Function to just POST data
        
        :param data: document to POST
        :param type: one of contracts or measures
        """
        s = Session()

        retries = Retry(total=5,
                backoff_factor=0.5,
                status_forcelist=[ 500, 502, 503, 504 ])
        
        s.mount('https://', HTTPAdapter(max_retries=retries))
        
        #response = request('POST', settings.BEEDATA_BASE_URL + settings.BEEDATA_ENDPOINTS[type],
        response = s.post(settings.BEEDATA_BASE_URL + settings.BEEDATA_ENDPOINTS[type],
                           cookies=self.cookie or self.do_login(), headers=self.http_headers,
                           cert=self.certificate, verify=False,
                           data=dumps(data))
            
        
        return response
        

    def modify_contract(self, data):
        """ Function to PATCH contract getting its etag from BeeData API.
        
        :param data: data to send on PATCH
        """
        # GET request to recover _etag field from BeeData API
        response = request('GET', settings.BEEDATA_BASE_URL + settings.BEEDATA_ENDPOINTS['contracts'] + '/%s' % data['contractId'],
                           cookies=self.cookie or self.do_login(), headers=self.http_headers,
                           cert=self.certificate, verify=False)
        response = response.json()

        # If the contract already exists in BeeData a PATCH is needed else we need to create the new contract in BeeData
        if '_etag' in response:
            # add If-Match header
            headers = {
                'Content-type': 'application/json',
                'X-CompanyId': str(settings.BEEDATA_COMPANYID),
                'If-Match': response['_etag']
            }
            # PATCH request
            response = request('PATCH', settings.BEEDATA_BASE_URL + settings.BEEDATA_ENDPOINTS['contracts'] + '/%s' % data['contractId'],
                               cookies=self.cookie or self.do_login(), headers=headers,
                               cert=self.certificate, verify=False,
                               data=dumps(data))
        else:
            response = request('POST', settings.BEEDATA_BASE_URL + settings.BEEDATA_ENDPOINTS['contracts'],
                               cookies=self.cookie or self.do_login(), headers=self.http_headers,
                               cert=self.certificate, verify=False,
                               data=dumps(data))
            
        return response
