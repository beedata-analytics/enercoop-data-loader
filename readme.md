HOW SCRIPT WORKS



# Requirements

- python3 virtual environment
-- install `requirements.txt` on virtualenv
- set `PYTHONIOENCODING=utf-8` environment variable to avoid issues reading files with utf-8 encoding
- MongoDB installation
- Define `settings.py`



# settings.py

A lot of settings can be defined here for future changes on formats. 
Important values to set are:

	- BEEDATA_LOGIN_USER = username
	- BEEDATA_LOGIN_PASSWORD = password
	- BEEDATA_CRT_PATH = .crt file path
	- BEEDATA_KEY_PATH = .key file path
	- BEEDATA_COMPANYID = 10-digit number
	
	- ENEDIS_LOGIN_USER = enedis user
	- ENEDIS_LOGIN_PASSWORD = enedis password
	- ENEDIS_INIT_LOGIN_MAIL = init login mail
	- ENEDIS_CONTRAT_ID = enedis contrat id
	- WSDL_PATH = .wsdl file path
	
	- MONGO_HOST = ip or 'localhost'
	- MONGO_DBNAME = db where information will be stored
	- MONGO_USERNAME = username
	- MONGO_PASSWORD = password
	
	- ANONYMIZE_KEY = secret string to keep PDL anonymized
	
	- MODIFICATIONS = number of contracts modifications per row on contracts CSV
	
	- Delimiters and datetime format for every CSV file
	- Dictionaries that maps CSV columns to Beedata needed fields for different CSV. If row names or format changes, they should be changed too
	


# WSDL files

On my tests, with ConsultationMesuresDetaillees-v1.0.wsdl provided I have


    /Dictionnaries		(also provided)
    /Enercoop
	    /Enercoop
		    ConsultationMesuresDetaillees-v1.0.wsdl
		    
		
If script fails to read the WSDL file inner references they may be changed manually (which are set to relative paths like ../../Dictionnaries/)
	
	



# Run the script

Once everything is setup we can run by calling the main `task.py` script like follows:

`python task.py --contracts path/to/contracts.csv --authorizations path/to/authorizations.csv --hours path/to/hours.csv --loglevel INFO --type PMAX`

If more verbose information is needed we can set --loglevel at DEBUG wich will verbose everything, including detailed information.

Parameter `--type` is optional. All measures will be fetched it is not set.

If it works correctly it should add information to the defined MongoDB database collections:

- Contracts: information about contracts including
 
	- contractId: main connector between Enercoop and Beedata
	- etag: Beedata document etag. Build when reading contracts from CSV, and used to determine if a contract should be modified on Beedata API or not.
	- prm: pdl
	- meteringPointId: anonymized pdl
	- last_measure_type: last measure recovered for every type

- Reports: every time script works properly it creates a report document that can be inspected to look for errors or different issues. It will have information like:
	- num_contracts: contracts processed on contracts CSV for that execution
	- start: when script started
	- finish_at: when script finished
	- results: a document for every contract processed
		- start: when contract process started
		- finish: when contract process finished
		- contractId:
		- contracts_report: which call was performed (if any) and its result
		- measures_report: for every type of call
			- measures: number of measures recovered for the call
			- from_date: from date for the enedis call
			- to_date: to date for enedis call
			- beedata_call_status: 200 if everything went well
			- beedata_call_error: if status with beedata was unexpected
			

		
	 
