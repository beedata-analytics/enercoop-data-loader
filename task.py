# encoding: utf-8

# utils imports
import argparse
import logging
from datetime import datetime
from multiprocessing import Pool


# custom imports
from lib.utils import get_contracts, process_contract, connect_mongo
from lib.report import Report


mongo_db = connect_mongo()
report = Report(mongo_db)


def setup_logger(args):
    loglevel = args.loglevel.upper()
    numeric_level = getattr(logging, loglevel, None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logger = logging.getLogger("app")
    logger.setLevel(numeric_level)
    formatter = logging.Formatter('PID [%(process)d] - %(asctime)s - %(levelname)s - %(message)s')
    
    # File handler to output to .log file
    log_file = "beedata_script_%s.log" % datetime.now().strftime("%Y-%m-%dT%H_%M_%SZ")
    ch = logging.FileHandler(log_file, 'w', 'utf-8')
    ch.setLevel(numeric_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    # Stream handler to output to CLI
    sh = logging.StreamHandler()
    sh.setLevel(numeric_level)
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    
    
    logging.getLogger("zeep").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    return logger



def multi_load(item):
    margindays = args.margindays
    measure_types = args.type
    result = process_contract(
        item['contract'], 
        item['data'], 
        item['type'],
        margindays,
        measure_types
    )
    
    if result:
        report.add_results(item['contract'], result)

def run(args):
    """Main thread. Get parameters from CLI and decides when to run with single thread or using multiprocess
    
    :param args: argparse arguments containing, at least: contracts, authorizations and hours files path, processes and margindays
    """
    logger = setup_logger(args)
    logger.info('Starting script... ')
    
    contracts = get_contracts(args)
    report.add_num_contracts(len(contracts.keys()))
    
    margindays = args.margindays
    measure_types = args.type
    
    # process every contract (row on the CSV)
    if args.processes == 1:
        logger.info('Processing files with single thread')
        for contract, data in contracts.items():
            result = process_contract(contract, data, data['contract_type'], #mongo_db, ws_client, beedata_client, 
                                      margindays, measure_types)
            if result:
                report.add_results(contract, result)
    else:
        logger.info('Processing files with [%s] threads' % args.processes)
        dict_list = []
        for contract, data in contracts.items():
            dict_list.append({
                'contract': contract,
                'data': data,
                'type': data['contract_type']
            })
        pool = Pool(processes=int(args.processes))
        pool.map(multi_load, dict_list)
    report.finish()
    logger.info('Script finished. ')
    
    


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Main script to create and load data (contracts and measures) to BeeData API')
    parser.add_argument('--contracts', required=True,
                        help='Contracts CSV to process')
    parser.add_argument('--authorizations', required=True,
                        help='Authorization CSV file')
    parser.add_argument('--hours', required=True,
                        help='Detail for discrimination hours per PRM CSV file')
    parser.add_argument('--loglevel', type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO',
                        help='Log Level. Can be set to DEBUG, INFO, WARNING or ERROR')
    parser.add_argument('--processes', type=int, default=1,
                        help='Number of threads used to process (max should be lower than the number of cores available). Default to 1.')
    parser.add_argument('--margindays', type=int, default=10,
                        help='Number of days to let some margin. It will set "top" date as: today - margindays. Default to 10.')
    parser.add_argument('--type', type=str, choices=['PMAX', 'CONSOGLO', 'CDC', 'ALL'], default='ALL',
                        help='Measures type to recover.')
    # reading command line arguments
    args = parser.parse_args()
    
    # start
    run(args)
