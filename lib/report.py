# encoding: utf-8

# utils imports
from datetime import datetime

class Report(object):
    
    def __init__(self, mongo_db, collection='Reports'):
        self.mongo_db = mongo_db
        self.collection = self.mongo_db[collection]
        self.report = {
            'start': datetime.now(),
            'status': 'RUNNING',
            'results': {}
        }
        self.id = None
        
    def save(self):
        if not self.id:
            self.id = self.collection.insert_one(self.report)
        else:
            self.collection.find_one_and_update({'_id': self.id}, self.report, upsert=True)
            
        
    def add_num_contracts(self, n):
        self.report['num_contracts'] = n
        
    def add_results(self, cid, result):
        self.report['results'][cid] = result
        
    def finish(self):
        self.report['status'] = 'FINISHED'
        self.report['finish_at'] = datetime.now()
        
        self.save()
    
        
    

