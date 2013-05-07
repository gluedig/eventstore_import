'''
Created on Apr 23, 2013

@author: gluedig
'''
import sqlite3
import sys
import os
import json
import pymongo
import datetime

def test_opers(mongodb):
    event_coll = mongodb.event_collection
    
    macs = event_coll.distinct('mac')
    print(str.format('unique MACs {0}', len(macs)))

    sta_macs = event_coll.find({'event_type': 0 }).distinct('mac')
    print(str.format('client MACs {0}', len(sta_macs)))
    
    ap_macs = event_coll.find({'event_type': 3 }).distinct('mac')
    print(str.format('AP MACs {0}', len(ap_macs)))

    
#    still_active = event_coll.aggregate([
#        {'$unwind': '$probed_ssids'},
#        {'$group': {'_id': '$probed_ssids',  'number': { '$sum': 1}}},
#        {'$sort': { 'number': -1}}
#    ])
    
#    print still_active
    
    from bson.code import Code
    mapper = Code("""function () {
    if (this.mac && this.probed_ssids) {
            emit(this.mac, {ssids: this.probed_ssids});
    }
}""")

    reducer = Code("""
function (key, values) { 
    var result = {ssids: []};
    var ssids_list = {};
    values.forEach(function(z) {
        if (z) {
            z.ssids.forEach( function(t) {
                if (!(t in ssids_list)) {
                    ssids_list[t] = true;
                    result.ssids.push(t);
                }
            });
        }
    });
    return result;
}""")    

    #result = event_coll.map_reduce(mapper, reducer, "results")
    #for doc in result.find():
    #    print doc
    
    
def sqlite_import(dbfile, mongodb):

    
    conn = sqlite3.connect(dbfile)
    try:
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM events')
        count = c.fetchone()[0]
        print(str.format("Backup db contains {0} events", count))
    
        c.execute('SELECT * FROM meta')
        (timestamp, store_id, first_event, last_event, lost_events) = c.fetchone()
        print(str.format('Backup meta: timestamp {0} store_id {1} first_event {2} last_event {3} lost_events {4}',
                         datetime.datetime.fromtimestamp(timestamp),
                         store_id, first_event, last_event, lost_events))
        
        
        import_first = first_event
        import_last = last_event
        
        store_meta = mongodb.meta_collection.find_one({"store_id": store_id})
        if store_meta:
            
            prev_first = store_meta['first_event']
            prev_last = store_meta['last_event']
            last_backup = store_meta['last_backup']
            print(str.format('Event store {0} found in db, prev_first {1} prev_last {2} last_backup {3}',
                             store_id, prev_first, prev_last,
                             datetime.datetime.fromtimestamp(last_backup)))
            
            
            if timestamp <= last_backup:
                raise Exception(str.format('Previous import timestamp {0} <= current import timestamp {1}',
                                           datetime.datetime.fromtimestamp(last_backup),
                                           datetime.datetime.fromtimestamp(timestamp)))
                
            if prev_first >= last_event:
                raise Exception(str.format('Previous import first event id {0} >= current import last event id {1}',
                                           prev_first, last_event))
                
            if prev_last >= last_event:
                raise Exception(str.format('Previous import last event id {0} >= current import last event id {1}',
                                           prev_last, last_event))
            
            if prev_last >= first_event:
                import_first = prev_last+1;
                print(str.format("Overlapping backup, importing from event {0}", import_first))
        
        new_store_meta = {
                          'store_id' : store_id,
                          'first_event' : store_meta['first_event'] if store_meta else first_event,
                          'last_event' : last_event,
                          'last_backup' : timestamp
                          }
        
        mongodb.meta_collection.update({'store_id': store_id}, new_store_meta, upsert=True)
    
        print(str.format('Importing events {0} to {1}', import_first, import_last))
        events = 0
        for event in c.execute(str.format('SELECT event FROM events WHERE id >= {0} AND id <= {1}',
                                          import_first, import_last)):
            if event and event[0] != '':
                try:
                    js = json.loads(event[0])
                    mongodb.event_collection.insert(js)
                    events += 1
                except ValueError:
                    pass
    finally:
        conn.close()
    
    print(str.format("imported {0} events, events collection now contains {1} documents",
                     events, mongodb.event_collection.count()))

    return events

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("not enough parameters, give me a backup file name")
        sys.exit(1)
    
    dbfile = sys.argv[1]
    if not os.path.exists(dbfile):
        print(str.format("db file {0} does not exist", dbfile))
        sys.exit(1)
    
    mongo = pymongo.MongoClient()
    mongo_db = mongo.test_db
            
    sqlite_import(dbfile, mongo_db)
    test_opers(mongo_db)
