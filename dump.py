'''
Created on Apr 23, 2013

@author: gluedig
'''
import sqlite3
import sys
import os
import json
import pymongo

def sqlite_import(dbfile, mongo_collection):

    conn = sqlite3.connect(dbfile)
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM events')
    count = c.fetchone()[0]
    print(str.format("db contains {0} events", count))

    events = 0
    for event in c.execute('SELECT event FROM events'):
        if event and event[0] != '':
            try:
                js = json.loads(event[0])
                mongo_collection.insert(js)
                events += 1
            except ValueError:
                pass

    conn.close()
    print(str.format("parsed {0} events, inserted {1} documents", events, mongo_collection.count()))

    return events

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("not enough parameters")
        sys.exit(1)
    
    dbfile = sys.argv[1]
    if not os.path.exists(dbfile):
        print(str.format("db file {0} does not exist", dbfile))
        sys.exit(1)
    
    mongo = pymongo.MongoClient()
    mongo_db = mongo.test_db
    mongo_coll = mongo_db.test_collection
    mongo_coll.remove()
    
    sqlite_import(dbfile, mongo_coll)
    macs = mongo_coll.distinct('mac')
    print(str.format('unique MACs {0}', len(macs)))

    sta_macs = mongo_coll.find({'event_type': 0 }).distinct('mac')
    print(str.format('client MACs {0}', len(sta_macs)))
    
    ap_macs = mongo_coll.find({'event_type': 3 }).distinct('mac')
    print(str.format('AP MACs {0}', len(ap_macs)))
    
#    still_active = mongo_coll.aggregate([
#        {'$unwind': '$probed_ssids'},
#        {'$group': {'_id': '$probed_ssids',  'number': { '$sum': 1}}},
#        {'$sort': { 'number': -1}}
#    ])
#    
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

    result = mongo_coll.map_reduce(mapper, reducer, "results")
    for doc in result.find():
        print doc