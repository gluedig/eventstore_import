'''
Created on Apr 23, 2013

@author: gluedig
'''
import sqlite3
import sys
import os

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("not enough parameters")
        sys.exit(1)
    
    dbfile = sys.argv[1]
    if not os.path.exists(dbfile):
        print(str.format("db file {0} does not exist", dbfile))
        sys.exit(1)
        
    conn = sqlite3.connect(dbfile)
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM events')
    count = c.fetchone()[0]
    print(str.format("db contains {0} events", count))
    
    
    for event in c.execute('SELECT event FROM events'):
        print event[0]
    
    
    conn.close()