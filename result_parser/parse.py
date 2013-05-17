'''
Created on May 16, 2013

@author: developer
'''
import sys
import csv
import datetime
import argparse

class mac_event:
    def __init__(self, timestamp, mac, rssi, ssids):
        self.first_seen = timestamp
        self.last_seen = timestamp
        self.mac = mac
        self.max_rssi = int(rssi)
        self.ssids = ssids
        self.updates = 0
        self.timepoint = timestamp
        
#        print str.format("New mac_event, mac: {0}", mac)
    
    def update(self, timestamp, rssi, ssids):
#        print str.format("Update mac_event, mac: {0}", mac)
        self.last_seen = timestamp
        self.updates += 1
        for ssid in ssids:
            if ssid not in self.ssids:
                self.ssids.append(ssid)
#                print str.format("new ssid: {0}", ssid)
                
        if int(rssi) > self.max_rssi:
            self.max_rssi = int(rssi)
            self.timepoint = timestamp
#            print str.format("new max rssi: {0}", rssi)

class person_event:
    def __init__(self, timestamp, kind):
        self.timestamp = timestamp
        self.kind = kind

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('events_file')
    parser.add_argument('people_file')
    parser.add_argument('--ldt', type=int, default=180, help="Long dwell time [s]")
    parser.add_argument('--rssi', type=int, default=-85, help="RSSI cutoff [dbm]")
    parser.add_argument('--deltat', type=int, default=15, help="event delta T [s]")

    args = parser.parse_args()
    events_file = args.events_file
    people_file = args.people_file
        
    print(str.format("Wifi events file: {0} people count file: {1}", events_file, people_file))

    long_dwell_time = args.ldt
    rssi_cutoff = args.rssi
    timedelta = datetime.timedelta(seconds=args.deltat)
    print str.format("Long dwell time {0}s RSSI cutoff {1} event delta T {2}s",
                     long_dwell_time, rssi_cutoff, timedelta.seconds)
    
    
        
    people_events = []
    
    with open(people_file, 'rb') as csvfile:
        people_reader = csv.reader(csvfile, skipinitialspace=True)
        for (timestamp, event_type) in people_reader:
            parsed_time = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            people_events.append(person_event(parsed_time, event_type))
    
    people_events.sort(key=lambda key: key.timestamp)
    
    unique_macs = set()
    mac_events = dict()
    
    with open(events_file, 'rb') as csvfile:
        event_reader = csv.reader(csvfile, skipinitialspace=True)
        
        for (timestamp, mac, rssi, ssids, event_type) in event_reader:
            unique_macs.add(mac)
            parsed_time = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            parsed_ssids = ssids.split(' ')
            
            if mac in mac_events:
                if event_type == "new":
                    event_data = mac_event(parsed_time, mac, rssi, parsed_ssids)
                    mac_events[mac].append(event_data)
                else:
                    if len(mac_events[mac]) > 1:
                        event_data = mac_events[mac][-1]
                    else:
                        event_data = mac_events[mac][0]
                        
                    event_data.update(parsed_time, rssi, parsed_ssids)
            else:
                mac_events[mac] = []
#                if event_type != "new":
#                    print str.format("no mac_event for mac {0} yet but mac_event type not new in line {1}", mac, event_reader.line_num)

                event_data = mac_event(parsed_time, mac, rssi, parsed_ssids)
                mac_events[mac].append(event_data)
    
    recurring_macs = set()
    one_time_macs = set()
    long_present_macs = set()
    zero_dwell_macs = set()
    low_power_macs = set()
    
    for mac in mac_events.keys():
        events = mac_events[mac]
        if len(events) > 1:
            recurring_macs.add(mac)
        else:
            one_time_macs.add(mac)
        
        for mac_event in events:
            mac_event.dwell_time = mac_event.last_seen - mac_event.first_seen
            
            if mac_event.dwell_time.seconds > long_dwell_time:
                long_present_macs.add(mac)
            elif mac_event.dwell_time.seconds == 0:
                zero_dwell_macs.add(mac)
            
            if mac in one_time_macs and mac_event.max_rssi < rssi_cutoff:
                low_power_macs.add(mac)
            
#            print str.format("{0} dwell time {1} updates {2} max rssi {3} first seen {4} last seen {5} timepoint {6}",
#                             mac, mac_event.dwell_time.seconds, mac_event.updates, mac_event.max_rssi,
#                             mac_event.first_seen, mac_event.last_seen, mac_event.timepoint)

    print str.format("People events {0} first: {1} last: {2} timespan {3} minutes",
                     len(people_events), people_events[0].timestamp, people_events[-1].timestamp,
                     (people_events[-1].timestamp - people_events[0].timestamp).seconds / 60)
    
    print str.format("MACs unique: {0} recurring: {1} one time: {2}",
                     len(unique_macs), len(recurring_macs), len(one_time_macs))
    print str.format("Long present (>{0}s) MACs: {1} zero dwell time MACs: {2}",
                     long_dwell_time,
                     len(long_present_macs), len(zero_dwell_macs))
    print str.format("Low power (max rssi <{0}) MACs: {1}",
                     rssi_cutoff, len(low_power_macs))
    
    matching_macs_total = 0
    max_matching = 0
    min_matching = len(unique_macs)
    no_match = 0
    one_match = []
    
    for event in people_events:
        timepoint = event.timestamp
        macs = set()
        
        for mac_event_arr in mac_events.values():
            for mac_event in mac_event_arr:
                mac_timepoint = mac_event.timepoint
                hi_bound = timepoint + timedelta
                low_bound = timepoint - timedelta
                
                if (mac_timepoint < hi_bound and mac_timepoint > low_bound) and \
                    mac_event.mac not in long_present_macs and \
                    mac_event.mac not in low_power_macs:
#                    print str.format("Event times {0} {1} {2} mac {3} time {4}", 
#                                     timepoint,low_bound, hi_bound,
#                                     mac_event.mac, mac_timepoint)
                    macs.add(mac_event.mac)
        
#        print str.format("Event at {0} type {1} may match {2} MACs ",
#                         timepoint, event.kind, len(macs))
        matching = len(macs)
        matching_macs_total += matching
        if matching > max_matching:
            max_matching = matching;
        if matching < min_matching:
            min_matching = matching 
        if matching == 0:
            no_match += 1
        if matching == 1:
            one_match.append((event, macs))
            
    print str.format("matching MACs per event: average {0} min {1} max {2}",
                      matching_macs_total / len(people_events), min_matching, max_matching)
    print str.format("events without matching MAC: {0}", no_match)
    print str.format("events with exactly one matching MAC: {0}", len(one_match))
    