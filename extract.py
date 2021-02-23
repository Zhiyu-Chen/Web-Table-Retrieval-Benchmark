from metadata import *
from glob import glob
from collections import defaultdict
import os, tarfile, json, csv
import pandas as pd

'''
match the WDC dump with the entity linking paper
1. read WDC: table id -> file name
2. linking data: table id -> entities
3. merge and prepare index
'''

def get_tid_entities():
    '''
    index of tables for entities  -> first wrong version. Corrected with get_table_entities() in indexer.py
    :return: tid:entities
    '''
    t2e = defaultdict(list)
    with open(match_file, 'r') as f_match:
        for line in f_match:
            tid = line.split('.json')[0]
            t2e[tid].append(line.split(',')[1].strip())
    return t2e

def get_table_entities():
    '''
    index of tables for entities
    :return: tid:entities
    '''
    t2e = defaultdict(list)
    f = open(match_file, 'r')
    csv_reader = csv.reader(f)
    for d in csv_reader:
        tid = d[0].split('.json')[0]
        t2e[tid].append(d[1])
    return t2e


def match_WDC():
    '''
    link WDC tables to entity table and generate matched results
    :return:
    '''
    t2e = get_table_entities() # size 3035305 tid 1438042990611.52_20150728002310-00280-ip-10-236-191-2_432654501_12
    f_dump = open(webtable_dump,'w')
    for i in range(51):
        if i < 10:
            f_no = "0" + str(i)
        else:
            f_no = str(i)

        fname = os.path.join(wdc_path,f_no+'.tar')
        print(fname)
        tar = tarfile.open(fname,"r")
        for member in tar.getmembers():
            if member.isdir():
                continue
            f=tar.extractfile(member)
            content=f.read()
            content = json.loads(content)
            json_name = member.name #0/1438042988458.74_20150728002308-00254-ip-10-236-191-2_867263717_6.json
            id_start = json_name.find('/')
            tid = json_name.split('.json')[0][id_start+1:]
            if tid not in t2e:
                continue
            content['json_loc'] = member.name
            content['entities'] = t2e[tid]
            f_dump.write(json.dumps(content) + '\n')

    f_dump.close()


def check_miss():
    '''
    from matched result, check the missed entity tables
    :return:
    '''
    f_wdc = open(webtable_dump,'r')
    wdc_tids = set()
    for line in f_wdc:
        line = json.loads(line)
        json_loc = line['json_loc']
        id_start = json_loc.find('/')
        tid = json_loc.split('.json')[0][id_start + 1:]
        wdc_tids.add(tid)

    t2e = get_tid_entities()
    for tid in t2e:
        if tid not in wdc_tids:
            print(tid)
            break


def check_match_tid():
    in_names = []
    out_names = []
    t2e = get_tid_entities()
    for i in range(51):
        if i < 10:
            f_no = "0" + str(i)
        else:
            f_no = str(i)
        fname = os.path.join(wdc_path,f_no+'.tar')
        print(fname)
        tar = tarfile.open(fname,"r")
        for member in tar.getmembers():
            if member.isdir():
                continue
            f=tar.extractfile(member)
            content=f.read()
            content = json.loads(content)
            json_name = member.name #0/1438042988458.74_20150728002308-00254-ip-10-236-191-2_867263717_6.json
            id_start = json_name.find('/')
            tid = json_name.split('.json')[0][id_start+1:]
            if tid not in t2e:
                out_names.append(json_name)
            else:
                in_names.append(json_name)
    return in_names,out_names


if __name__ == '__main__':
    match_WDC()