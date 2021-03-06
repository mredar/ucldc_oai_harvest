import sys
import csv
import os
import codecs
import logging
logging.basicConfig(level=logging.INFO)
from sickle import Sickle
from sickle.models import Record
import solr
from lxml import etree

import time

try:
    DIR_HARVEST_ROOT = os.environ['DIR_OAI_HARVEST_ROOT']
except KeyError:
    msg = "You must set the DIR_OAI_HARVEST_ROOT environment variable to save record files"
    print msg
    raise Exception(msg)

URL_SOLR = os.environ.get('URL_SOLR', 'http://54.243.192.165:8080/solr/dc-collection/')
QUEUE_OAI_HARVEST = os.environ.get('QUEUE_OAI_HARVEST', 'OAI_harvest')
HARVEST_LIST_FILE = "./UCLDC harvest collections - Sheet1.csv"

import string
VALID_PATH_CHARS = "-_. %s%s" % (string.ascii_letters, string.digits)

def get_oai_harvest_sets(oai_sets_file=HARVEST_LIST_FILE): #Eventually to read collection registry
    oai_sets = []
    config = csv.reader(open(oai_sets_file))
    for row in config:
        (campus, collection, description, access_restrictions, URL, metadata_level, metadata_standard, ready_for_surfacing, URL_harvest, nutch_regex, type_harvest, oai_set, oai_metadata) = row[0:13]
        if type_harvest.lower() == 'oai':
            oai_sets.append((URL_harvest, oai_set, oai_metadata))
    return oai_sets

def harvest_oai_sets(oai_sets, dir_root=DIR_HARVEST_ROOT):
    n_harvest_recs = 0
    for (URL_harvest, oai_set, oai_metadata) in oai_sets:
        time_start = time.time()
        oai_set_path = ''.join([ c if c in VALID_PATH_CHARS else '_' for c in oai_set ])
        dir_records = os.path.join(DIR_HARVEST_ROOT, oai_set_path)
        if not os.path.exists(dir_records):
            os.mkdir(dir_records) 
        logging.info("URL to harvest:" + URL_harvest)
        client=Sickle(URL_harvest)
        logging.info("OAI SET:"+oai_set+" MD:"+oai_metadata)
        records = client.ListRecords(set=oai_set, metadataPrefix=oai_metadata)
        for rec in records:
            rec_id_et = rec.header.xml.find("./{http://www.openarchives.org/OAI/2.0/}identifier")
            rec_id = rec_id_et.text
            #translate any bad path chars in rec_id to form filename
            rec_id_path = ''.join([ c if c in VALID_PATH_CHARS else '_' for c in rec_id ])
            with codecs.open(os.path.join(dir_records, rec_id_path+".xml"), 'w' , 'utf-8') as foo:
                foo.write(rec.raw)
            logging.debug("solr_index next on rec:"+unicode(rec))
            solr_index_record(rec)
            n_harvest_recs += 1
        time_end = time.time()
        print "OAI : ", URL_harvest, ' : ', oai_set, ' took: ', unicode(time_end-time_start)
    return n_harvest_recs

def solr_index_record(sickle_rec):
    '''Index the sickle record object in solr'''
    #TODO: make this global for efficiency?
    s = solr.Solr(URL_SOLR)
    sdoc = sickle_rec.metadata
    sdoc['id'] = sickle_rec.header.identifier
    sdoc['title_exact'] = sdoc['title'][0]
    #ADD REPO RELATION!!! AND ANY OTHER COLLECTION REGISTRY RELEVANT STUFF HERE
    logging.debug(''.join(('SDOC:\n', unicode(sdoc))))
    s.add(sdoc, commit=True)

def index_harvested_sets(dir_root=DIR_HARVEST_ROOT):
    '''Add the records created by the OAI harvest to the solr index'''
    for (root, dirs, files) in os.walk(dir_root):
        for f in files:
            fpath = os.path.join(root, f)
            with open(fpath) as fin:
                tree = etree.parse(fin)
            rec = Record(tree.getroot())
            solr_index_record(rec)
            sys.exit()

def main(args):
    "Harvest records given in csv file"
    oai_sets = get_oai_harvest_sets()
    harvest_oai_sets(oai_sets)
    #index_harvested_sets()
    #index_harvested_sets(dir_root="./data")

if __name__=='__main__':
    main(sys.argv)
