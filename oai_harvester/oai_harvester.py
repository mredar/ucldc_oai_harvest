'''UCLDC OAI Harvester: Collects records from OAI interfaces and inputs to
basic solr schema. Driven off the collection registry'''
'''
imagining right now that this will be woken by a crontab. It then looks at the
incoming queue and processes any "READY" msgs (maybe only ready ones there)

should you have a number of queues : ready, processing, errors?
Actually, SQS uses a visibility timeout to make msgs "invisible" while being processed. Client process can up the timeout if necessary. May need similar behavior here.

while a msg in queue:
    get msg and set timeout?
    harvest from msg
    delete message from queue

'''
import sys
import csv
import os
import codecs
import datetime
import logging
logging.basicConfig(level=logging.INFO)
import json
from sickle import Sickle
from sickle.models import Record
import solr
from lxml import etree
from ucldc_queue import UCLDC_Queue
import boto.sqs as sqs

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
        print row[0:13]
        if type_harvest.lower() == 'oai':
            oai_sets.append((URL_harvest, oai_set, oai_metadata))
    return oai_sets

def harvest_oai_sets(oai_sets, dir_root=DIR_HARVEST_ROOT):
    n_harvest_recs = 0
    for (URL_harvest, oai_set, oai_metadata) in oai_sets:
        oai_set_path = ''.join([ c if c in VALID_PATH_CHARS else '_' for c in oai_set ])
        dir_records = os.path.join(DIR_HARVEST_ROOT, oai_set_path)
        if not os.path.exists(dir_records):
            os.mkdir(dir_records) 
        client=Sickle(URL_harvest)
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
        return n_harvest_recs

def harvest_to_solr_oai_set(oai_set):
    '''Harvest the oai set and return a list of records?
    The oai_set is the message dict from SQS'''
    client=Sickle(oai_set['url'])
    records = client.ListRecords(set=oai_set['set_spec'], metadataPrefix='oai_dc')
    n = 0
    dt_start = datetime.datetime.now()
    for rec in records:
        n += 1
        dt_iter = datetime.datetime.now()
        elapsed_time = (dt_iter -dt_start).seconds
        if (n % 100) == 0:
            logging.info("Set has taken :" + str(elapsed_time) + " seconds.")
            logging.info("OAI REC NUM: " + str(n) + " SET:" + str(oai_set))
        solr_index_record(rec, extra_metadata={'campus':oai_set['campus']})

def solr_index_record(sickle_rec, extra_metadata=None):
    '''Index the sickle record object in solr'''
    #TODO: make this global for efficiency?
    s = solr.Solr(URL_SOLR)
    sdoc = sickle_rec.metadata
    sdoc['id'] = sickle_rec.header.identifier
    sdoc['title_exact'] = sdoc['title'][0]
    #ADD REPO RELATION!!! AND ANY OTHER COLLECTION REGISTRY RELEVANT STUFF HERE
    if 'campus' in extra_metadata:
        for campus in extra_metadata['campus']:
            if 'publisher' in sdoc:
                sdoc['publisher'].append(campus['name'])
            else:
                sdoc['publisher'] = [campus['name'],]
    s.add(sdoc, commit=True)

def process_oai_queue():
    '''Run on any messages in the OAI_harvest queue'''
    conn=sqs.connect_to_region('us-east-1')
    q_oai = conn.get_queue(QUEUE_OAI_HARVEST) #10 min timeout, should be OK?
    n = 0 
    m = q_oai.read()
    #TODO: need to pass the message to subroutines, may need to up the
    #visibility_timeout? -- If working on mulitple machines/processes.
    #could save timeout in registry
    while m:
        n += 1
        dt_start = datetime.datetime.now()
        logging.info("\n" + str(dt_start) + " START MESSAGE " + str(n) + "\n\n")
        msg_dict = json.loads(m.get_body())
        #msg_dict is {url:XX, set_spec:YY, campus:[{resource_uri:ZZ, slug:TT, name: QQ},]}
        logging.info(msg_dict)
        harvest_to_solr_oai_set(msg_dict)
        #if ok, delete m
        dt_end = datetime.datetime.now()
        logging.info("\n\n\n============== " + str((dt_end-dt_start).seconds) + " seconds Done with Message:" + str(n) + " : " + m.get_body() +  "\n\n\n\n")
        q_oai.delete_message(m)
        m = q_oai.read()


def main(args):
    process_oai_queue()

if __name__=='__main__':
    main(sys.argv)
