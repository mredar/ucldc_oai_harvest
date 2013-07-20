#! /usr/bin/env python
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
import time
import logging
logging.basicConfig(level=logging.INFO)
import json
import traceback
import hashlib
from sickle import Sickle
from sickle.models import Record
import solr
from lxml import etree
import boto.sqs as sqs

QUEUE_OAI_HARVEST = os.environ.get('QUEUE_OAI_HARVEST', 'OAI_harvest')
QUEUE_OAI_HARVEST_ERR = os.environ.get('QUEUE_OAI_HARVEST_ERR', 'OAI_harvest_error')
QUEUE_OAI_HARVEST_HARVESTING = os.environ.get('QUEUE_OAI_HARVEST_HARVESTING', 'OAI_harvest_harvesting')

#INTIAL dev machine (nutch-dev) URL_SOLR = os.environ.get('URL_SOLR', 'http://54.243.192.165:8080/solr/dc-collection/')
URL_SOLR = os.environ.get('URL_SOLR', 'http://107.21.228.130:8080/solr/dc-collection/')

SQS_CONNECTION = sqs.connect_to_region('us-east-1')

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
        solr_index_record(rec, extra_metadata=oai_set)

def pad_partial_datestamp(dt_str):
    '''Convert any strings from OAI to a valid Solr date string
    OAI datestamp is often of form YYYY-MM-DD need to pad out the format to
    solr's format: YYYY-MM-DDTHH:MM:SSZ
    '''
    dt = dt_str
    if dt.find('T') < 0:
        dt = dt + 'T00:00:00Z'
    return dt
def get_md5_id_from_oai_identifiers(ids):
    '''From a list of oai identifier fields, pick a URL and convert to md5
    to use as solr id
    '''
    for i in ids:
        if i[:5] == 'http:':
            md5= hashlib.md5()
            md5.update(i)
            return md5.hexdigest()
    raise Exception("NO URL found in identifiers")

def solr_index_record(sickle_rec, extra_metadata=None):
    '''Index the sickle record object in solr'''
    #TODO: make this global for efficiency?
    s = solr.Solr(URL_SOLR)
    sdoc = sickle_rec.metadata
    #use URL identifier md5 hash as id
    #should probably move to solr, to help with other inputs
    sdoc['id'] = get_md5_id_from_oai_identifiers(sdoc['identifier'])
    oai_dt = pad_partial_datestamp(sickle_rec.header.datestamp)
    #collisions here?
    #sdoc['title_exact'] = sdoc['title'][0]
    # how to make created write once, then read only - update processor in
    # solr
    sdoc['created'] = sdoc['last_modified'] = oai_dt
    if 'campus' in extra_metadata:
        sdoc['campus'] = []
        for campus in extra_metadata['campus']:
            if 'publisher' in sdoc:
                sdoc['publisher'].append(campus['name'])
            else:
                sdoc['publisher'] = [campus['name'],]
            sdoc['campus'].append(campus['name'])
    sdoc['collection_name'] = extra_metadata['collection_name']
    s.add(sdoc, commit=True)

def delete_msg_by_content_from_queue(q, msg):
    '''Can't just hold an added message object, must retrieve from 
    queue and then delete. Just delete the first matching body
    '''
    m = q.read()
    while m:
        if m.get_body() == msg.get_body():
            m.delete()
            return
        m = q.read()
        
def process_oai_queue():
    '''Run on any messages in the OAI_harvest queue'''
    q_oai = SQS_CONNECTION.get_queue(QUEUE_OAI_HARVEST)
    q_harvesting = SQS_CONNECTION.get_queue(QUEUE_OAI_HARVEST_HARVESTING)
    n = 0 
    m = q_oai.read()
    while m:
        m_harvesting = q_harvesting.write(m)
        q_oai.delete_message(m) #delete, will pass result to another queue
        n += 1
        dt_start = datetime.datetime.now()
        logging.info("\n" + str(dt_start) + " START MESSAGE " + str(n) + "\n\n")
        msg_dict = json.loads(m.get_body())
        #msg_dict is {url:XX, set_spec:YY, campus:[{resource_uri:ZZ, slug:TT, name: QQ},]}
        logging.info(msg_dict)
        try:
            harvest_to_solr_oai_set(msg_dict)
            dt_end = datetime.datetime.now()
            logging.info("\n\n\n============== " + str((dt_end-dt_start).seconds) + " seconds Done with Message:" + str(n) + " : " + m.get_body() +  "\n\n\n\n")
        except Exception, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            # add message to error q
            q_err = SQS_CONNECTION.get_queue(QUEUE_OAI_HARVEST_ERR)
            msg_dict['exceptinfo'] = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
            logging.error(str(msg_dict))
            msg = json.dumps(msg_dict)
            q_msg = sqs.message.Message()
            q_msg.set_body(msg)
            status = q_err.write(q_msg)
            time.sleep(10) #make sure harvesting message back on queue
        # this doesn't work, need to "read" the message from queue to
        # get a receipt handle that can be used to delete
        #print "DELETE MESSAGE RE VAL", m_harvesting.delete()
        delete_msg_by_content_from_queue(q_harvesting, m_harvesting)
        m = q_oai.read()

def main(args):
    process_oai_queue()

if __name__=='__main__':
    #TODO: test here?
    main(sys.argv)
