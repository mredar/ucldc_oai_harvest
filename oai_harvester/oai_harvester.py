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
from ucldc_queue import UCLDC_Queues, QUEUE_OAI_HARVEST, QUEUE_OAI_HARVEST_ERR
import boto.sqs as sqs

URL_SOLR = os.environ.get('URL_SOLR', 'http://54.243.192.165:8080/solr/dc-collection/')


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
    queues=UCLDC_Queues()
    q_oai = queues.get_queue(QUEUE_OAI_HARVEST)
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
        try:
            harvest_to_solr_oai_set(msg_dict)
            dt_end = datetime.datetime.now()
            logging.info("\n\n\n============== " + str((dt_end-dt_start).seconds) + " seconds Done with Message:" + str(n) + " : " + m.get_body() +  "\n\n\n\n")
        except Exception, e:
            # add message to error q, will be deleted from incoming
            q_err = queues.get_queue(QUEUE_OAI_HARVEST_ERR)
            msg_dict['excp'] = str(e)
            msg = json.dumps(msg_dict)
            q_msg = sqs.message.Message()
            q_msg.set_body(msg)
            status = q_err.write(q_msg)
        q_oai.delete_message(m)
        m = q_oai.read()

def main(args):
    process_oai_queue()

if __name__=='__main__':
    #TODO: test here?
    main(sys.argv)
