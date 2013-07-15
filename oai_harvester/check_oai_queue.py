#!/usr/bin/env python
'''Check a queue and get times and info for messages in q'''
import sys
import logging
import json
import datetime
import boto.sqs as sqs
from ucldc_queue import UCLDC_Queues
#q_names = ('QUEUE_OAI_HARVEST', 'QUEUE_OAI_HARVEST_ERR', 'QUEUE_OAI_HARVEST_HARVESTING')
#from ucldc_queue import QUEUE_OAI_HARVEST, QUEUE_OAI_HARVEST_ERR, QUEUE_OAI_HARVEST_HARVESTING

logging.basicConfig(level=logging.INFO)

def main(args):
    q_name = args[1]
    queues=UCLDC_Queues()
    q_harvesting = queues.get_queue(q_name)
    msgs = q_harvesting.get_messages(num_messages=10, visibility_timeout=10, attributes='All') #copied from boto implementation of save_to_file
    while(msgs):
        for m in msgs:
            msg_dict = json.loads(m.get_body())
            print 'ID:', m.id, '\n'
            sent_dt = datetime.datetime.fromtimestamp(float(m.attributes['SentTimestamp'])/1000)
            print 'SENT AT: ', sent_dt
            print 'IN QUEUE FOR:', datetime.datetime.now()-sent_dt
            print msg_dict
            print '\n\n'
        msgs = q_harvesting.get_messages(num_messages=10, visibility_timeout=10, attributes='All')

if __name__=="__main__":
    if len(sys.argv) < 2:
        print 'Usage: check_oai_queue.py <queue_name>'
    main(sys.argv)
