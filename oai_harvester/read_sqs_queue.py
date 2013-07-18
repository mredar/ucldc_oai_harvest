#!/usr/bin/env python
'''Check a queue and get times and info for messages in q'''
import sys
import os
import logging
import json
import datetime
import boto.sqs as sqs

QUEUE_OAI_HARVEST = os.environ.get('QUEUE_OAI_HARVEST', 'OAI_harvest')
QUEUE_OAI_HARVEST_ERR = os.environ.get('QUEUE_OAI_HARVEST_ERR', 'OAI_harvest_error')
QUEUE_OAI_HARVEST_HARVESTING = os.environ.get('QUEUE_OAI_HARVEST_HARVESTING', 'OAI_harvest_harvesting')

SQS_CONNECTION = sqs.connect_to_region('us-east-1')


logging.basicConfig(level=logging.INFO)

def main(args):
    q_name = args[1]
    q_harvesting = SQS_CONNECTION.get_queue(q_name)
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
        print 'Usage: read_sqs_queue.py <queue_name>'
    main(sys.argv)
