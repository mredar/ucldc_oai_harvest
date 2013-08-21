#! /usr/bin/env python
'''Find the best guess item url from the DC "identifier" field stored
in the solr docs
'''

import os, sys
from collections import defaultdict
import solr

URL_SOLR = os.environ.get('URL_SOLR', 'http://107.21.228.130:8080/solr/dc-collection/')


#this is the list of acceptable input to solr index, needed because the 
#doc returned by solrpy has additional fields that err out on update.
INPUT_DOC_KEYS = ('id', 'collection_name', 'campus', 'repository',
                  'identifier', 'title', 'contributor', 'coverage',
                  'creator', 'date', 'description', 'format', 'identifier',
                  'language', 'publisher', 'relation', 'rights', 'source',
                  'subject', 'type', 
                  )

def main(args):
#for initial pass, just take first url in identifier
    if len(args) > 3:
        print 'Usage: find_item_url.py <query: default: *:*> <fq term e.g. \'campus:"San Diego"\'>'
        sys.exit(1)
    s = solr.Solr(URL_SOLR)
    fq = '!url_item:[* TO *]'
    query = "*:*"
    if len(args) >= 2:
        query = args[1]
    if len(args) == 3:
        fq = args[2]
    sort = 'id asc'
    print "Q:", query, " FQ:", fq
    ids = defaultdict(int)
    n_added = n_retrieved = 0
    resp = s.select(query, fq=fq, sort=sort, start=n_retrieved)
    while(resp):
        for hit in resp.results:
            n_retrieved += 1
            ids[hit['id']] += 1
            if ids[hit['id']] > 1:
                print "DUP:::", hit['id'], " --> ", str(ids[hit['id']]), " N:", str(n_retrieved)
                continue
            url_identifiers = []
            for i in hit['identifier']:
                if (i[:5] == 'http:') or (i[:6] == 'https:'):
                    url_identifiers.append(i)
            #choose the "best" url to grab an image from
            if len(url_identifiers) == 0:
                print "+++NO URL ID FOR:", hit['id']
                continue
            url_item = url_identifiers[0]
            solr_up_doc = {'url_item':url_item}
            for key, val in hit.items():
                if key in INPUT_DOC_KEYS:
                    solr_up_doc[key] = val
            print solr_up_doc['id'], solr_up_doc['url_item']
            s.add(solr_up_doc)
            n_added += 1
            if n_added % 100 == 0:
                s.commit()
                print n_added, " url_item added"
                #print "Last item:", solr_up_doc['id'], solr_up_doc['url_item']
        print "NEXT Q:", query, fq, str(n_retrieved)
        #resp = s.select(query, fq=fq, start=n_retrieved)
        s.commit()
        resp = resp.next_batch()
    print "NUM", n_added, '/', n_retrieved

if __name__=='__main__':
    main(sys.argv)
