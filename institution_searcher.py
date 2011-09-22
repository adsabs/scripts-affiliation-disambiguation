#!/usr/bin/python

import solr
import sys

from local_config import SOLR_URL, HTTP_USER, HTTP_PASS, SCORE_PERCENTAGE

CONNECTION = solr.SolrConnection(SOLR_URL, http_user=HTTP_USER, http_pass=HTTP_PASS)

def search_institution(institution):
    """
    Searches an institution and returns the best match i.e. the best result.
    """
    response = CONNECTION.query(institution, defType='lucene')
    if response.numFound > 0:
        minimum_score = response.results[0]['score'] * SCORE_PERCENTAGE
        for result in response.results:
            score = float(result['score'])
            if score >= minimum_score:
                print '%.2f' % result, result['id'], result['display_name']
            else:
                break
    else:
        print 'No result found.'

if __name__ == '__main__':    
    search_institution(sys.argv[-1])
