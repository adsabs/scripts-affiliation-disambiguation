#!/usr/bin/python

from celery.task import task
import re
import solr
import sys

from local_config import SOLR_URL, HTTP_USER, HTTP_PASS, SCORE_PERCENTAGE

CONNECTION = solr.SolrConnection(SOLR_URL, http_user=HTTP_USER, http_pass=HTTP_PASS)

RE_CLEAN_AFF = re.compile('[()[\]:\-/&]')
RE_MULTIPLE_SPACES = re.compile('\s\s+')

def search_institution(institution):
    """
    Searches an institution and returns the best match i.e. the best result.
    """
    response = CONNECTION.query(_clean_affiliation(institution))
    if response.numFound > 0:
        minimum_score = response.results[0]['score'] * SCORE_PERCENTAGE
        for result in response.results:
            score = float(result['score'])
            if score >= minimum_score:
                print '%.2f' % score, result['id'], result['display_name']
            else:
                break
    else:
        print 'No result found.'

def get_match(institution):
    try:
        response = CONNECTION.query(_clean_affiliation(institution))
    except:
        print _clean_affiliation(institution)
        raise

    if response.numFound > 0:
        return RE_MULTIPLE_SPACES.sub(' ', response.results[0]['display_name'].strip())
        
    else:
        return None

@task
def match_institutions(institutions):
    results = []
    for icn, institution in institutions:
        match = get_match(institution)
        results.append((icn, institution, match))
    return results

def _clean_affiliation(aff):
    aff = RE_CLEAN_AFF.sub(' ', aff)
    aff = RE_MULTIPLE_SPACES.sub(' ', aff)
    return aff.strip()

if __name__ == '__main__':
    search_institution(sys.argv[-1])
