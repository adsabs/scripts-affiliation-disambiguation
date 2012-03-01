#!/usr/bin/python

from celery.task import task
import re
import solr
import sys

from local_config import SOLR_URL, HTTP_USER, HTTP_PASS, SCORE_PERCENTAGE

CONNECTION = solr.SolrConnection(SOLR_URL, http_user=HTTP_USER, http_pass=HTTP_PASS)

RE_CLEAN_AFF = re.compile('[()[\]:\-/&]')
RE_MULTIPLE_SPACES = re.compile('\s\s+')

def search_institution(institution, minimum_score=SCORE_PERCENTAGE):
    """
    Searches an institution and returns the best match i.e. the best result.
    """
    response = CONNECTION.query(_clean_affiliation(institution))
    if response.numFound > 0:
        minimum_score = response.results[0]['score'] * minimum_score
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
        open('/tmp/solr_errors', 'a').write(_clean_affiliation(institution) + '\n')
        raise

    if response.numFound > 0:
        first_match_name = RE_MULTIPLE_SPACES.sub(' ', response.results[0]['display_name'].strip())
        if response.numFound == 1:
            return (first_match_name, -1)
        else: 
            score = get_separation_score(response)
            return (first_match_name, score)
    else:
        return None

def get_separation_score(response):
    """
    For a Solr response, compute the separation score which is derived from the
    ratio between the two first Solr scores.
    """
    score1 = response.results[0]['score']
    score2 = response.results[1]['score']

    return (1 - score2 / score1) * 100

def get_top_results(institution, n):
    try:
        response = CONNECTION.query(_clean_affiliation(institution))
    except:
        print _clean_affiliation(institution)
        raise

    if response.numFound > 0:
        out = []
        for i in range(n):
            try:
                result = response.results[i]
            except IndexError:
                break
            score = result['score']
            name = RE_MULTIPLE_SPACES.sub(' ', result['display_name'].strip())
            out.append((score, name))
        return out
    else:
        return None

@task
def search_institutions(institutions):
    results = []
    for institution in institutions:
        match = get_match(institution)
        if match is None:
            results.append((institution, None, None))
        else:
            results.append((institution, match[0], match[1]))
    return results

@task
def match_institutions(institutions):
    results = []
    for icn, institution in institutions:
        match = get_match(institution)
        results.append((icn, institution, match))
    return results

@task
def get_match_ratio(institutions):
    results = []
    for institution in institutions:
        top_results = get_top_results(institution, 2)
        results.append((institution, top_results))
    return results

def _clean_affiliation(aff):
    aff = RE_CLEAN_AFF.sub(' ', aff)
    aff = RE_MULTIPLE_SPACES.sub(' ', aff)
    aff = re.sub('(^|\s)OR($|\s)', r'\1"OR"\2', aff)
    return aff.strip()

if __name__ == '__main__':
    search_institution(sys.argv[-1])
