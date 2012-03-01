#!/usr/bin/python

from celery.task import task
import re
import solr
import sys
import time
import json

from local_config import SOLR_URL, HTTP_USER, HTTP_PASS, SCORE_PERCENTAGE

CONNECTION = solr.SolrConnection(SOLR_URL, http_user=HTTP_USER, http_pass=HTTP_PASS)

RE_CLEAN_AFF = re.compile('[()[\]:\-/&]')
RE_MULTIPLE_SPACES = re.compile('\s\s+')

import logging
logging.basicConfig(filename='error.log', level=logging.DEBUG)

def search_institution(institution, clean_up=True):
    """
    Searches an institution and returns the response object.
    """
    if clean_up:
        institution = _clean_affiliation(institution)

    try:
        response = CONNECTION.query(institution)
    except Exception, e:
        error = {
                'institution': institution,
                'clean_up': clean_up,
                'time': time.asctime(),
                'exception': e.reason,
                }
        logging.warning(json.dumps(error))
        return None

    return response.results

@task
def search_institutions(institutions, clean_up=True):
    """
    Searches for multiple institutions.
    """
    results = []
    for institution in institutions:
        result = search_institution(institution, clean_up)
        results.append((institution, result))

    return results

def search_institutions_parallel(institutions, clean_up=True, number_of_processes=20):
    """
    Search for multiple institutions with multiple processes.
    """
    if number_of_processes <= 0:
        print 'Incorrect number of processes: %d' % number_of_processes
        return

    results = []
    chunk_size = len(institutions) / number_of_processes or 1

    while institutions:
        # Get a chunk of institutions.
        chunk = institutions[:chunk_size]
        institutions = institutions[chunk_size:]
        # Create the task and store the result object.
        r = search_institutions.delay(chunk)
        results.append(r)

    # Now we wait that all tasks complete.
    while not any([not result.ready() for result in results]):
        time.sleep(0.1)

   # Extract the results.
    out = []
    for result in results:
        out += result.result

    return out

def get_best_matches(institution, minimum_score=SCORE_PERCENTAGE):
    """
    Searches an institution and returns the best match i.e. the best result.
    """
    results = search_institution(institution)
    if results:
        minimum_score = results[0]['score'] * minimum_score
        for result in results:
            score = float(result['score'])
            if score >= minimum_score:
                print '%.2f' % score, result['id'], result['display_name']
            else:
                break
    else:
        print 'No result found.'

def get_match(institution):
    try:
        results = search_institution(institution)
    except:
        print _clean_affiliation(institution)
        open('/tmp/solr_errors', 'a').write(_clean_affiliation(institution) + '\n')
        raise

    if results:
        first_match_name = RE_MULTIPLE_SPACES.sub(' ', results[0]['display_name'].strip())
        if len(results) == 1:
            return (first_match_name, -1)
        else:
            score = get_separation_score(results)
            return (first_match_name, score)
    else:
        return None

def get_separation_score(results):
    """
    For a Solr response, compute the separation score which is derived from the
    ratio between the two first Solr scores.
    """
    score1 = results[0]['score']
    score2 = results[1]['score']

    return (1 - score2 / score1) * 100

def get_top_results(institution, n):
    try:
        results = search_institution(institution)
    except:
        print _clean_affiliation(institution)
        raise

    if results:
        out = []
        for i in range(n):
            try:
                result = results[i]
            except IndexError:
                break
            score = result['score']
            name = RE_MULTIPLE_SPACES.sub(' ', result['display_name'].strip())
            out.append((score, name))
        return out
    else:
        return None

@task
def get_best_matchess(institutions):
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
    get_best_matches(sys.argv[-1])
