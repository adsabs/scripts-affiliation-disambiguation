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
logging.basicConfig(filename='error.log', level=logging.WARNING)

def search_institution(institution, clean_up=True):
    """
    Searches an institution and returns the response object.
    """
    clean_institution = clean_up and _clean_affiliation(institution) or institution

    try:
        response = CONNECTION.query(clean_institution, fields=('id', 'display_name', 'score'))
    except Exception, e:
        error = {
                'institution': institution,
                'clean_institution': clean_institution,
                'clean_up': clean_up,
                'time': time.asctime(),
                'exception': e.reason,
                }
        logging.warning(json.dumps(error))
        return None

    return response.results

@task
def search_institutions(institutions, clean_up=True, number_of_processes=1):
    """
    Searches for multiple institutions.
    """
    results = []

    if number_of_processes < 1:
        logging.ERROR('Incorrect number of processes: %d' % number_of_processes)
        return
    elif number_of_processes == 1:
        for institution in institutions:
            result = search_institution(institution, clean_up)
            results.append((institution, result))
    else:
        # Perform a parallelized search.
        task_results = []
        chunk_size = len(institutions) / number_of_processes or 1
        if chunk_size > 1000:
            # Limit maximum size of the chunks to 1,000 institutions.
            chunk_size = 1000

        for chunk in (institutions[i:i+chunk_size] for i in xrange(0, len(institutions), chunk_size)):
            # Create the task and store the result object.
            r = search_institutions.delay(chunk, clean_up, number_of_processes=1)
            task_results.append(r)

        # Now we wait that all tasks complete.
        while any([not task_result.ready() for task_result in task_results]):
            time.sleep(0.1)

       # Extract the results.
        for task_result in task_results:
            results += task_result.result

    return results

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
    # Put reserved search terms in between quotes.
    aff = re.sub('(^|\s)(OR|AND|NOT)($|\s)', r'\1"\2"\3', aff)
    return aff.strip()

if __name__ == '__main__':
    get_best_matches(sys.argv[-1])
