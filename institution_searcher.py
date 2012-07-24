#!/usr/bin/python

import ConfigParser
import json
import logging
import os
import re
import solr
import sys
import time
import multiprocessing
import unicodedata

NUM_OF_CPUS = multiprocessing.cpu_count()

if not os.path.exists('var/error.log'):
    if not os.path.exists('var'):
        os.mkdir('var')
    open('var/error.log', 'w').close() 

logging.basicConfig(filename='var/error.log', level=logging.WARNING)

try:
    from celery.task import task
except ImportError:
    # If celery is not available, fake a decorator.
    def task(func):
        def empty_decorator(*args, **kwargs):
            return func(*args, **kwargs)
        return empty_decorator

cfg = ConfigParser.ConfigParser()
cfg.read('accounts.cfg')

CONNECTION = solr.SolrConnection(cfg.get('solr', 'url'),
        http_user=cfg.get('solr', 'user'),
        http_pass=cfg.get('solr', 'password'))

RE_MULTIPLE_SPACES = re.compile('\s+')

SCORE_PERCENTAGE = 0.8

def search_institution(institution, clean_up=True, logic="OR", fuzzy=False, postprocess=False, fields=('id', 'display_name', 'score')):
    """
    Searches an institution and returns the response object.
    """
    clean_institution = clean_up and _clean_affiliation(institution) or institution
    if fuzzy:
        clean_institution = re.sub('(\s|$)', r'~\1', clean_institution)
    if logic != 'OR':
        clean_institution = clean_institution.replace(' ', ' %s ' % logic)

    try:
        response = CONNECTION.query(clean_institution, fields=fields)
    except Exception, e:
        error = {
                'institution': institution,
                'clean_institution': clean_institution,
                'clean_up': clean_up,
                'time': time.asctime(),
                'exception': e.reason,
                }
        logging.error(json.dumps(error))
        return None

    results = list(response.results)
    if postprocess == True:
        process_results(clean_institution, results)

    return results

def process_results(query, results):
    """
    Perform post-processing of the results to improve the matching.
    """
    if len(results) > 1:
        if get_separation_score(results) <= 0.2:
            query = query.decode('utf_8')
            name0 = results[0]['display_name']
            name1 = results[1]['display_name']

            fquery = fingerprint(query)
            fname0 = fingerprint(name0)
            fname1 = fingerprint(name1)

            p0 = len(fname0.intersection(fquery)) / len(fname0)
            p1 = len(fname1.intersection(fquery)) / len(fname1)
            if p1 > p0:
                print 'INFO: Query "%s" now matches "%s" instead of "%s".' % (query, name1, name0)
                results[0], results[1] = results[1], results[0]

@task
def search_institutions(institutions, clean_up=True, number_of_processes=NUM_OF_CPUS - 2):
    """
    Searches for multiple institutions.
    """
    results = []

    if number_of_processes == 1:
        for institution in institutions:
            result = search_institution(institution, clean_up)
            results.append((institution, result))
    elif number_of_processes > 1:
        # Perform a parallelized search.
        task_results = []
        chunk_size = len(institutions) / number_of_processes or 1
        chunk_size = min(chunk_size, 1000)

        for chunk in (institutions[i:i+chunk_size] for i in xrange(0, len(institutions), chunk_size)):
            # Create the task and store the result object.
            try:
                r = search_institutions.delay(chunk, clean_up, number_of_processes=1)
            except AttributeError:
                print >> sys.stderr, "Error: Multiprocessing is not available without celery."
                return

            task_results.append(r)

        # Now we wait that all tasks complete.
        while any([not task_result.ready() for task_result in task_results]):
            time.sleep(0.1)

       # Extract the results.
        for task_result in task_results:
            results += task_result.result
    else:
        logging.ERROR('Incorrect number of processes: %d' % number_of_processes)
        return

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

    return (1 - score2 / score1)

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

def output_results(results):
    output = []
    for result in results:
        if not result[1]:
            output.append(result[0])
        elif len(result[1]) == 1:
            name1 = result[1][0]['display_name']
            output.append('%s\t%s' % (result[0], name1))
        else:
            name1 = result[1][0]['display_name']
            name2 = result[1][1]['display_name']
            score1 = result[1][0]['score']
            score2 = result[1][1]['score']
            ratio = score2 / score1
            output.append('%s\t%s\t%s\t%.2f' % (result[0], name1, name2, ratio))

    return '\n'.join(output).encode('utf-8')

def strip_accents(s):
    return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))

def fingerprint(s):
    """
    Returns a set of words from the string.
    """
    return set(match.group() for match in re.finditer('\w\w+', s))

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

RE_CLEAN_AFF = re.compile('[()[\]:&"]')

def _clean_affiliation(aff):
    aff = RE_CLEAN_AFF.sub(' ', aff)
    aff = re.sub('(^|\s)-', r'\1', aff)
    # Put reserved search terms in between quotes.
    aff = re.sub('(^|\s)(or|and|not|OR|AND|NOT)($|\s)', r'\1 \3', aff)
    # Hack to allow separate token search when separated by slash or semicolon.
    aff = re.sub('[;,/-]', ' ', aff)
    aff = re.sub('\s\s+', ' ', aff)
    return aff.strip()

if __name__ == '__main__':
    get_best_matches(sys.argv[-1])
