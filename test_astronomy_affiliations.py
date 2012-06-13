from collections import defaultdict
import re
import sys

import institution_searcher as s
import spreadsheet_interface as spreadsheet
from ads_refine.clean_ads_affiliations import _preclean_affiliation
from ads.Unicode import UnicodeHandlerError

def get_affiliations(path='/proj/ads/abstracts/ast/update/affils.jan12.merged'):
    """
    Reads the affiliations from an affiliation file and returns a dictionary
    {affiliation: number of occurrences}.
    """
    affiliations = defaultdict(int)
    for line in open(path):
        try:
            affiliation = line.decode('utf-8').strip().rsplit('\t', 1)[-1]
            affiliation = _preclean_affiliation(affiliation)
        except (UnicodeHandlerError, UnicodeDecodeError):
            print 'Error:', affiliation
            continue

        # Remove emails.
        affiliation = re.sub('<EMAIL>[^<]*(<\/EMAIL>|$)', ' ', affiliation)
        affiliation = re.sub('\(?[a-zA-Z0-9.-]+@[a-zA-Z,.-]+\)?', ' ', affiliation)
        # Format spaces.
        affiliation = re.sub('\s\+', ' ', affiliation.strip())
        affiliations[affiliation.encode('utf8')] += 1

    return affiliations

def output_results(results, path):
    lines = []
    for affiliation, match, score in results:
        affiliation = affiliation.encode('utf-8')
        if match is None:
            match = ''
        else:
            match = match.encode('utf-8')
        if score is None:
            score = ''
        else:
            score = str(score)

        lines.append('\t'.join((affiliation, match, score)))
    out = '\n'.join(lines)
    open(path, 'w').write(out)

def upload_matched(matched):
    output = []
    matched = [(affiliations[aff], aff, res) for aff, res in matched]
    print 'Found %d matched affiliations.' % len(matched)
    for number, aff, res in sorted(matched, key=lambda r: int(r[0]), reverse=True)[:2500]:
        d = {'affiliation': aff, 'number': str(number)}
        d['first'] = res[0]['display_name']
        if len(res) > 1:
            d['second'] = res[1]['display_name']
            d['ratio'] = str(1 - res[1]['score'] / res[0]['score'])
        output.append(d)
    print 'Exporting 2,500 results to Google Docs.'

    spreadsheet.connect()
    spreadsheet.upload_data(output, 'Matched')

def upload_unmatched(unmatched):
    output = [{'affiliation': r[0], 'number': str(affiliations[r[0]])} for r in unmatched]
    output = sorted(output, key=lambda r: int(r['number']), reverse=True)
    print 'Found %d unmatched affiliations.' % len(output)
    print 'Exporting to Google Docs.'

    spreadsheet.connect()
    spreadsheet.upload_data(output, 'Unmatched')

if __name__ == '__main__':
    # Run the affiliation disambiguation and upload the unmatched affiliations
    # to Google Docs.
    affiliation_file = sys.argv[-1]
    print 'Reading affiliations from %s.' % affiliation_file
    try:
        affiliations = get_affiliations(path=affiliation_file)
    except IOError, e:
        print 'Impossible to read file: %s' % e

    print 'Got %d affiliations.' % len(affiliations)
    print 'Disambiguating...'
    res = s.search_institutions(affiliations.keys())
    print 'Done disambiguating.'
    unmatched = [r for r in res if not r[1]]
    upload_unmatched(unmatched)
    matched = [r for r in res if r[1]]
    upload_matched(matched)
