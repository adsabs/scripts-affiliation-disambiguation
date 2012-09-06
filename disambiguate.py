from collections import defaultdict
import os
import re
import time

import institution_searcher as s
import spreadsheet_interface
from clean_ads_affiliations import _preclean_affiliation
from ads.Unicode import UnicodeHandlerError

STATS = {}

def get_affiliations(path):
    """
    Reads the affiliations from an affiliation file and returns a dictionary
    {affiliation: number of occurrences}.
    """
    affiliations = defaultdict(int)
    affiliation_number, problem_affiliation_number = 0, 0

    tagged_email_regex = re.compile('<EMAIL>[^<]*(<\/EMAIL>|$)')
    untagged_email_regex = re.compile('\(?[a-zA-Z0-9.-]+@[a-zA-Z,.-]+\)?')
    spaces_regex = re.compile('\s\s+')

    for line in open(path):
        try:
            affiliation = line.decode('utf-8').strip().rsplit('\t', 1)[-1]
            affiliation = _preclean_affiliation(affiliation)
        except (UnicodeHandlerError, UnicodeDecodeError):
            print 'Error:', affiliation
            problem_affiliation_number += 1
            continue

        # Remove emails.
        affiliation = tagged_email_regex.sub(' ', affiliation)
        affiliation = untagged_email_regex.sub(' ', affiliation)
        # Format spaces.
        affiliation = spaces_regex.sub(' ', affiliation.strip())
        affiliations[affiliation.encode('utf_8')] += 1
        affiliation_number += 1

    STATS.update({
        'affs': affiliation_number,
        'problemaffs': problem_affiliation_number,
        'uniqueaffs': len(affiliations),
        })
    return affiliations

def output_results(results, path):
    lines = []
    for affiliation, match, score in results:
        affiliation = affiliation
        if match is None:
            match = ''
        else:
            match = match
        if score is None:
            score = ''

        lines.append('\t'.join((affiliation, match, score)))
    out = '\n'.join(lines)
    open(path, 'w').write(out)

def upload_matched(matched, spreadsheet_name, output_number, affiliations):
    output = []
    matched = [(affiliations[aff], aff, res) for aff, res in matched]
    print 'Found %d matched affiliations.' % len(matched)
    for number, aff, res in sorted(matched, key=lambda r: int(r[0]), reverse=True)[:output_number]:
        d = {'affiliation': aff, 'number': number}
        d['first'] = res[0]['display_name']
        if len(res) > 1:
            d['second'] = res[1]['display_name']
            d['confidence'] = '%.2f' % (1 - res[1]['score'] / res[0]['score'])
        output.append(d)
    print 'Exporting %d results to Google Docs.' % len(output)

    spreadsheet_interface.upload_data(output, spreadsheet_name, 'Matched')

def upload_unmatched(unmatched, spreadsheet_name, output_number, affiliations):
    output = [{'affiliation': r[0], 'number': affiliations[r[0]]} for r in unmatched]
    output = sorted(output, key=lambda r: int(r['number']), reverse=True)[:output_number]
    print 'Found %d unmatched affiliations.' % len(output)
    print 'Exporting %d results to Google Docs.' % len(output)

    spreadsheet_interface.upload_data(output, spreadsheet_name, 'Unmatched')

def main(affiliation_file, spreadsheet_name, everything, output_number):
    """
    Run the affiliation disambiguation and upload the unmatched affiliations
    to Google Docs.
    """
    STATS['datetime'] = time.asctime()
    STATS['affiliationfile'] = os.path.basename(affiliation_file)
    print 'Reading affiliations from %s.' % affiliation_file
    try:
        affiliations = get_affiliations(affiliation_file)
    except IOError, e:
        print 'Impossible to read file: %s' % e

    print 'Found %d unique affiliations.' % len(affiliations)

    if not everything:
        # Let's just disambiguate the most frequent affiliations.
        affiliations = dict(sorted(affiliations.items(), key=lambda aff: aff[1], reverse=True)[:output_number])

    print 'Disambiguating %d affiliations...' % len(affiliations)
    res = s.search_institutions(affiliations.keys())
    print 'Done disambiguating.'

    spreadsheet_interface.connect()
    unmatched = [r for r in res if not r[1]]
    STATS['unmatched'] = len(unmatched)
    upload_unmatched(unmatched, spreadsheet_name, output_number, affiliations)
    matched = [r for r in res if r[1]]
    STATS['matched'] = len(matched)
    upload_matched(matched, spreadsheet_name, output_number, affiliations)

    spreadsheet_interface.upload_statistics(STATS, spreadsheet_name)

if __name__ == '__main__':
    from optparse import OptionParser
    usage = "usage: %prog [options] affiliation_file spreadsheet_name"
    parser = OptionParser(usage=usage)
    parser.add_option("-n", "--output-number", dest="output_number", default='1000',
            help="number of affs to output", metavar="OUTPUT_NUMBER")
    parser.add_option("-e", "--everything",
            action="store_true", dest="everything", default=False,
            help="find both matched and unmatched affiliations")

    options, args = parser.parse_args()
    if len(args) != 2:
        parser.error("incorrect number of arguments")
    else:
        affiliation_file = args[0]
        spreadsheet_name = args[1]

    try:
        output_number = int(options.output_number)
        STATS['limit'] = output_number
    except TypeError:
        parser.error('wrong output number')

    main(affiliation_file, spreadsheet_name, options.everything, output_number)
