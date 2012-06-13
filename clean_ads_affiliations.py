import codecs
import os
import re
import sys

assert sys.hexversion >= 0x02060000

from csv_utils import escape_csv
from collections import defaultdict

try:
    import ads.Unicode
except ImportError:
    sys.path.append('/proj/ads/soft/python/lib/site-packages')
    import ads.Unicode

UNICODE_HANDLER = ads.Unicode.UnicodeHandler()
SPACES_REGEX = re.compile('\s+')

def _preclean_affiliation(aff):
    """
    Performs basic cleaning operations on an affiliation string.
    """
    aff = aff.decode('utf8')
    aff = SPACES_REGEX.sub(' ', aff).strip()
    aff = UNICODE_HANDLER.ent2u(aff)
    return aff

def clean_ads_affs(path, verbose=0):
    """
    Reads an ADS affiliation file in the form:
    bibcode\taffiliation

    Returns a file in the form:
    affiliation\tbibcode1 bibcode2
    """
    msg('-- Create the list of bibcodes.', verbose)

    affiliations = defaultdict(list)

    for line in open(path):
        line = line.strip()
        # Sandwich.
        try:
            line = line.decode('utf8')
        except UnicodeDecodeError:
            print 'UNICODE ERROR:', line
            continue
        bibcode, position, affiliation = line.strip().split('\t', 2)
        try:
            affiliation = _preclean_affiliation(escape_csv(affiliation))
        except ads.Unicode.UnicodeHandlerError:
            print 'ENTITY ERROR:', line
            continue
        affiliations[affiliation].append('%s,%s' % (bibcode, position))

    msg('-- Transform back to list', verbose)
    affiliations = sorted(affiliations.items())
    affiliations = ['\t'.join([aff, ' '.join(bibcodes)]) for aff, bibcodes in affiliations]

    if path.endswith('.merged'):
        new_path = os.path.join('/tmp', os.path.basename(path)[:-7] + '.reversed')
    else:
        new_path = os.path.join('/tmp', os.path.basename(path) + '.reversed')

    msg('-- Writing to file %s.' % new_path, verbose)
    open(new_path, 'w').write('\n'.join(affiliations).encode('utf8'))

    msg('-- Done writing to file.', verbose)

    return new_path

def msg(message, verbose):
    if verbose:
        print message
