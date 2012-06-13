import re
import sys

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
