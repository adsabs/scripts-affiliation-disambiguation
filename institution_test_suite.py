from collections import defaultdict
import os
import time

import institution_searcher as s


def get_icns():
#   os.chdir('desy_affiliations')
#   import desy_affs
#   icns = desy_affs.get_icns()
#   os.chdir('..')

#   d_icns = defaultdict(list)
#   for icn, inst in icns:
#       d_icns[icn].append(inst)

#   return d_icns

    import marshal
    return marshal.load(open('icns.marshal'))

PROCESS_NUMBER = 20

def extend_icns(icns):
    out = []
    for icn, institutions in icns.items():
        for institution in institutions:
            out.append((icn, institution))
    return out

def test(icns):
    if isinstance(icns, dict):
        icns = extend_icns(icns)

    results = []
    chunk_size = len(icns) / PROCESS_NUMBER + 1

    while icns:
        chunk = icns[:chunk_size]
        icns = icns[chunk_size:]
        results.append(s.match_institutions.delay(chunk))

    while not all([r.ready() for r in results]):
        time.sleep(0.1)

    out = []
    for r in results:
        out += r.result

    print_statistics(out)

    return out

def print_statistics(results):
    correct = [r for r in results if r[0] == r[2]]
    print '%s/%s (%.2f%%)' % (len(correct), len(results), (float(len(correct)) / len(results) * 100))

def format_results(results):

    from BeautifulSoup import UnicodeDammit

    new_results = []
    for line in results:
        new_line = []
        for elem in line:
            if elem is None:
                new_line.append('')
            else:
                new_line.append(UnicodeDammit(elem).unicode)
        new_results.append('\t'.join(new_line))

    return '\n'.join(new_results)
