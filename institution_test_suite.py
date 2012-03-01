from collections import defaultdict
import Levenshtein
import marshal
import os
import re
import time

import institution_searcher as s

RE_SPACES = re.compile('\s+')

def get_icns(reextract=False):
    if reextract:
        os.chdir('desy_affiliations')
        import desy_affs
        icns = desy_affs.get_icns()
        os.chdir('..')

        # Find the match file between old and new ICNs.
        match = dict(line.strip().split('\t') for line in open('old_new.txt').readlines())

        for icn in icns.keys():
            if '; 'in icn or icn.endswith(' to be removed'):
                # Delete pairs of affiliations.
                del icns[icn]
            else:
                if icn in match:
                    new_icn = RE_SPACES.sub(' ', match[icn].strip())
                else:
                    new_icn = icn

                try:
                    new_icn = new_icn.decode('utf-8')
                except:
                    pass

                new_icn = RE_SPACES.sub(' ', new_icn.strip())

                if new_icn != icn:
                    icns[new_icn] = sorted(list(set(icns.get(new_icn, []) + icns.pop(icn, []))))

        return icns
    else:
        return marshal.load(open('icns.marshal'))

PROCESS_NUMBER = 20

def extend_icns(icns):
    out = []
    for icn, institutions in icns.items():
        for institution in institutions:
            out.append((icn, institution))
    return out

def test_icns_only(icns):
    return test([(icn, icn) for icn in icns])

def analyse_icns(res, icns):
    out = []
    for original, _, matched in res:
        if matched is not None:
            if not isinstance(original, str):
                original = original.encode('utf-8')
            if not isinstance(matched, str):
                matched = matched.encode('utf-8')

            if original != matched:
                out.append((
                    Levenshtein.distance(original, matched),
                    1. / len(icns[original]),
                    original,
                    matched
                    ))
    return sorted(out)

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

def test_ratio(icns):
    res = test(icns)
    correct_matches, incorrect_matches = separate_results(res)

    correct_ratios = compute_ratios(correct_matches)
    incorrect_ratios = compute_ratios(incorrect_matches)

    return correct_ratios, incorrect_ratios

def compute_ratios(matches):
    out = []

    for inst, results in get_two_first_results(matches):
        if results and len(results) >= 2:
            out.append((inst, float(int(results[1][0] / results[0][0] * 20)) / 20))

    return out

def display_ratios(ratios):
    clustered = defaultdict(list)
    for inst, ratio in ratios:
        clustered[ratio].append(clustered)

    for i in [float(i) / 20 for i in range(0, 21)]:
        print len(clustered[i])

def get_two_first_results(institutions):
    results = []
    chunk_size = len(institutions) / PROCESS_NUMBER + 1

    first = []

    while institutions:
        chunk = institutions[:chunk_size]
        institutions = institutions[chunk_size:]
        results.append(s.get_match_ratio.delay(chunk))

    while not all([r.ready() for r in results]):
        time.sleep(0.1)

    out = []
    for r in results:
        out += r.result

    return out

def separate_results(res):
    correct, error = [], []
    for r in res:
        if r[0] == r[2]:
            correct.append(r[1])
        else:
            error.append(r[1])

    return correct, error

def get_sorted_errors(res):
    errors = [r for r in res if r[0] != r[2]]
    sorted_errors = defaultdict(list)
    for r in errors:
        sorted_errors[r[0]].append((r[1], r[2]))

    return sorted(((len(v), k) for k, v in sorted_errors.items()), reverse=True)

PREVIOUS_SCORE = 0

def print_statistics(results):
    correct = [r for r in results if r[0] == r[2]]
    score = float(len(correct)) / len(results) * 100
    print '%s/%s (%.2f%%)' % (len(correct), len(results), score)
    global PREVIOUS_SCORE
    print 'Previous score: %.2f%%' % PREVIOUS_SCORE
    PREVIOUS_SCORE = score

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
