#!/usr/bin/python

import os
import re
import solr
import sys
import time
import urllib2

import invenio.bibrecord as bibrecord

from local_config import SOLR_URL, HTTP_USER, HTTP_PASS

CONNECTION = solr.SolrConnection(SOLR_URL, http_user=HTTP_USER, http_pass=HTTP_PASS)

INDEX_FIELDS = {
        'institution': ['110__a', '110__t', '110__u', '110__x'],
        'institution_acronym': ['110__a', '110__t', '110__u', '110__x'],
        'department': ['110__b'],
        'address': ['371__a'],
        'city': ['371__b'],
        'state': ['371__c'],
        'country': ['371__d'],
        'zip_code': ['371__e'],
        'country_code': ['371__g'],
        }

def delete_solr_documents():
    CONNECTION.delete_query('*:*')
    CONNECTION.commit()

def get_institution_marcxml():
    """
    Downloads the Inspire institution database.
    """
    marcxml = download_institution_chunk(0)
    out = open('etc/institutions_000.xm', 'w')
    out.write(marcxml)
    out.close()

    match = re.search('<!-- Search-Engine-Total-Number-Of-Results: (\d+) -->', marcxml)
    number_of_results = int(match.group(1))
    number_of_chunks = number_of_results / 200 + 1

    for i in range(1, number_of_chunks):
        marcxml = download_institution_chunk(i)
        out = open('etc/institutions_%03d.xm' % i, 'w')
        out.write(marcxml)
        out.close()

def download_institution_chunk(chunk_number, chunk_size=200):
    jrec = chunk_size * chunk_number + 1
    request = urllib2.Request('http://inspirebeta.net/search?cc=Institutions&jrec=%d&rg=%d&of=xm' % (jrec, chunk_size), headers={'User-Agent': 'Benoit Thiell, SAO/NASA ADS'})
    response = urllib2.urlopen(request)
    marcxml = response.read()
    response.close()
    return marcxml

def get_institution_records(path):
    """
    Returns all institution records in a BibRecord structure.
    """
    return [res[0] for res in bibrecord.create_records(open(path).read())]

def index_records(records):
    """
    Indexes all the institution records and then commits.
    """
    to_add = []
    for record in records:
        if not record_is_deleted(record):
            data = get_indexable_data(record)
            if not data['display_name'].startswith('Unlisted') and \
                not data['display_name'].startswith('obsolete'):
                to_add.append(data)

    CONNECTION.add_many(to_add)

def get_indexable_data(record):
    """
    Returns indexable data for a Bibrecord institution record in Solr.
    """
    # Mapped from https://twiki.cern.ch/twiki/bin/view/Inspire/DevelopmentRecordMarkupInstitutions#Field_Mapping_final
    data = {}

    data['id'] = bibrecord.record_get_field_value(record, '001')
    display_name = bibrecord.record_get_field_value(record, '110', '', '', 't') or \
            bibrecord.record_get_field_value(record, '110', '', '', 'u') or \
            bibrecord.record_get_field_value(record, '110', '', '', 'a')
    data['display_name'] = display_name.decode('utf-8')
    desy_icn = bibrecord.record_get_field_value(record, '110', '', '', 'u')
    data['desy_icn'] = desy_icn.decode('utf-8')

    for index, tags in INDEX_FIELDS.items():
        values = []
        for tag in tags:
            for value in bibrecord.record_get_field_values(record, tag[:3],
                    tag[3], tag[4], tag[5]):
                values.append(value.decode('utf-8'))
        if values:
            data[index] = list(set(values))

    old = bibrecord.record_get_field_value(record, '110', '', '', 'u')
    new = bibrecord.record_get_field_value(record, '110', '', '', 't')

    if old and new and old != new:
        open('old_new.txt', 'a').write('%s\t%s\n' % (old, new))

    return data

def record_is_deleted(record):
    """
    Checks if a record is deleted.
    """
    return bibrecord.record_get_field_value(record, '980', '', '', 'c') == 'DELETED'

if __name__ == '__main__':
    if sys.argv[-1] == '--download':
        print time.asctime() + ': Delete all previous institution files.'
        for path in os.listdir('etc'):
            os.remove('etc/' + path)
        print time.asctime() + ': Download the Inspire institution database.'
        get_institution_marcxml()
    print time.asctime() + ': Delete all documents in Solr.'
    delete_solr_documents()
    print time.asctime() + ': Indexing in Solr.'
    for path in sorted(os.listdir('etc')):
        print time.asctime() + ': File %s.' % path
        records = get_institution_records('etc/' + path)
        index_records(records)
    CONNECTION.commit()
