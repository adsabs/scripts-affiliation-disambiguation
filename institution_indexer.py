#!/usr/bin/python

import ConfigParser
import os
import re
import solr
import sys
import time
import urllib2

try:
    import invenio.bibrecord as bibrecord
except ImportError:
    # Invenio is not installed, use fallback standalone bibrecord.
    import bibrecord

cfg = ConfigParser.ConfigParser()
cfg.read('accounts.cfg')

CONNECTION = solr.SolrConnection(cfg.get('solr', 'url'),
        http_user=cfg.get('solr', 'user'),
        http_pass=cfg.get('solr', 'password'))

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

if not os.path.exists('etc'):
    os.mkdir('etc')

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

def get_name_variants(record):
    """
    Return indexable values in the 410 field.
    """
    name_variants = set()
    if '410' in record:
        fields = bibrecord.record_get_field_instances(record, '410')
        for field in fields:
            values = bibrecord.field_get_subfield_values(field, 'a')
            if values:
                if 'ADS' in bibrecord.field_get_subfield_values(field, '9'):
                    # Always index field with source ADS.
                    for value in values:
                        name_variants.add(value.decode('utf_8'))
                else:
                    # Disregard uppercase space-separated fields.
                    for value in values:
                        if not re.match('\s*[A-Z]+\s[A-Z ]+$', value):
                            name_variants.add(value.decode('utf_8'))

    return list(name_variants)

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
    data['display_name'] = display_name.decode('utf_8')
    desy_icn = bibrecord.record_get_field_value(record, '110', '', '', 'u')
    data['desy_icn'] = desy_icn.decode('utf_8')

    for index, tags in INDEX_FIELDS.items():
        values = []
        for tag in tags:
            for value in bibrecord.record_get_field_values(record, tag[:3],
                    tag[3], tag[4], tag[5]):
                values.append(value.decode('utf_8'))
        if values:
            data[index] = list(set(values))

    # Name variants
    name_variants = get_name_variants(record)
    if name_variants:
        data['name_variants'] = name_variants

    old = bibrecord.record_get_field_value(record, '110', '', '', 'u')
    new = bibrecord.record_get_field_value(record, '110', '', '', 't')

    if old and new and old != new:
        open('etc/old_new.txt', 'a').write('%s\t%s\n' % (old, new))

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
