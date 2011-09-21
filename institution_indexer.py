import sys
import invenio.bibrecord as bibrecord
import solr

SOLR_URL = 'http://labs.adsabs.harvard.edu/affiliations/solr'
HTTP_USER = 'benoit'
HTTP_PASS = 'thisisthesolrpassword'
CONNECTION = solr.SolrConnection(SOLR_URL, http_user=HTTP_USER, http_pass=HTTP_PASS)

INDEX_FIELDS = {
        'institution': ['110__a', '110__t', '110__u', '110__x', '410__a', '410__g'],
        'department': ['110__b'],
        'address': ['371__a'],
        'city': ['371__b'],
        'state': ['371__c'],
        'country': ['371__d'],
        'zip_code': ['371__e'],
        'country_code': ['371__g'],
        }

def get_institution_records(path):
    """
    Returns all institution records in a BibRecord structure.
    """
    return [res[0] for res in bibrecord.create_records(open(path).read())]

def index_records(records):
    """
    Indexes all the institution records and then commits.
    """
    print 'Going to index %d records.' % len(records)

    for record in records:
        try:
            data = get_indexable_data(record)
            CONNECTION.add(
                    id=data.get('id'),
                    institution=data.get('institution'),
                    department=data.get('department'),
                    address=data.get('address'),
                    city=data.get('city'),
                    state=data.get('state'),
                    country=data.get('country'),
                    zip_code=data.get('zip_code'),
                    country_code=data.get('country_code'),
                    )
            print 'Record %s done.' % data['id']
        except Exception:
            print 'Problem with record %s.' % data['id']
            raise

    CONNECTION.commit()

def get_indexable_data(record):
    """
    Returns indexable data for a Bibrecord institution record in Solr.
    """
    # Mapped from https://twiki.cern.ch/twiki/bin/view/Inspire/DevelopmentRecordMarkupInstitutions#Field_Mapping_final
    data = {}

    data['id'] = bibrecord.record_get_field_value(record, '001')

    for index, tags in INDEX_FIELDS.items():
        values = []
        for tag in tags:
            for value in bibrecord.record_get_field_values(record, tag[:3],
                    tag[3], tag[4], tag[5]):
                values.append(value.decode('utf-8'))
        if values:
            data[index] = list(set(values))

    return data

if __name__ == '__main__':
    records = get_institution_records(sys.argv[-1])
    index_records(records)
