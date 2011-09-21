import solr
import sys

from config import solr_url, http_user, http_pass

CONNECTION = solr.SolrConnection(solr_url, http_user=http_user, http_pass=http_pass)

def search_institution(institution):
    """
    Searches an institution and returns the best match i.e. the best result.
    """
    response = CONNECTION.query(institution, defType='lucene')
    if response.numFound > 0:
        match = response.results[0]
        return match['id']
    else:
        return None

if __name__ == '__main__':    
    institution_id = search_institution(sys.argv[-1])
    print institution_id
