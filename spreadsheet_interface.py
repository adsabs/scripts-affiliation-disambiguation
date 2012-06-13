"""
Module to interact with Google Docs Spreadsheet.
"""

import ConfigParser
import time
import gdata.spreadsheet.text_db

CLIENT = None

cfg = ConfigParser.ConfigParser()
cfg.read('accounts.cfg')

def connect():
    global CLIENT
    CLIENT = gdata.spreadsheet.text_db.DatabaseClient(cfg.get('spreadsheet', 'user'),
            cfg.get('spreadsheet', 'password'))
    return CLIENT

def upload_data(data, title='None'):
    db = CLIENT.GetDatabases(name="Astronomy affiliations disambiguation")[0]
    table = db.CreateTable('%s (%s)' % (title, time.strftime('%b %d, %Y')), data[0].keys())
    for record in data:
        try:
            table.AddRecord(record)
        except:
            # In case of problem, retry before failing.
            time.sleep(1)
            table.AddRecord(record)
