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

def upload_data(data, spreadsheet_name, title='None'):
    db = CLIENT.GetDatabases(name=spreadsheet_name)[0]
    table = db.CreateTable('%s (%s)' % (title, time.strftime('%b %d, %Y')), data[0].keys())
    for record in data:
        format_record(record)
        try:
            table.AddRecord(record)
        except:
            # In case of problem, retry before failing.
            time.sleep(1)
            table.AddRecord(record)

def upload_statistics(statistics, spreadsheet_name):
    """
    Adds a stat entry to the statistics sheet of the spreadsheet.
    """
    format_record(statistics)

    db = CLIENT.GetDatabases(name=spreadsheet_name)[0]
    table = db.GetTables(name="Statistics")[0]
    table.AddRecord(statistics)

def format_record(record):
    """
    Makes sure that everything is an encoded UTF-8 string.
    """
    for k, v in record.items():
        if isinstance(v, unicode):
            record[k] = v.encode('utf_8')
        elif not isinstance(v, str):
            record[k] = str(v)
