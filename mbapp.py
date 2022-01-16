# interface for gsheet ui for demo app 'myapp'
# -----------------------------------------------------
# Import
# -----------------------------------------------------
import datetime as dt
import pandas as pd
import sys

from sqlgsheet import database as db
from dag import Nested

# -----------------------------------------------------
# Module variables
# -----------------------------------------------------

# constants
UI_SHEET = 'mbapp'
VALUE_LABEL = 'size_mb'

# dynamic
UI_CONFIG = {}
TABLES = {}

# -----------------------------------------------------
# Setup
# -----------------------------------------------------


def load():
    ''' loads basic configuration information for the module
    '''
    # loads the module interface to gsheet
    db.load()

    # load configuration information
    load_config()

    #load tables from gsheet
    load_gsheets()


def load_config():
    global UI_CONFIG
    config_tbl = db.get_sheet(UI_SHEET, 'config')
    UI_CONFIG = get_reporting_config(config_tbl)


def load_gsheets():
    global TABLES
    tables_config = db.GSHEET_CONFIG[UI_SHEET]['sheets'].copy()
    for t in tables_config:
        TABLES[t] = {}
        TABLES[t]['gsheet'] = db.get_sheet(UI_SHEET, t)


def get_reporting_config(tbl):
    DATE_FORMAT = '%Y-%m-%d'
    config = {}
    groups = list(tbl['group'].unique())
    for grp in groups:
        group_tbl = tbl[tbl['group'] == grp][['parameter', 'value', 'data_type']]
        params = dict(group_tbl[['parameter', 'value']]
                      .set_index('parameter')['value'])
        data_types = dict(group_tbl[['parameter', 'data_type']]
                          .set_index('parameter')['data_type'])
        for p in params:
            data_type = data_types[p]
            if ~(data_type == 'str'):
                if ~pd.isnull(params[p]):
                    if data_type in db.NUMERIC_TYPES:
                        numStr = params[p]
                        if numStr == '':
                            numStr = '0'
                        if data_type == 'int':
                            params[p] = int(numStr)
                        elif data_type == 'float':
                            params[p] = float(numStr)
                    elif data_type == 'date':
                        params[p] = dt.datetime.strptime(params[p], DATE_FORMAT).date()
        config[grp] = params
    return config


# -----------------------------------------------------
# Main
# -----------------------------------------------------


def update():
    # 01 load tables
    load()

    # 02 get form info
    profile_metadata = TABLES['profile_metadata']['gsheet'].copy()
    profile_data = TABLES['profile_data']['gsheet'].copy()
    profiles = TABLES['profile_records']['gsheet'].copy()

    #03 do something


def record_profile():
    """records a profile record from form input
    """
    load()

    #01 record form input
    metadata = TABLES['profile_metadata']['gsheet'].copy()
    profile_tbl = TABLES['profile_data']['gsheet'].copy()

    #02 create profile instance
    profile = Nested(profile_tbl, value_label=VALUE_LABEL)

    #03 store the record in the sqlite database
    #03.01 create record using metadata
    record = metadata.iloc[0]
    record.rename({'profile_datetime' : 'datetime'}, inplace=True)
    record[VALUE_LABEL] = profile.size
    record['profile'] = profile.json_str()
    record['profile_id'] = get_next_profile_id()
    new_record = pd.DataFrame.from_records([record])

    #03.02 append the new record to the database
    db.update_table(new_record, 'profile_records', True)

    # 04 posts records to the UI
    post_to_ui()


def load_profile():
    """load a profile record from the profile id in the ui form
    """
    load()

    profile_id = 0
    profile_id_tbl = TABLES['profile_id']['gsheet'].copy()
    profile_data = TABLES['profile_data']['gsheet'].copy()
    if len(profile_id_tbl) > 0:
        profile_id = profile_id_tbl['profile_id'].iloc[0]
    if profile_id > 0:
        profile_record = get_profile_record(profile_id)
        profile_record.rename({'datetime': 'profile_datetime'}, inplace=True)
        profile_tbl = pd.DataFrame.from_records([profile_record])
        metadata = profile_tbl[['asset', 'profile_datetime']].copy()
        nested_table = Nested(profile_record['profile'],
                              value_label=VALUE_LABEL).table()
        profile_data.update(nested_table)
        TABLES['profile_data']['gsheet'] = profile_data
        TABLES['profile_metadata']['gsheet'] = metadata

        post_to_ui()


def get_profile_record(profile_id):
    profile_record = []
    qry_stm = 'SELECT * FROM profile_records '
    qry_stm = qry_stm + 'WHERE profile_id = ' + str(profile_id) + ';'
    qry_result = pd.read_sql(qry_stm, con=db.engine)
    if len(qry_result) > 0:
        profile_record = qry_result.iloc[0].copy()
    return profile_record


def post_to_ui():
    metadata = TABLES['profile_metadata']['gsheet'].copy()
    profile_tbl = TABLES['profile_data']['gsheet'].copy()
    records = TABLES['profile_records']['gsheet'].copy()

    #01 post profile form
    db.post_to_gsheet(metadata,
                      UI_SHEET,
                      'profile_metadata',
                      input_option='USER_ENTERED')
    db.post_to_gsheet(profile_tbl,
                      UI_SHEET,
                      'profile_data',
                      input_option='USER_ENTERED')

    #02 post records
    # (future option to shorten to a summary of the most recent)
    gsheet_fields = records.columns
    records_tbl = db.get_table('profile_records')
    TABLES['profile_records']['sql'] = records_tbl
    gsheet_tbl = records_tbl[gsheet_fields].copy()
    TABLES['profile_records']['gsheet'] = gsheet_tbl
    db.post_to_gsheet(gsheet_tbl,
                      UI_SHEET,
                      'profile_records',
                      input_option='USER_ENTERED')


def get_next_profile_id():
    profile_id = 1
    if db.table_exists('profile_records'):
        records_tbl = db.get_table('profile_records')
        profile_id = records_tbl['profile_id'].max()+1
    return profile_id


if __name__ == "__main__":
    if len(sys.argv) > 1:
        procedure = sys.argv[1]
        if procedure == 'load_profile':
            load_profile()
        elif procedure == 'record_profile':
            record_profile()
        else:
            print('procedure: %s not found' % procedure)
    else:
        update()

# -----------------------------------------------------
# Reference code
# -----------------------------------------------------