# interface for gsheet ui for demo app 'myapp'
# -----------------------------------------------------
# Import
# -----------------------------------------------------
import datetime as dt
import numpy as np
import pandas as pd
import json
import re


def to_json(df, value_field='value'):
    """converts expanded table to json
    :param df: expanded table
    :type df: pd.DataFrame
    :return: json
    :rtype: str
    """
    json_str = ''
    if len(df) > 0:
        N = len(df)
        values = df[value_field].values
        rows = df[[f for f in df.columns if not f == value_field]].values
        for i in range(N):
            row = rows[i]
            va = values[i]
            lvla = np.where(row != '')[0][0]
            a = row[lvla]
            if i < (N-1):
                lvlb = np.where(rows[i+1] != '')[0][0]
                txn_pair = [(a, lvla, va), lvlb]
            else:
                txn_pair = [(a, lvla, va)]
            txn_str = transition_str(txn_pair, value_field)
            json_str = json_str + txn_str
    return '{' + json_str + '}'


def transition_str(txn_pair, value_str='value'):
    a, lvla, value = txn_pair[0]
    a = '"' + a + '"'
    value = '"'+ value_str + '": "' + str(value) + '"'
    tail_str = ''
    if len(txn_pair) > 1:
        #not EOL
        lvlb = txn_pair[1]
        if lvlb <= lvla:
            tail_str = ','
            if lvlb < lvla:
                head_str = a + ': {' + value + '}' + '}'*(lvla-lvlb+1)
            else:
                head_str = a + ': {' + value + '}'
        else:
            head_str = a + ': {' + value + ', "subfolders": {'
    else:
        head_str = a + ': {' + value + '}'*(lvla + 3)
    return head_str + tail_str


def to_table(json_str, value_field='value'):
    """ renders a json to a table with
    rows = labels and columns = depth +
    additional column for value field.

    row = (label, level, value)
    level = count('{') - count('}')

    :param json_str:
    :type json_str: str
    :param value_field: label for value field
    :type value_field: str
    :return: nested_table:
    :rtype: pd.DataFrame
    """
    nested_table = [] #initialize return variable

    #01 get depth from net sum of { and } markers
    start_p = [x.start() for x in re.finditer('{', json_str)]
    end_p = [x.start() for x in re.finditer('}', json_str)]
    if len(start_p) > 0 and len(end_p) > 0:
        df = pd.DataFrame({'start_p': pd.Series([1 for x in start_p], index=start_p),
                           'end_p': pd.Series([-1 for x in end_p], index=end_p)})
        df.fillna(0, inplace=True)
        df['depth'] = df['start_p'].cumsum() + df['end_p'].cumsum()
        del df['start_p'], df['end_p']
        df['depth'] = df['depth'].astype(int)

        #02 get sublevel from depth
        df['level'] = df['depth'].apply(lambda x: int((x + 1) / 2))
        df['sublevel'] = df.apply(lambda x: int(2 * x['level'] - x['depth']), axis=1)

        #03 get substring within {} from positional marker (index)
        df.reset_index(inplace=True)
        df['next'] = df['index'].shift(-1)
        df['next'].fillna(0, inplace=True)
        df['next'] = df['next'].astype(int)
        df['substring'] = df.apply(lambda x: json_str[int(x['index'] + 1):int(x['next'])], axis=1)

        #04 extract the label from substring using sublevel==1 and dropping unnecessary characters
        df['label'] = df.apply(lambda x: ''.join(
            c for c in x['substring'] if not
            c in [',', '"', ':', ' ']) if x['sublevel'] == 1 else '', axis=1)

        #05 extract the value from substring using sublevel==0, positional markers
        # and convert from str to float
        df['value_str'] = df.apply(
            lambda x: x['substring'][x['substring'].find(':') + 3:x['substring'].find(
                '"', x['substring'].find(':') + 3)] if x['sublevel'] == 0 else '', axis=1)
        df[value_field] = df['value_str'].apply(lambda x: float(x) if len(x) > 0 else np.nan)

        #06 group by label,
        # and add new fields value, label based on sublevel==0 (value) and ==1 (label)
        value_tbl = df[df['sublevel'] == 0][value_field].reset_index()
        label_tbl = df[df['sublevel'] == 1][['level', 'label']].reset_index()
        del label_tbl['index'], value_tbl['index']
        df2 = label_tbl.join(value_tbl)
        df2 = df2[df2['label'] != ''].copy()
        df2.reset_index(inplace=True)
        del df2['index']

        #07 expand the level fields level_1, level_2, ... to visually order the labels
        level_max = df2['level'].max()
        levels = range(level_max)
        level_labels = ['level_'+str(l+1) for l in levels]
        for l in levels:
            df2[level_labels[l]] = df2.apply(
                lambda x: x['label'] if x['level'] == l+1 else '', axis=1)

        #08 drop intermediate fields which are not used in final table format
        table_fields = level_labels + [value_field]
        nested_table = df2[table_fields].copy()
    return nested_table


class Nested(object):
    _json = {}
    _json_str = ''
    _table = []
    size = None
    value_label = ''
    def __repr__(self):
        return self._json.__repr__()

    def __init__(self, nested_list, value_label='value'):
        self.value_label = value_label
        if type(nested_list) == str:
            self.from_json_str(nested_list)
        elif type(nested_list) == dict:
            self.from_json(nested_list)
        elif type(nested_list) == pd.DataFrame:
            self.from_table(nested_list)
        else:
            raise ValueError('invalid type')
        self.set_size()

    def from_json_str(self, json_str):
        """ class constructor from json as string '{}'

        :param json_str:
        :type json_str: str
        """
        self._json_str = json_str
        self._json = json.loads(self._json_str)
        self._table = to_table(
            self._json_str,
            value_field=self.value_label)

    def from_json(self, json_dict):
        """ class constructor from json as dictionary {}

        :param json_dict:
        :type json_dict: dict
        """
        self._json = json_dict
        self._json_str = json.dumps(json_dict)
        self._table = to_table(
            self._json_str,
            value_field=self.value_label)

    def from_table(self, nested_table):
        """ class constructor from nested table

        :param nested_table:
        :type nested_table: pd.DataFrame
        """
        self._table = nested_table
        self._json_str = to_json(
            nested_table,
            value_field=self.value_label)
        self._json = json.loads(self._json_str)

    def json_str(self):
        """
        :rtype: str
        """
        return self._json_str

    def json_dict(self):
        """
        :rtype: dict
        """
        return self._json

    def table(self):
        """
        :rtype: pd.DataFrame
        """
        return self._table

    def set_size(self):
        if len(self._table)>0:
            self.size = self._table[
                self._table['level_1'] != ''][self.value_label].sum()