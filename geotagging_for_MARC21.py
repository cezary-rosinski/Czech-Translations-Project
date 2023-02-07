import pandas as pd
import numpy as np
import geopandas as gpd
from ast import literal_eval
from tqdm import tqdm
from shapely.geometry import shape, Point
import regex as re
import requests
from concurrent.futures import ThreadPoolExecutor


#marc df
#parsed publishing places
#geonames query
# new columns in df

#%% def

def marc_parser_for_field(string, subfield_code):
    subfield_list = re.findall(f'{subfield_code}.', string)
    for subfield in subfield_list:
        subfield_escape = re.escape(subfield)
        string = re.sub(f'({subfield_escape})', r'❦\1', string)
    string = [e.split('\n')[0].strip() for e in string.split('❦') if e]
    dictionary_fields = [e for e in string if re.escape(e)[:len(subfield_code)] == subfield_code]
    dictionary_fields = [{subfield_list[i]:e[len(subfield_list[i]):]} for i, e in enumerate(dictionary_fields)]
    return dictionary_fields

#%% load MARC21 dataframe

marc21_df = pd.read_excel('MARC21_test_dataframe.xlsx')
type(marc21_df.at[0,'260'])

#%% parse column with geographical names

marc_field_publishing_places = '260'

places_from_records = list(zip([literal_eval(e)[0] for e in marc21_df['001'].to_list()], [[el['$a'].replace('[','').replace(']','').replace(' :','').strip() for el in marc_parser_for_field(literal_eval(e)[0], '\\$') if '$a' in el] for e in marc21_df[marc_field_publishing_places].to_list()]))

places_names = set([el for sub in [e[-1] for e in places_from_records] for el in sub])

def harvest_geonames(place_name, geonames_username):  
    url = 'http://api.geonames.org/searchJSON?'
    params = {'username': geonames_username, 'q': place_name, 'featureClass': 'P', 'style': 'FULL'}
    result = requests.get(url, params=params).json()
    result = max([e for e in result.get('geonames')], key=lambda x: x.get('score'))
    temp_dict = {k:v for k,v in result.items() if k in ['geonameId', 'name', 'countryName', 'lat', 'lng']}
    temp_dict.update({'place name': place_name})
    return temp_dict

with ThreadPoolExecutor() as executor:
    geonames_response = list(tqdm(executor.map(lambda p: harvest_geonames(p, 'crosinski'), places_names)))

test = [(i, [ele for sub in [[el for el in geonames_response if el['place name'] == e] for e in name] for ele in sub]) for i, name in places_from_records]

df = pd.DataFrame(test)

#spiąć identyfikatory biblio z odpowiedzią geonames


