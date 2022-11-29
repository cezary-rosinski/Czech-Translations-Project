import pandas as pd
from ast import literal_eval
from collections import Counter
from geopy import distance
from tqdm import tqdm
import warnings
from pandas.core.common import SettingWithCopyWarning
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
from my_functions import gsheet_to_df

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
#%%
translations_df = pd.read_excel('translations_after_manual_full_geonames_2022-11-23.xlsx')

places_df = translations_df.copy()[['001', 'geonames_id', 'geonames_name', 'geonames_country', 'geonames_lat', 'geonames_lng']]
places_df = places_df.loc[places_df['geonames_id'].notnull()]
for column in places_df.columns.values[1:]:
    places_df[column] = places_df[column].apply(lambda x: literal_eval(x))

places_df_with_001 = places_df.set_index('001').apply(pd.Series.explode).reset_index()

places_frequency = dict(Counter(places_df_with_001['geonames_id'].to_list()))
places_frequency = {k: v for k, v in sorted(places_frequency.items(), key=lambda item: item[1], reverse=True)}

places_df = places_df_with_001.drop(columns='001').drop_duplicates().reset_index(drop=True)

places_dict = {}
for i, row in places_df.iterrows():
    places_dict.update({row['geonames_id']: row.to_dict()})
    
places_locations_dict = {k:(float(places_dict.get(k).get('geonames_lat')), float(places_dict.get(k).get('geonames_lng'))) for k,v in places_frequency.items()}

used_places = set()
similar_places = {}
for k,v in tqdm(places_frequency.items()):
    if k not in used_places:
        # k = 3067696
        k_location = places_locations_dict.get(k)
        close_places = {ka:distance.distance(k_location,va).km for ka,va in places_locations_dict.items() if ka != k and ka not in used_places}
        close_places = {ka for ka, va in close_places.items() if va <= 20}
        used_places.update(close_places)
        similar_places.update({k:close_places})

similar_places_filtered = {k:v for k,v in similar_places.items() if v}    

df = pd.DataFrame()
for k,v in tqdm(similar_places_filtered.items()):
    # k = 293397
    # v = similar_places_filtered.get(k)
    v.add(k)
    test_df = places_df.loc[places_df['geonames_id'].isin(v)]
    test_df['geonames_group'] = k
    df = pd.concat([df, test_df])
    
df = df.reset_index(drop=True)
df['no. of records'] = df['geonames_id'].apply(lambda x: places_frequency.get(x))

df.to_excel('places_with_neighbourhood.xlsx', index=False)

#%% 20 km range with distance

range_20_km = gsheet_to_df('1fuFy_ZemnkNTP6dAjc6Q3U3HJZL07vsduxDOER8A0ss', '20km')

groupby = range_20_km.groupby('geonames_group')

distance_to_center_dict = {}
for name, group in tqdm(groupby, total=len(groupby)):
    # name = '3067696'
    # group = groupby.get_group(name)
    center_distance = group.loc[group['geonames_id'] == name][['geonames_lat', 'geonames_lng']].values.tolist()[0]
    distances_dict = group.copy()[['geonames_id', 'geonames_lat', 'geonames_lng']].values.tolist()
    distances_dict = dict(zip([e[0] for e in distances_dict], [e[1:] for e in distances_dict]))
    distances = {k:round(distance.distance(center_distance,v).km, 2) for k,v in distances_dict.items()}
    distance_to_center_dict.update(distances)

df = pd.DataFrame.from_dict(distance_to_center_dict, orient='index').reset_index()






















