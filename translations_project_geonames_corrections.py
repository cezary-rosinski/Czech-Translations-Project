#%% import
import pickle
import regex as re
import requests
import pandas as pd
from collections import Counter, defaultdict, ChainMap
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
from my_functions import marc_parser_dict_for_field, create_google_worksheet, gsheet_to_df, cluster_strings
import numpy as np
from unidecode import unidecode
sys.path.insert(1, 'C:/Users/Cezary/Documents/miasto-wies')
from geonames_accounts import geonames_users
import random
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from ast import literal_eval
from itertools import groupby
import gspread as gs
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from xml.etree import ElementTree

#%% def

def simplify_place_name(x):
    return ''.join([unidecode(e).lower() for e in re.findall('[\p{L}- ]',  x)]).strip()

def all_equal(iterable):
    g = groupby(iterable)
    return next(g, True) and not next(g, False)

#%% przygotowanie danych gonames

# with open('translations_places.pickle', 'rb') as f:
#     places_geonames = pickle.load(f)
# with open('translations_places_2.pickle', 'rb') as f:
#     places_geonames_2 = pickle.load(f)

# places_geonames_extra = {}
# places_geonames_extra.update({'v praze':places_geonames['praha']})
# places_geonames_extra.update({'reinbek bei hamburg':places_geonames['reinbek']})
# places_geonames_extra.update({'v ljubljani':places_geonames['ljubljana']})
# places_geonames_extra.update({'v brne':places_geonames['brno']})
# places_geonames_extra.update({'sofiia':places_geonames['sofia']})
# places_geonames_extra.update({'v bratislave':places_geonames['bratislava']})
# places_geonames_extra.update({'v gorici':places_geonames['gorizia']})
# places_geonames_extra.update({'u zagrebu':places_geonames['zagreb']})
# places_geonames_extra.update({'w budysinje':places_geonames['budisin']})
# places_geonames_extra.update({'turciansky sv martin':places_geonames['martin']})
# places_geonames_extra.update({'s-peterburg':places_geonames['st petersburg']})
# places_geonames_extra.update({'berlin ua':places_geonames['berlin']})
# places_geonames_extra.update({'nakladatelstvi ceskoslovenske akademie ved':places_geonames['praha']})
# places_geonames_extra.update({'nayi dilli':places_geonames['new delhi']})
# places_geonames_extra.update({'paul hamlyn':places_geonames['london']})
# places_geonames_extra.update({'unwin':places_geonames['london']})
# places_geonames_extra.update({'soul tukpyolsi':places_geonames['seoul']})
# places_geonames_extra.update({'v ostrave':places_geonames['ostrava']})
# places_geonames_extra.update({'ottensheim an der donau':places_geonames['ottensheim']})
# places_geonames_extra.update({'mor ostrava':places_geonames['ostrava']})
# places_geonames_extra.update({'troppau':places_geonames['opava']})
# places_geonames_extra.update({'g allen':places_geonames['london']})
# places_geonames_extra.update({'frankfurt a m':places_geonames['frankfurt am main']})
# places_geonames_extra.update({'v kosiciach':places_geonames['kosice']})
# places_geonames_extra.update({'olmutz':places_geonames['olomouc']})
# places_geonames_extra.update({'helsingissa':places_geonames['helsinki']})
# places_geonames_extra.update({'mahr-ostrau':places_geonames['ostrava']})
# places_geonames_extra.update({'v ziline':places_geonames['zilina']})
# places_geonames_extra.update({'v plzni':places_geonames['plzen']})
# places_geonames_extra.update({'artia':places_geonames['praha']})
# places_geonames_extra.update({'praha in-':places_geonames['praha']})
# places_geonames_extra.update({'klagenfurt am worthersee':places_geonames['klagenfurt']})
# places_geonames_extra.update({'prjasiv':places_geonames['presov']})
# places_geonames_extra.update({'esplugas de llobregat':places_geonames['esplugues de llobregat']})
# places_geonames_extra.update({'v celovcu':places_geonames['klagenfurt']})
# places_geonames_extra.update({'london printed in czechoslovakia':places_geonames['london']})
# places_geonames_extra.update({'warzsawa':places_geonames['warszawa']})
# places_geonames_extra.update({'tai bei xian xin dian shi':places_geonames['taiwan']})
# places_geonames_extra.update({'ciudad de mexico':places_geonames['mexico']})
# places_geonames_extra.update({'poszony':places_geonames['bratislava']})
# places_geonames_extra.update({'budysyn':places_geonames['bautzen']})
# places_geonames_extra.update({'spolek ceskych bibliofilu':places_geonames['praha']})
# places_geonames_extra.update({'v londyne':places_geonames['london']})
# places_geonames_extra.update({'korea':places_geonames['seoul']})
# places_geonames_extra.update({'madarsko':places_geonames['budapest']})
# places_geonames_extra.update({'na smichove':places_geonames['praha']})
# places_geonames_extra.update({'wien ua':places_geonames['wien']})
# places_geonames_extra.update({'hki':places_geonames['helsinki']})
# places_geonames_extra.update({'prag ii':places_geonames['praha']})
# places_geonames_extra.update({'sv praha':places_geonames['praha']})
# places_geonames_extra.update({'kassel-wilhelmshoehe':places_geonames['kassel']})
# places_geonames_extra.update({'matica slovenska':places_geonames['martin']})
# places_geonames_extra.update({'basil blackwell':places_geonames['oxford']})
# places_geonames_extra.update({'amsterodam':places_geonames['amsterdam']})
# places_geonames_extra.update({'boosey':places_geonames['london']})
# places_geonames_extra.update({'bratislave':places_geonames['bratislava']})
# places_geonames_extra.update({'evans bros':places_geonames['london']})
# places_geonames_extra.update({'fore publications':places_geonames['london']})
# places_geonames_extra.update({'g allen':places_geonames['london']})
# places_geonames_extra.update({'george sheppard':places_geonames['oxford']})
# places_geonames_extra.update({'hamburg wegner':places_geonames['hamburg']})
# places_geonames_extra.update({'heinemann':places_geonames['london']})
# places_geonames_extra.update({'hogarth press':places_geonames['london']})
# places_geonames_extra.update({'hutchinson':places_geonames['london']})
# places_geonames_extra.update({'i nicholson':places_geonames['london']})
# places_geonames_extra.update({'john lane':places_geonames['london']})
# places_geonames_extra.update({'jonathan cape':places_geonames['london']})
# places_geonames_extra.update({'kattowitz':places_geonames['katowice']})
# places_geonames_extra.update({'methuen':places_geonames['london']})
# places_geonames_extra.update({'moderschan':places_geonames['praha']})
# places_geonames_extra.update({'new english library':places_geonames['london']})
# places_geonames_extra.update({'orbis pub co':places_geonames['praha']})
# places_geonames_extra.update({'pp ix':places_geonames['london']})
# places_geonames_extra.update({'praha prag':places_geonames['praha']})
# places_geonames_extra.update({'robert anscombe':places_geonames['london']})
# places_geonames_extra.update({'spck':places_geonames['london']})
# places_geonames_extra.update({'spring books':places_geonames['london']})
# places_geonames_extra.update({'supraphon':places_geonames['praha']})
# places_geonames_extra.update({'u sisku':places_geonames['praha']})
# places_geonames_extra.update({'unwin':places_geonames['london']})
# places_geonames_extra.update({'v cheshskoi pragie':places_geonames['praha']})
# places_geonames_extra.update({'v prahe':places_geonames['praha']})
# places_geonames_extra.update({'w prazy':places_geonames['praha']})
# places_geonames_extra.update({'watson':places_geonames['london']})
# places_geonames_extra.update({'william heinemann':places_geonames['london']})
# places_geonames_extra.update({'xv paris':places_geonames['paris']})           

# places_geonames_extra.update({'aarau': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661881&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'basel': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2661604&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'kopenhagen': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2618425&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'hradec kralove': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3074967&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'presov': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=723819&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'nadlac': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=672546&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'monchaltorf': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2659631&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'kyonggi-do paju-si': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=1840898&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'hong kong': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=1819729&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'prishtine': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=786714&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'chester springs': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=5184082&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'gardena': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=5351549&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'kisineu': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=618426&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'w chosebuzu': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2939811&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'bombay': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=1275339&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'calcutta': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=1275004&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'tuzla': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3188582&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'prjasiv': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=723819&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'hradec kralove': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3074967&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'koniggratz': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3074967&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'weitra': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2761538&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'cairo': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=360630&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'al-qahirat': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=360630&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'hildesheim': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2904789&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'taipei': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=1668341&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'tubingen': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2820860&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'sibiu': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=667268&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'kbh': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2618425&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'aarau': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=7285013&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'altenmedingen': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=6552815&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'am heiligen berge bei olmutz': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3069011&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'avon': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=6451134&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'bad aibling': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=6558227&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'bad goisern': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=7872008&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'bad homburg': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=6559114&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'bakingampeta vijayavada': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=1253184&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'basingstoke': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2656192&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'bassac': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3034714&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'bjelovar': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3203982&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'boucherville': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=5906267&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'brandys nad labem': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3078837&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'brazilio': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3469058&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'chapeco': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3466296&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'cormons': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3178085&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'crows nest': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2207821&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'csorna': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3053918&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'daun': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2938714&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'dinslaken': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2936871&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'doran': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=5118226&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'east rutherford': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=5097459&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'englewood cliffs': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=5097677&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'galati': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=677697&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'grenoble': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3014728&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'gweru': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=890422&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'haida': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3069381&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'harmonds-worth': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=6951076&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'hoboken': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=5099133&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'hof': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2902768&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'hof a d saal': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2902768&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'horn': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2775516&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'idstein': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2896736&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'kbh': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2618425&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'kirchseeon': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2890381&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'kissingen': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2953424&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'kremsier': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3072649&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'la tour-daigues': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3006213&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'la vergne': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=4635031&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'leinfelden bei stuttgart': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2879185&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'leitmeritz': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3071677&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'ljouvert': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2751792&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'ludewig': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2890381&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'melbourne': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2158177&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'melhus': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3146125&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'mem martins': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2266464&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'mensk': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=625144&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'mestecko': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3058730&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'mouton': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2747373&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'na prevaljah': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3192484&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'neuotting am inn': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2864387&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'neustadt in holstein': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2864034&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'newburyport': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=4945256&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'nimes': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2990363&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'nokis': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=601294&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'novomesto': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3194351&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'oradea': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=671768&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'oud-gastel': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2748956&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'oude-god': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2789549&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'paiania': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=256197&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'penguin books': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=6951076&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'petrovec': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3058505&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'pozega': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3190589&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'prace': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3067716&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'purley': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2639842&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'reinbek bei hamburg': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2848845&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'reinbek hbg': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2848845&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'remschied': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2848273&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'riga': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=456172&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'rotmanka': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=7576449&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'salzburg': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2766824&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'sibiu': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=667268&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'stanislawow': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=758416&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'starnberg am see': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2950767&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'szentendre': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3044681&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'the haugue': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2747373&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'tonder': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2611497&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'treben': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3064117&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'usti nad labem': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3063548&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'v sevljusi': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=688746&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'v uzgorode': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=690548&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'vila do conde': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2732649&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'vila nova de famalicao': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2732547&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'villeneuve dascq': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=6543862&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'vrsac': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=784136&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'vrutky': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3056683&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'vught': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2745154&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'w budine': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3078478&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'w pesti': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3054643&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'wattenheim': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2813638&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'weimar': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2812482&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'zurich': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2657896&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'freiburg i breisgau': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2925177&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'schwarz-kostelezt': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3073152&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'riedstadt': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3272941&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'glasgow': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2648579&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'teschen': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=3101321&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'polock': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=623317&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'brest': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=629634&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'leonberg': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2878695&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'warmbronn': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2814088&username=crosinski').json().items() if ka in places_geonames['praha']}})
# places_geonames_extra.update({'dresden': {ka:va for ka,va in requests.get('http://api.geonames.org/getJSON?geonameId=2935022&username=crosinski').json().items() if ka in places_geonames['praha']}})                         


# places_geonames = {k:v for k,v in places_geonames.items() if isinstance(v, dict)}
# places_geonames_2 = {k:v for k,v in places_geonames_2.items() if isinstance(v, dict)}

# places_geonames.update(places_geonames_2)
# places_geonames.update(places_geonames_extra)

# with open('translations_places_all.pickle', 'wb') as handle:
#     pickle.dump(places_geonames, handle, protocol=pickle.HIGHEST_PROTOCOL)
    
with open('translations_places_all.pickle', 'rb') as f:
    places_geonames = pickle.load(f)

#%%dane bibliograficzne

#wgranie kompletnych danych
all_records_df = pd.read_excel(r"C:\Users\Cezary\Downloads\everything_merged_2022-02-24.xlsx")

#wgranie pliku ondreja
ov_records = pd.read_excel(r"translation_database_clusters_year_author_language_2022-03-14.xlsx")

#filtrowanie all po ids z pliku ov
all_records_df = all_records_df.loc[all_records_df['001'].isin(ov_records['001'])]

#wgranie aktualnego pliku
translations_df = pd.read_excel('translations_after_manual_2022-11-02.xlsx')  
# translations_df = pd.read_excel('translation_before_manual_2022-09-20.xlsx')  

# translations_df_new.shape[0] #26392
#27068 -- all records
single_records = len([e for e in translations_df['group_ids'].to_list() if '❦' not in e])
# len([e for e in translations_df_new['group_ids'].to_list() if '❦' not in e]) #15130
# 15425 single records
grouped_records = len([e for e in translations_df['group_ids'].to_list() if '❦' in e])
# len([e for e in translations_df_new['group_ids'].to_list() if '❦' in e]) #11262
#grouped_records == 43%

grouped_ids = [el for sub in [e.split('❦') for e in translations_df['group_ids'].to_list() if '❦' in e] for el in sub]
# len([el for sub in [e.split('❦') for e in translations_df_new['group_ids'].to_list() if '❦' in e] for el in sub]) #35078
# 11643 rekordów powstało z 36749 rekordów

#%% przypisanie geonames do rekordów
country_codes = pd.read_excel('translation_country_codes.xlsx')

country_codes = [list(e[-1]) for e in country_codes.iterrows()]
country_codes = dict(zip([e[0] for e in country_codes], [{'MARC_name': e[1], 'iso_alpha_2': e[2], 'Geonames_name': e[-1]} for e in country_codes]))

places = all_records_df[['001', '008', '260']]

places['country'] = places['008'].apply(lambda x: x[15:18])
places.drop(columns='008', inplace=True)
places['country'] = places['country'].str.replace('\\', '', regex=False)
places['country_name'] = places['country'].apply(lambda x: country_codes[x]['MARC_name'] if x in country_codes else 'unknown')
places['geonames_name'] = places['country'].apply(lambda x: country_codes[x]['Geonames_name'] if x in country_codes else 'unknown')
places['places'] = places['260'].apply(lambda x: [list(e.values())[0] for e in marc_parser_dict_for_field(x, '\$') if '$a' in e] if not(isinstance( x, float)) else x)
places['places'] = places['places'].apply(lambda x: ''.join([f'$a{e}' for e in x]) if not(isinstance(x, float)) else np.nan)

places['places'] = places['places'].apply(lambda x: re.sub('( : )(?!\$)', r'$b', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub('( ; )(?!\$)', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub('\d', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub(' - ', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub(' \& ', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub(', ', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub('\(', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub('\[', r'$a', x) if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: re.sub('\/', r'$a', x) if pd.notnull(x) else x)

places['places'] = places['places'].apply(lambda x: [list(e.values())[0] for e in marc_parser_dict_for_field(x, '\$') if '$a' in e] if pd.notnull(x) else x)
places['places'] = places['places'].apply(lambda x: x if x else np.nan)
#manualne korekty
places.at[places.loc[places['001'] == 561629681].index.values[0], 'places'] = ['London', 'Glasgow']
places.at[places.loc[places['001'] == 162375520].index.values[0], 'places'] = ['Dresden', 'Leipzig']
places.at[places.loc[places['001'] == 469594167].index.values[0], 'places'] = ['Düsseldorf', 'Köln']
places.at[places.loc[places['001'] == 504116129].index.values[0], 'places'] = ['London', 'New York']
places.at[places.loc[places['001'] == 809046852].index.values[0], 'places'] = ['London', 'New York']
places.at[places.loc[places['001'] == 310786855].index.values[0], 'places'] = ['Leonberg', 'Warmbronn']
places.at[places.loc[places['001'] == 804915536].index.values[0], 'places'] = ['Polock', 'Brest']
places.at[places.loc[places['001'] == 263668500].index.values[0], 'places'] = ['Praha', 'Berlin']
places.at[places.loc[places['001'] == 504310019].index.values[0], 'places'] = ['Wien', 'Teschen']
places.at[places.loc[places['001'] == 367432746].index.values[0], 'places'] = ['Wien', 'Leipzig']

places['simple'] = places['places'].apply(lambda x: [simplify_place_name(e).strip() for e in x] if not(isinstance(x, float)) else x if pd.notnull(x) else x)
places['simple'] = places['simple'].apply(lambda x: [e for e in x if e] if not(isinstance(x, float)) else np.nan)                    
                   

places['geonames'] = places['simple'].apply(lambda x: [places_geonames[e]['geonameId'] if e in places_geonames else np.nan for e in x] if not(isinstance(x, float)) else np.nan)

places['geonames'] = places['geonames'].apply(lambda x: list(set([e for e in x if pd.notnull(e)])) if isinstance(x, list) else x)
places['geonames'] = places['geonames'].apply(lambda x: x if x else np.nan)

geonames_ids = [e for e in places['geonames'].to_list() if not(isinstance(e, float))]
geonames_ids = set([e for sub in geonames_ids for e in sub if not(isinstance(e, float))])

geonames_resp = {}
# for geoname in geonames_ids:
def get_geonames_country(geoname_id):
    # geoname_id = list(geonames_ids)[0]
    user = random.choice(geonames_users)
    #w funkcję wpisać losowanie randomowego username
    try:
        geonames_resp[geoname_id] = requests.get(f'http://api.geonames.org/getJSON?geonameId={geoname_id}&username={user}').json()['countryName']
    except KeyError:
        get_geonames_country(geoname_id)

with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(get_geonames_country, geonames_ids), total=len(geonames_ids)))
    
#dodać kolumnę    
    
places['geonames_country'] = places['geonames'].apply(lambda x: [geonames_resp[e] for e in x if not(isinstance(e, float))] if not(isinstance(x, float)) else x)   
    
#dodać koordynaty
    
def get_geonames_name(geoname_id):
    # geoname_id = list(geonames_ids)[0]
    user = random.choice(geonames_users)
    #w funkcję wpisać losowanie randomowego username
    try:
        geonames_resp[geoname_id] = requests.get(f'http://api.geonames.org/getJSON?geonameId={geoname_id}&username={user}').json()['name']
    except KeyError:
        get_geonames_name(geoname_id)    
        
def get_geonames_coordinates(geoname_id):
    # geoname_id = list(geonames_ids)[0]
    user = random.choice(geonames_users)
    #w funkcję wpisać losowanie randomowego username
    try:
        response = requests.get(f'http://api.geonames.org/getJSON?geonameId={geoname_id}&username={user}').json()
        lat = response['lat']
        lng = response['lng']
        geonames_resp[geoname_id] = {'lat': lat,
                                     'lng': lng}
    except KeyError:
        get_geonames_coordinates(geoname_id) 
    
geonames_resp = {}
with ThreadPoolExecutor(max_workers=50) as executor:
    list(tqdm(executor.map(get_geonames_name, geonames_ids), total=len(geonames_ids)))
places['geonames_place_name'] = places['geonames'].apply(lambda x: [geonames_resp[e] for e in x if not(isinstance(e, float))] if not(isinstance(x, float)) else x)   

geonames_resp = {}
with ThreadPoolExecutor(max_workers=50) as executor:
    list(tqdm(executor.map(get_geonames_coordinates, geonames_ids), total=len(geonames_ids)))
places['geonames_lat'] = places['geonames'].apply(lambda x: [geonames_resp[e]['lat'] for e in x if not(isinstance(e, float))] if not(isinstance(x, float)) else x)   
places['geonames_lng'] = places['geonames'].apply(lambda x: [geonames_resp[e]['lng'] for e in x if not(isinstance(e, float))] if not(isinstance(x, float)) else x)                              

places_full = places.copy() 
places = places[['001', 'geonames', 'geonames_place_name', 'geonames_country', 'geonames_lat', 'geonames_lng']].rename(columns={'geonames':'geonames_id','geonames_place_name':'geonames_name'})
for column in places.columns[1:]:
    places[column] = places[column].apply(lambda x: tuple(x) if isinstance(x, list) else x)

places = places.drop_duplicates()

places_grouped = places.groupby('001')
places_new = pd.DataFrame()
for name, group in tqdm(places_grouped, total=len(places_grouped)):
    # name = 3719272
    # group = places_grouped.get_group(name)
    if group.shape[0] > 1:
        group = group.loc[group['geonames_id'].notnull()]
        places_new = pd.concat([places_new, group])
    else:
        places_new = pd.concat([places_new, group])

for column in places_new.columns[1:]:
    places_new[column] = places_new[column].apply(lambda x: list(x) if isinstance(x, tuple) else x)
        
test = places_full.loc[places_full['001'].isin([e[0] for e in Counter(places_new['001']).most_common(118)])]
test2 = places_new.loc[places_new['001'].isin([e[0] for e in Counter(places_new['001']).most_common(118)])]

test2_grouped = test2.groupby('001')
test2 = {}
for name, group in tqdm(test2_grouped, total=len(test2_grouped)):
    # name = 2973189
    # group = test2_grouped.get_group(name).to_dict(orient='index')
    group = group.to_dict(orient='index')
    c = Counter()
    for d in group.values():
        c.update(d)
    c['001'] = name
    geo_ids = [e for e in Counter(c.get('geonames_id'))]
    geo_indices = [c.get('geonames_id').index(e) for e in geo_ids]
        
    for k,v in c.items():
        if isinstance(v, list):
            v = [e for i, e in enumerate(v) if i in geo_indices]
            c[k] = v
    group = {c.get('001'): {k:v for k,v in c.items() if k != '001'}}
    test2.update(group)
    
test2 = pd.DataFrame.from_dict(test2, orient='index').reset_index().rename(columns={'index':'001'})

places = places_new.loc[~places_new['001'].isin(test2['001'])]
places = pd.concat([places, test2]).sort_values('001').reset_index(drop=True)

#ile rekordów
places.shape[0] #54926
#ma gonames
places.loc[places['geonames_id'].notnull()].shape[0] #51498; 93.7%
#problematyczne rekordy:
    #	1015947429


#!!!             UWAGA!!!!!!!!
# !!!dla tych 6% jeśli można wskazać kraj, to wybieramy stolicę!!!

#%% porównanie geonames coverage
    
records_groups_dict = dict(zip(translations_df['001'].to_list(), [e.split('❦') for e in translations_df['group_ids'].to_list()]))
records_groups_multiple_dict = {k:v for k,v in records_groups_dict.items() if len(v) > 1}

places_dict = places.copy()
places_dict.index = places['001']
places_dict.drop(columns='001',inplace=True)
places_dict = places_dict.to_dict(orient='index')

#porównanie miejsc dla zgrupowanych rekordów

# tylko nazwy

places_compared = {k:[places_dict.get(int(e)).get('geonames_name') for e in v] for k,v in records_groups_multiple_dict.items()}

different_places = {k:v for k,v in places_compared.items() if not all_equal(v)}

different_places_ids = {k:records_groups_multiple_dict.get(k) for k,v in different_places.items()}
# 2900 (wcześniej: 3112) grup ma różne miejsca wydania

records_to_check_df = pd.DataFrame()
for k,v in tqdm(different_places_ids.items()):
    test_df_geo = places.loc[places['001'].isin([int(e) for e in v])]
    test_df = all_records_df.loc[all_records_df['001'].isin([int(e) for e in v])][['001', '008', '100', '245', '260']]
    test_df = pd.merge(test_df, test_df_geo, on='001', how='inner')
    test_df.insert(loc=0, column='group', value=k)
    records_to_check_df = pd.concat([records_to_check_df, test_df])
records_to_check_df['to separate'] = None

# 2900 (wcześniej: 3112) grup przekłada się na 12116 (wcześniej: 13139)

#%%
gc = gs.oauth()
gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

sheet = gc.create('deduplicated_records_to_be_checked', '1YLfF5NyFVXC6NYpp-WhjGxMDxqXn8FT3')
create_google_worksheet(sheet.id, 'deduplicated_records_to_be_checked', records_to_check_df)
#%% coma corrections

translations_df = pd.read_excel('translations_after_manual_2022-11-02.xlsx')  
deduplicated = gsheet_to_df('1jwbZZHqzETCkUW04M-ftFt1vx9yyBp_WIfPIJt0JOYs', 'deduplicated_records_to_be_checked').rename(columns={1:'001',8:'008',100:'100',245:'245',260:'260'})

country_codes = pd.read_excel('translation_country_codes.xlsx')
country_codes = [list(e[-1]) for e in country_codes.iterrows()]
country_codes = dict(zip([e[0] for e in country_codes], [{'MARC_name': e[1], 'iso_alpha_2': e[2], 'Geonames_name': e[-1]} for e in country_codes]))


places = translations_df[['001', 'author_id', '008', '260']]
places = deduplicated[['001', '008', '260']]
# places = places[places.index.isin([1, 2, 72, 155, 262, 445, 667, 1437, 2298, 4402, 13258])]
all_places = pd.DataFrame()
for places in [translations_df[['001', 'author_id', '008', '260']], deduplicated[['001', '008', '260']]]:

    places['001'] = places['001'].astype(np.int64)
    places['country'] = places['008'].apply(lambda x: x[15:18])
    places.drop(columns='008', inplace=True)
    places['country'] = places['country'].str.replace('\\', '', regex=False)
    places['country_name'] = places['country'].apply(lambda x: country_codes[x]['MARC_name'] if x in country_codes else 'unknown')
    places['geonames_name'] = places['country'].apply(lambda x: country_codes[x]['Geonames_name'] if x in country_codes else 'unknown')
    #155, 13258
    # places['places'] = places['260'].str.replace(' - ', '$a')
    places['places'] = places['260'].apply(lambda x: [list(e.values())[0] for e in marc_parser_dict_for_field(x, '\$') if '$a' in e] if not(isinstance( x, float)) else x)
    places['places'] = places['places'].apply(lambda x: ''.join([f'$a{e}' for e in x]) if not(isinstance(x, float)) else np.nan)
    
    places['places'] = places['places'].apply(lambda x: re.sub('( : )(?!\$)', r'$b', x) if pd.notnull(x) else x)
    places['places'] = places['places'].apply(lambda x: re.sub('( ; )(?!\$)', r'$a', x) if pd.notnull(x) else x)
    places['places'] = places['places'].apply(lambda x: re.sub('\d', r'$a', x) if pd.notnull(x) else x)
    places['places'] = places['places'].apply(lambda x: re.sub(' - ', r'$a', x) if pd.notnull(x) else x)
    places['places'] = places['places'].apply(lambda x: re.sub(' \& ', r'$a', x) if pd.notnull(x) else x)
    # places['places'] = places['places'].apply(lambda x: re.sub(', ', r'$a', x) if pd.notnull(x) else x)
    places['places'] = places['places'].apply(lambda x: re.sub('\(', r'$a', x) if pd.notnull(x) else x)
    places['places'] = places['places'].apply(lambda x: re.sub('\[', r'$a', x) if pd.notnull(x) else x)
    places['places'] = places['places'].apply(lambda x: re.sub('\/', r'$a', x) if pd.notnull(x) else x)
    
    places['places'] = places['places'].apply(lambda x: [list(e.values())[0] for e in marc_parser_dict_for_field(x, '\$') if '$a' in e] if pd.notnull(x) else x)
    places['places'] = places['places'].apply(lambda x: x if x else np.nan)
    
    places['simple'] = places['places'].apply(lambda x: [simplify_place_name(e).strip() for e in x] if not(isinstance(x, float)) else x if pd.notnull(x) else x)
    places['simple'] = places['simple'].apply(lambda x: [e for e in x if e] if not(isinstance(x, float)) else np.nan)
    
    places['comma'] = places['places'].apply(lambda x: any(e for e in x if ',' in e and (e.endswith(',') == False if e.count(',') == 1 else True)) if not(isinstance(x, float)) else False)
    places = places.loc[places['comma'] == True]
    
    try:
        places = places.loc[~places['001'].isin(all_places['001'])]
    except KeyError:
        pass
    
    ger_do_dopytania = [10000015968, 31413396, 72674954, 678825773, 263635407, 220880807, 760615705, 603848884, 719948759, 221924160, 678695024, 19318756, 57397812, 760555923]
    ger_out = [e for e in places.loc[places['country'] == 'gw']['001'].to_list() if e not in ger_do_dopytania]
    places = places.loc[~places['001'].isin(ger_out)]
    
    all_places = pd.concat([all_places, places])

# deduplicated.loc[deduplicated['001'] == '760555923'][['geonames_id', 'geonames_name', 'geonames_country']].values.tolist()
#manualnie
#72397243 to Berlin
#Duseldorf
#Iowa City
#TUTAJ
#72239206 to Praga

places_dict = {}
for i, row in tqdm(all_places.iterrows(), total=all_places.shape[0]):
    try:
        for el in row['simple']:
            if el and el not in places_dict:
                places_dict[el] = {'records': [row['001']],
                                   'country': [row['country_name']]}
            elif el: 
                places_dict[el]['records'].append(row['001'])
                places_dict[el]['country'].append(row['country_name'])
    except TypeError:
        pass
    
places_dict = {k:v for k,v in places_dict.items() if len(k) > 2}
places_dict_multiple_countries = {k:{ke:set([e for e in va if e not in ['No place, unknown, or undetermined', 'unknown']]) if ke == 'country' else va for ke,va in v.items()} for k,v in places_dict.items()}
{k:v.update({'geonames_country': {va['Geonames_name'] for ke,va in country_codes.items() if va['MARC_name'] in v['country']}}) for k,v in places_dict_multiple_countries.items()}
places_dict_multiple_countries = {k:{ke:{e for e in va if pd.notnull(e)} if ke == 'geonames_country' else va for ke,va in v.items()} for k,v in places_dict_multiple_countries.items()}

def get_most_frequent_country(x):
    try:
        return Counter({key:val for key,val in dict(Counter(x)).items() if key not in ['No place, unknown, or undetermined', 'unknown'] and pd.notnull(key)}).most_common(1)[0][0]
    except IndexError:
        return 'unknown'

places_dict = {k:{ke:get_most_frequent_country(va) if ke == 'country' else va for ke, va in v.items()} for k,v in places_dict.items()}
{k:v.update({'geonames_country': {va['Geonames_name'] for ke,va in country_codes.items() if va['MARC_name'] == v['country']}}) for k,v in places_dict.items()}
places_dict = {k:{ke:va.pop() if ke == 'geonames_country' and len(va) != 0 else va if ke != 'geonames_country' else np.nan for ke,va in v.items()} for k,v in places_dict.items()}

length_ordered = sorted([e for e in places_dict], key=len, reverse=True)
frequency = {k:len(v['records']) for k,v in places_dict.items()}
places_ordered = [e for e in sorted([e for e in frequency], key=frequency.get, reverse=True)]
places_clusters = cluster_strings(places_ordered, 0.75)
places_clusters_with_country = {k:[(e, places_dict[e]['country'], places_dict[e]['geonames_country']) for e in v] for k,v in places_clusters.items()}


#uwaga na wroclaw poland
atest = all_places.loc[all_places['simple'].apply(lambda x: any(e for e in x if e == 'luzern frankfurt am main'))]
deduplicated['001'] = deduplicated['001'].astype(np.int64)
deduplicated_limited = deduplicated.loc[~deduplicated['001'].isin(translations_df['001'])]
everything = pd.concat([translations_df, deduplicated_limited])[['001', 'geonames_id', 'geonames_name', 'geonames_country']]
all_places_wide = pd.merge(all_places, everything, on='001', how='left')

all_places_wide.to_excel('all_places.xlsx', index=False)
ttt = translations_df.loc[translations_df['001'] == 713003756]
[['geonames_id', 'geonames_name', 'geonames_country']].values.tolist()
deduplicated.loc[deduplicated['001'] == '10239118'][['geonames_id', 'geonames_name', 'geonames_country']].values.tolist()

#%% do odpytki po manualnej edycji

fixed_places_df = pd.read_excel('fixed_places.xlsx', sheet_name='fixed_places')
fixed_places = fixed_places_df[['001', 'geonames_id']].values.tolist()

# Counter([e[0] for e in fixed_places]).most_common(10)

fixed_places = dict(zip([e[0] for e in fixed_places], [literal_eval(e[-1]) for e in fixed_places]))

places_for_query = set([e for sub in fixed_places.values() for e in sub])

places_geonames = {}
users_index = 0

for place in tqdm(places_for_query):
    # place = list(places_for_query)[0]
    url = 'http://api.geonames.org/get?'
    params = {'username': geonames_users[users_index], 'geonameId': place}
    response = requests.get(url, params=params)
    
    tree = ElementTree.fromstring(response.content)
    data = dict(ChainMap(*[{e.tag: e.text} for e in tree if e.tag in ['name', 'lat', 'lng', 'geonameId', 'countryName']]))
    places_geonames.update({place:data})
    
fixed_places_geonames = {k:[places_geonames.get(e) for e in v] for k,v in fixed_places.items()}
{k:v for k,v in fixed_places_geonames.items() if any(e.get('geonameId') == '2661881' for e in v)}

df = pd.DataFrame()
for k,v in tqdm(fixed_places_geonames.items()):
    # k = 2968007
    # v = fixed_places_geonames.get(k)
    temp_df = pd.DataFrame(v)
    temp_df['001'] = k
    df = pd.concat([df, temp_df])

df.reset_index(drop=True, inplace=True)  
df = df.groupby('001').agg(lambda x: x.to_list()).reset_index()
# test = df.loc[df['001'] == 2968007]

df.to_excel('fixed_places_with_geonames.xlsx', index=False)

#%%następne kroki:
    #1. podmieniam deduplicated wartościami z fixed
fixed_df = gsheet_to_df('1jwbZZHqzETCkUW04M-ftFt1vx9yyBp_WIfPIJt0JOYs', 'deduplicated_records_to_be_checked_fixed')
fixed_df['geonames_id'] = fixed_df['geonames_id'].apply(lambda x: [int(e) for e in literal_eval(x)] if not isinstance(x, float) else x)

places_for_query = [e for e in fixed_df['geonames_id'] if not isinstance(e, float)]
places_for_query = set([e for sub in places_for_query for e in sub])

places_geonames = {}
users_index = 0
for place in tqdm(places_for_query):
    # place = list(places_for_query)[0]
    url = 'http://api.geonames.org/get?'
    params = {'username': geonames_users[users_index], 'geonameId': place}
    response = requests.get(url, params=params)
    
    tree = ElementTree.fromstring(response.content)
    data = dict(ChainMap(*[{e.tag: e.text} for e in tree if e.tag in ['name', 'lat', 'lng', 'geonameId', 'countryName']]))
    places_geonames.update({place:data})

fixed_places = dict(zip(fixed_df[1], fixed_df['geonames_id']))

fixed_places_geonames = {k:[places_geonames.get(e) for e in v] if isinstance(v, list) else v for k,v in fixed_places.items()}

df = pd.DataFrame()
for k,v in tqdm(fixed_places_geonames.items()):
    # k = 2968007
    # v = fixed_places_geonames.get(k)
    try:
        temp_df = pd.DataFrame(v)
    except ValueError:
        temp_df = pd.DataFrame()
    temp_df['001'] = k
    df = pd.concat([df, temp_df])

df.reset_index(drop=True, inplace=True)  
df = df.groupby('001').agg(lambda x: x.to_list()).reset_index().rename(columns={'countryName': 'geonames_country', 'geonameId': 'geonames_id', 'lng': 'geonames_lng', 'lat': 'geonames_lat', 'name': 'geonames_name'})[['001', 'geonames_id', 'geonames_name', 'geonames_country', 'geonames_lat', 'geonames_lng']]

fixed_df_corrected = pd.merge(fixed_df[['group', 1]].rename(columns={1: '001'}), df, on='001', how='left')

meta_fixed = fixed_df_corrected.groupby('group').agg(lambda x: x.to_list()).reset_index()
for column in meta_fixed[['geonames_id', 'geonames_name', 'geonames_country', 'geonames_lat', 'geonames_lng']]:
    new_column = []
    for cell in meta_fixed[column]:
        # cell = test['geonames_id'][2]
        cell = [e for e in cell if not isinstance(e, float)]
        cell = [e for sub in cell for e in sub]
        new_column.append(cell)
    meta_fixed[column] = new_column

def get_unique_indexes(x):
    # x = test['geonames_id'][1]
    indexes = []
    unique_values = []
    for i, e in enumerate(x):
        if e not in unique_values:
            unique_values.append(e)
            indexes.append(i)
    return indexes

meta_fixed['indexes'] = meta_fixed['geonames_id'].apply(lambda x: get_unique_indexes(x))

columns = ['geonames_id', 'geonames_name', 'geonames_country', 'geonames_lat', 'geonames_lng']
for column in columns:
    # column = columns[0]
    new_column = []
    for i, cell in enumerate(meta_fixed[column]):
        new_cell = [e for ind, e in enumerate(cell) if ind in meta_fixed['indexes'][i]]
        new_column.append(new_cell)
    meta_fixed[column] = new_column
meta_fixed.drop(columns='indexes', inplace=True)

    #2. grupuję wartości z deduplicated i podmieniam nimi translations_df
translations_df = pd.read_excel('translations_after_manual_2022-11-02.xlsx')
meta_fixed['001'] = meta_fixed['group'].astype(np.int64)
meta_fixed.drop(columns='group', inplace=True)

test = translations_df.loc[translations_df['001'].isin(meta_fixed['001'].to_list())]
test['001'] = test['001'].astype(np.int64)
test = pd.merge(test.drop(columns=['geonames_id', 'geonames_name', 'geonames_country', 'geonames_lat', 'geonames_lng']), meta_fixed, on='001', how='left')
translations_df = translations_df.loc[~translations_df['001'].isin(test['001'].to_list())]
translations_df = pd.concat([translations_df, test])
  
    #3. podmieniam translations_df tymi wartościami z fixed, które nie były użyte w deduplicated
correction_df = gsheet_to_df('1jwbZZHqzETCkUW04M-ftFt1vx9yyBp_WIfPIJt0JOYs', 'fixed')
correction_df = correction_df.loc[correction_df['jest w deduplicated'] == 'False'].drop(columns='jest w deduplicated')
correction_df['001'] = correction_df['001'].astype(np.int64)
test = translations_df.loc[translations_df['001'].isin(correction_df['001'].to_list())]
test['001'] = test['001'].astype(np.int64)
test = pd.merge(test.drop(columns=['geonames_id', 'geonames_name', 'geonames_country', 'geonames_lat', 'geonames_lng']), correction_df, on='001', how='left')
translations_df = translations_df.loc[~translations_df['001'].isin(test['001'].to_list())]
translations_df = pd.concat([translations_df, test])

translations_df.to_excel('translations_after_manual_2022-11-21.xlsx', index=False)
    #4. sortuję translations_df i patrzę, czy nie można, czegoś semi-automatycznie naprawić (np. Dusseldorf)
#WRÓCIĆ
edit = pd.read_excel('translations_after_manual_2022-11-21.xlsx', sheet_name='edit')
edit_x = edit.loc[edit['errors'] == 'x']
edit2 = pd.read_excel('test.xlsx')
edit2_x = edit2.loc[edit2['errors'] == 'x']

edit = pd.concat([edit_x,edit2_x])
edit.to_excel('edit.xlsx', index=False)

edited = pd.read_excel('edit.xlsx')
edited['geonames_id'] = edited['geonames_id'].apply(lambda x: [int(e) for e in literal_eval(x)] if not isinstance(x, float) else x)

places_for_query = [e for e in edited['geonames_id'] if not isinstance(e, float)]
places_for_query = set([e for sub in places_for_query for e in sub])

places_geonames = {}
users_index = 0
for place in tqdm(places_for_query):
    # place = list(places_for_query)[0]
    url = 'http://api.geonames.org/get?'
    params = {'username': geonames_users[users_index], 'geonameId': place}
    response = requests.get(url, params=params)
    
    tree = ElementTree.fromstring(response.content)
    data = dict(ChainMap(*[{e.tag: e.text} for e in tree if e.tag in ['name', 'lat', 'lng', 'geonameId', 'countryName']]))
    places_geonames.update({place:data})

fixed_places = dict(zip(edited['001'], edited['geonames_id']))

fixed_places_geonames = {k:[places_geonames.get(e) for e in v] if isinstance(v, list) else v for k,v in fixed_places.items()}

df = pd.DataFrame()
for k,v in tqdm(fixed_places_geonames.items()):
    # k = 2968007
    # v = fixed_places_geonames.get(k)
    try:
        temp_df = pd.DataFrame(v)
    except ValueError:
        temp_df = pd.DataFrame()
    temp_df['001'] = k
    df = pd.concat([df, temp_df])

df.reset_index(drop=True, inplace=True)  
df = df.groupby('001').agg(lambda x: x.to_list()).reset_index().rename(columns={'countryName': 'geonames_country', 'geonameId': 'geonames_id', 'lng': 'geonames_lng', 'lat': 'geonames_lat', 'name': 'geonames_name'})[['001', 'geonames_id', 'geonames_name', 'geonames_country', 'geonames_lat', 'geonames_lng']]

translations_df = pd.read_excel('translations_after_manual_2022-11-21.xlsx')
translations_df['geonames_id'] = translations_df['geonames_id'].apply(lambda x: [int(e) for e in literal_eval(x)] if pd.notnull(x) else x)
test = translations_df.loc[translations_df['001'].isin(df['001'].to_list())]
test = pd.merge(test.drop(columns=['geonames_id', 'geonames_name', 'geonames_country', 'geonames_lat', 'geonames_lng']), df, on='001', how='left')
translations_df = translations_df.loc[~translations_df['001'].isin(test['001'].to_list())]
translations_df = pd.concat([translations_df, test])
translations_df['geonames_id'] = translations_df['geonames_id'].apply(lambda x: [int(e) for e in x] if isinstance(x, list) else x)

translations_df.to_excel('translations_after_manual_2022-11-23.xlsx', index=False)

# geonames_ids = [literal_eval(e) for e in edit.loc[edit['errors'] == 'x']['geonames_id'].to_list()]
# geonames_ids = set([int(e) for sub in geonames_ids for e in sub])

# translations_df = pd.read_excel('translations_after_manual_2022-11-21.xlsx', sheet_name='Sheet1')
# translations_df['geonames_id'] = translations_df['geonames_id'].apply(lambda x: [int(e) for e in literal_eval(x)] if pd.notnull(x) else x)

# selected = translations_df.loc[translations_df['geonames_id'].apply(lambda x: True if isinstance(x, list) and [e for e in x if e in geonames_ids] else False)]

# ids = [1275339, 1692192, 1788927, 1793705, 1788927, 1793511, 1788927, 1805540, 1850147, 2514256, 2610310, 2618425, 2619669, 2638703, 264371, 2660076, 456172, 2719318, 2928810, 2761669, 2814880, 2825297, 2842688, 2873289, 2873759, 2873891, 12358494, 2898304, 2936759, 2942341, 2951881, 3057304, 2988507, 3077920, 3080047, 3085056, 3097257, 3102456, 3105976, 3117735, 3165185, 3170102, 3191839, 3194452, 3186886, 3448439, 3469058, 3703443, 4464368, 4509177, 4744091, 5052916, 4930956, 5037649, 5809844, 6544440, 668746, 6930414, 7280438, 758248, 8948794]

# selected_2 = translations_df.loc[translations_df['geonames_id'].apply(lambda x: True if isinstance(x, list) and [e for e in x if e in ids] else False)]
# selected_2 = selected_2.loc[~selected_2['001'].isin(edit_x['001'].to_list())][['001', '008', '100', '245', '260', 'geonames_id', 'geonames_name', 'geonames_country', 'geonames_lat', 'geonames_lng']]
# selected_2.to_excel('test.xlsx', index=False)

#tutaj zaznaczyć wszystkie rekordy, które mają podejrzane miejscowości (coś mogłem przeoczyć): arkusz edit
    #5. dla rekordów, które nie mają geonames, szukam po nazwie wydawnictwa, czy jest w rekordach z miejscem, jeśli tak, to przejmuję

edit = pd.read_excel('translations_after_manual_2022-11-21.xlsx', sheet_name='edit')
no_geo_tag = edit.loc[edit['geonames_id'].isna()][['001', '260']]
publishing_houses = [e[-1] for e in no_geo_tag.values.tolist() if isinstance(e[-1], str)]
publishing_houses = [[list(el.values()) for el in marc_parser_dict_for_field(e, '\$') if '$a' in el or '$b' in el] for e in publishing_houses]
publishing_houses = [e for sub in publishing_houses for e in sub]
publishing_houses = [e.strip() for sub in publishing_houses for e in sub]
test = dict(Counter(publishing_houses))

publisher_fixed = pd.read_excel('publishers_fixed.xlsx')
publisher_fixed = publisher_fixed.loc[publisher_fixed['geonames'].notnull()]
publisher_fixed['geonames'] = publisher_fixed['geonames'].apply(lambda x: [int(e) for e in literal_eval(x)])
publisher_fixed_dict = dict(zip(publisher_fixed['publisher'], publisher_fixed['geonames']))

places_for_query = set([el for sub in publisher_fixed_dict.values() for el in sub])

places_geonames = {}
users_index = 0
for place in tqdm(places_for_query):
    # place = list(places_for_query)[0]
    url = 'http://api.geonames.org/get?'
    params = {'username': geonames_users[users_index], 'geonameId': place}
    response = requests.get(url, params=params)
    
    tree = ElementTree.fromstring(response.content)
    data = dict(ChainMap(*[{e.tag: e.text} for e in tree if e.tag in ['name', 'lat', 'lng', 'geonameId', 'countryName']]))
    places_geonames.update({place:data})

fixed_places = {k:[places_geonames.get(e) for e in v] for k,v in publisher_fixed_dict.items()}

ttt = dict(zip(no_geo_tag['001'], no_geo_tag['260']))
ttt = {k:v for k,v in ttt.items() if not isinstance(v, float)}
ttt2 = {k:v for k,v in ttt.items() if any(e in v for e in fixed_places)}

test_dict = {}
for string in fixed_places:
    for field in ttt2.values():
        if string in field:
            test_dict.update({string: field})

print(list(test_dict.keys())[list(test_dict.values()).index(r'\1$bAbacus,$c2014.')])
ttt3 = {k:fixed_places.get(list(test_dict.keys())[list(test_dict.values()).index(v)]) if v in test_dict.values() else None for k,v in ttt2.items()}
fixed_places_geonames = {k:v for k,v in ttt3.items() if v}

df = pd.DataFrame()
for k,v in tqdm(fixed_places_geonames.items()):
    # k = 2968007
    # v = fixed_places_geonames.get(k)
    try:
        temp_df = pd.DataFrame(v)
    except ValueError:
        temp_df = pd.DataFrame()
    temp_df['001'] = k
    df = pd.concat([df, temp_df])

df.reset_index(drop=True, inplace=True)  
df = df.groupby('001').agg(lambda x: x.to_list()).reset_index().rename(columns={'countryName': 'geonames_country', 'geonameId': 'geonames_id', 'lng': 'geonames_lng', 'lat': 'geonames_lat', 'name': 'geonames_name'})[['001', 'geonames_id', 'geonames_name', 'geonames_country', 'geonames_lat', 'geonames_lng']]
df['geonames_id'] = df['geonames_id'].apply(lambda x: [int(e) for e in x])

test = translations_df.loc[translations_df['001'].isin(df['001'].to_list())]
test = pd.merge(test.drop(columns=['geonames_id', 'geonames_name', 'geonames_country', 'geonames_lat', 'geonames_lng']), df, on='001', how='left')
translations_df = translations_df.loc[~translations_df['001'].isin(test['001'].to_list())]
translations_df = pd.concat([translations_df, test])

translations_df.to_excel('translations_after_manual_full_geonames_2022-11-23.xlsx', index=False)
#do usunięcia:
    # 847948186, 855576165

# wszystkie geonames sprowadzić do postaci intigera

























    
   

















