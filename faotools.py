import pymongo, json
from constants import *

_producing_area_codes = None
def get_producing_area_codes():
    global _producing_area_codes
    if _producing_area_codes is None:
        _producing_area_codes = list(set(db.production_crops.distinct(AREA_CODE) + db.production_livestock_primary.distinct(AREA_CODE)))
    return _producing_area_codes

_producing_areas = None
def get_producing_areas():
    global _producing_areas
    if _producing_areas is None:
        _producing_areas = db.production_livestock.aggregate([{'$group': {'_id': {'Area': '$Area', 'Area Code': '$Area Code'}}}])
        _producing_areas = [d['_id'] for d in _producing_areas]
        _producing_areas = sorted(_producing_areas, key=lambda x: x['Area'])
    return _producing_areas

_production_crop_item_codes = None
def get_production_crop_item_codes():
    global _production_crop_item_codes
    if _production_crop_item_codes is None:
        _production_crop_item_codes = db.production_crops.distinct(ITEM_CODE)
    return _production_crop_item_codes

_production_livestock_primary_item_codes = None
def get_production_livestock_primary_item_codes():
    global _production_livestock_primary_item_codes
    if _production_livestock_primary_item_codes is None:
        _production_livestock_primary_item_codes = db.production_livestock_primary.distinct(ITEM_CODE)
    return _production_livestock_primary_item_codes

def get_element_value(item_code, year, area_code, collection_name, element_code):
    query = {ELEMENT_CODE: element_code}
    projection = {VALUE: 1, FLAG: 1, UNIT: 1}
    if year is not None:
        query[YEAR_CODE] = year
    else:
        projection[YEAR_CODE] = 1
    if area_code is not None:
        query[AREA_CODE] = area_code
    else:
        projection[AREA_CODE] = 1
        projection[AREA] = 1
    query[ITEM_CODE] = item_code
    queryset = db[collection_name].find(query, projection)
    result = list(queryset)
    num_results = len(result)
    if num_results == 0:
        return 0, None, None
    elif num_results == 1:
        rec = result[0]
        return rec[VALUE], rec[UNIT], rec[FLAG]
    else:
        return result

def get_country_area(area_code, year=None):
    return get_element_value(COUNTRY_AREA_ITEM_CODE, year, area_code, LAND_USE_COL, AREA_ELEMENT_CODE)

def get_yield_for_crop(item_code, year=None, area_code=None):
    return get_element_value(item_code, year, area_code, PRODUCTION_CROPS_COL, YIELD_ELEMENT_CODE)

def get_area_harvested_for_crop(item_code, year=None, area_code=None, fields=['value']):
    return get_element_value(item_code, year, area_code, PRODUCTION_CROPS_COL, AREA_HARVESTED_ELEMENT_CODE)

def get_production_for_crop(item_code, year=None, area_code=None, fields=['value']):
    return get_element_value(item_code, year, area_code, PRODUCTION_CROPS_COL, PRODUCTION_ELEMENT_CODE)

def get_num_animals(item_code, year=None, area_code=None):
    return get_element_value(item_code, year, area_code, PRODUCTION_LIVESTOCK_COL, STOCKS_ELEMENT_CODE)

def get_production_for_livestock_primary(item_code, year=None, area_code=None):
    return get_element_value(item_code, year, area_code, PRODUCTION_LIVESTOCK_PRIMARY_COL, PRODUCTION_ELEMENT_CODE)

def get_indigenous_production_for_livestock_primary(item_code, year=None, area_code=None):
    return get_element_value(item_code, year, area_code, PRODUCTION_LIVESTOCK_PRIMARY_COL, PRODUCTION_ELEMENT_CODE)   

with open('config.json', 'r') as config_file:
    config = json.loads(config_file.read())

client = pymongo.MongoClient(config['mongo_host'], config['mongo_port'])
db = client[config['mongo_name']]


#get_production_crop_item_codes()
#print(get_production_for_crop(15, 2002, 5501))
#print(get_country_area(5501, 2002))
#print(get_num_animals(1107, year=2002, area_code=2))
#print(get_production_for_livestock_primary(919, year=2002, area_code=2))