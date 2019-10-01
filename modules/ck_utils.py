import requests, time, json, bs4, os, pandas as pd, re, pytz, logging, pdb, boto3, matplotlib.pyplot as plt, reverse_geocoder as rg, iso3166, asks, trio, copy, sys, numpy as np
from collections import Counter
from enum import Enum, auto
from geopy.geocoders import Nominatim
from datetime import timedelta, date, datetime
from dgnutils import *

CK_API_SECRET = os.environ['CK_API_SECRET']
CK_API_KEY = os.environ['CK_API_KEY']
boto3.setup_default_session(profile_name='might-could')
asks.init("trio")
# pdb.set_trace() #Tracing

# API WRAPPER {{{

class CK_Api(Enum):
	# All enums must have "data_name" and "api_type"
	TAG_SUBS = {'data_name': 'tags', 'api_type': 'subscriptions', 'item_type': 'url'}
	SEQ_SUBS = {'data_name': 'courses', 'api_type': 'subscriptions', 'item_type': 'url'} # courses is legacy name for v3
	FORM_SUBS = {'data_name': 'forms', 'api_type': 'subscriptions', 'item_type': 'url'}
	TAG_LIST = {'data_name': 'tags', 'api_type': 'list'}
	SEQ_LIST = {'data_name': 'courses', 'api_type': 'list'} # courses is legacy name for v3
	FORM_LIST = {'data_name': 'forms', 'api_type': 'list'}
	SUB = {'data_name': 'subscribers', 'api_type': 'other', 'item_type':'url'}
	SUB_EMAIL = {'data_name': 'subscribers', 'api_type': 'other', 'item_type': 'payload', "payload_type": "email_address"} 
	ALL_SUBS = {'data_name': 'subscribers', 'api_type': 'other'}
	
	def url_and_data(self, data):
		""" Get the url string with data"""
		return_data = {}

		url = 'https://api.convertkit.com/v3/' + self.value["data_name"] # starting for all apis

		if "item_type" in self.value:
			if self.value["item_type"] == "url": url += f"/{data}" # If data goes in the url do it here
			if self.value["item_type"] == "payload": return_data[self.value["payload_type"]] = data # if payload, do it here

		if self.value["api_type"] == "subscriptions": url += f"/subscriptions" # if api is for subscriptions, get them here
		
		return url, return_data

def get_flat_sub_dict(sub):
	"""convert a nested dictionary of a subscriber into a flat dict"""
	return {k:v for k,v in list(sub.items()) + list(sub.get('fields',{}).items()) if k != 'fields'}
	

def get_api_data(ck_api: CK_Api, api_item: int, include_unsubscribers=False, test=False, session=None, **kwargs):
	"""
	Currently accepts the following APIs
		TAG_SUBS: e.g. https://api.convertkit.com/v3/tags/899142/subscriptions (api_item = tag_id)
		SEQ_SUBS: e.g. https://api.convertkit.com/v3/sequences/375580/subscriptions (api_item = sequence_id)
		FORM_SUBS: e.g. https://api.convertkit.com/v3/forms/341476/subscriptions (api_item = form_id)
		SUB: e.g. https://api.convertkit.com/v3/subscribers/531681162 (api_item = subscriber_id)
		SUB_EMAIL: e.g. https://api.convertkit.com/v3/subscribers (api_item = email_address)
		ALL_SUBS: e.g. https://api.convertkit.com/v3/subscribers (api_item = None)
	Currently only works with GET commands
	Uses concurrency if there is more than 1 page to gather (trio and asks)
	
	Parameters
	----------
	ck_api (CK_Api(Enum)): CK Api to determine the api to evaluate
	api_item (int): The tag, or sequence based on the "ck_api" that was passed in
	include_unsubscribers (boolen): Determines whether unsubscribers should be included
	test (boolean): provides only the first page for multi-page responses
	session (session obj): allows all requests to be passed through a session 
	"""  

	return_data = []; total_pages=1
	if not session: session = requests.session()
	headers={'content-type': 'application/json'}
	data = {'api_secret': CK_API_SECRET, 'api_key': CK_API_KEY, **kwargs};
	
	url, extra_data = ck_api.url_and_data(api_item)
	data.update(extra_data)
	
	if not include_unsubscribers: 
		data['subscriber_state'] = 'active'
	else:
		data['sort_field'] = 'cancelled_at'
	 
	logging.debug(f'Getting url {url} with data {data} for get_api_data') 
	
	# First pull to get total_pages (and all data if total_pages is 1)
	with session.get(url, data=json.dumps(data), headers=headers) as resp:
		error_catching(resp, url) # Error catching
		json_resp = json.loads(resp.text)
		total_pages = json_resp.get('total_pages', 1) if not test else 1;

	if total_pages == 1:
		return_data = extract_ck_api_data(ck_api, json_resp)
	else:
		# Do async pull of the data
		return_data = trio.run(get_api_data_async, url, ck_api, data, headers, total_pages) 

	return return_data

def error_catching(resp, url):
	"""
	Does the error catching for get_api_data
	"""
	if resp.status_code != 200: raise Exception(f"FAILURE ({resp.status_code} is non-200 code)::{url}")
	if json.loads(resp.text).get('error', '').lower() == "not found": raise SubscriberDNE('Possibly this subscriber was deleted')
	if resp.text.strip().lower() == 'forbidden': raise ConnectionError('Connection result is "Forbidden". Please wait a while')

def extract_ck_api_data(ck_api, json_resp):
	"""
	Gets the necessary data from a json_resp in get_api_data (could be concurrent or syncronous)
	"""
	return_data = []
	# convert json_resp into 
	if ck_api.value["api_type"] == 'subscriptions': # form subs, seq subs, etc
		subscriptions = json_resp['subscriptions']
		return_data += [{k:v if k != 'subscriber' else get_flat_sub_dict(v) for k,v in s.items()} for s in subscriptions]
	elif ck_api.value["api_type"] == 'list': # list forms, list sequences, etc
		return_data += json_resp[ck_api.value["data_name"]]
	elif ck_api == CK_Api.SUB:
		logging.debug('Confirmed type is sub')
		return_data = get_flat_sub_dict(json_resp.get('subscriber',{}))
	elif ck_api == CK_Api.SUB_EMAIL:
		logging.debug('Confirmed type is sub_email')
		return_data = get_flat_sub_dict(next(iter(json_resp.get('subscribers',[{}])), {})) # Get first item
	elif ck_api == CK_Api.ALL_SUBS:
		return_data += [get_flat_sub_dict(s) for s in json_resp.get('subscribers', [])]
	else:
		raise Exception(f'Could not confirm type of {ck_api}, {CK_Api.SEQ_SUBS}, {ck_api==CK_Api.SEQ_SUBS}')
	return return_data

# https://trio.readthedocs.io/en/stable/tutorial.html
# https://stackoverflow.com/questions/52671346/how-to-gather-task-results-in-trio 
async def fetch(url, ck_api, data, headers, results, page):
	"""
	Child concurrent get of CK api page, extracts necessary info based on the ck_api in use
	"""
	json_resp = json.loads((await asks.get(url, data=json.dumps({**data, 'page':page}), headers=headers)).text)
	result = extract_ck_api_data(ck_api, json_resp)
	results += result if type(result)==list else [result]

async def get_api_data_async(url, ck_api, data, headers, total_pages):
	"""
	Parent concurrent get of CK api page, extracts necessary info based on the ck_api in use
	"""
	results = []
	async with trio.open_nursery() as nursery:
		for page in range(total_pages):
			nursery.start_soon(fetch, url, ck_api, data, headers, results, page + 1) # "1 to total" vs of "0 to total-1"
	return results

async def get_api_single_subscribers_async(subscribers):
	"""
	Parent concurrent get of CK api page, extracts necessary info based on the ck_api in use
	"""
	results = []
	ck_api = CK_Api.SUB
	data = {'api_secret': CK_API_SECRET, 'api_key': CK_API_KEY};
	headers={'content-type': 'application/json'}
	async with trio.open_nursery() as nursery:
		for subscriber in subscribers:
			url, _ = ck_api.url_and_data(subscriber)
			nursery.start_soon(fetch, url, ck_api, data, headers, results, 1) # "1 to total" vs of "0 to total-1"
	return results
# results = trio.run(get_api_data_async, url, ck_api, data, headers, total_pages)


# get_api_data(CK_Api.SUB, 531681162)
# get_api_data(CK_Api.SUB_EMAIL, "dalefarwalker@gmail.com")
# get_api_data(CK_Api.TAG_SUBS, 969850, test=True)
# get_api_data(CK_Api.SEQ_SUBS, 377440, test=True)
# get_api_data(CK_Api.ALL_SUBS, None, test=True)
	
def update_sub_everywhere_with_misc(sub_id:int, cursor, session=None, **fields):
	if type(sub_id) != int: raise Exception(f'Must provide an int for sub_id, not {type(sub_id)}')
	try:
		sub_data = update_ck_sub_with_misc(sub_id, session=session, **fields)
		update_subscriber_in_rds(sub_id, cursor, sub_data=sub_data)
	except:
		sub_data = None
		print(f'Error with sub_id {sub_id}')
	return sub_data

def update_ck_sub_with_misc(sub_id:int, session=None, **fields):
	"""
	Updates a subscriber in CK with the data passed in the dictionary "fields"
	
	Parameters
	==========
	fields: This is the dictionary of items that will be update in CK. 
	It will be passed as a subitem of "fields". Example items are "unsubscribe_date", "loc_city", "loc_state", "subscribe_date"
	"""
	if not session: session = requests.session()
	r = session.put(
		url=f'https://api.convertkit.com/v3/subscribers/{sub_id}', 
		data=json.dumps({'api_secret': CK_API_SECRET, 'fields':fields}), 
		headers={'content-type': 'application/json'}
	);

	if r.status_code not in [200, 202]: 
		logging.warn(f'Status code of {r.status_code} not in 200s')
		raise Exception(r.text)
	return get_flat_sub_dict(json.loads(r.text)["subscriber"])

def has_attrs(subscriber, _all=True, **attrs):
	""" 
	Parameters
	==========
	subscriber (dict): normal subscriber dict {'id':1111, 'state':active, ...}
	attr (kwarg): the aspect to evaluate
	_all (bool): If true, use "all", otherwise use "any"
	"""
	bool_fn = all if _all else any
	attrs = {k:v if type(v) in [list, tuple] else [v] for k,v in attrs.items()}; # ensure they are all lists
	return bool_fn(subscriber.get(k, object) in v for k,v in attrs.items())

# }}}

# MYSQL WRAPPER {{{
def get_mysql_dicts(cursor):
	"""
	Returns
	=======
	all_rds_dict -> subscriber_id: subscriber info
	launch_info_dict -> "Launch_3": launch info
	launch_dict -> subscriber_id: ["Launch_1", "Launch_2", "Launch_3"]
	purchase_dict -> subscriber_id: purchase info
	emails_dict -> subscriber_id: purchase info
	"""
	print(f'Loading (1/5), all_rds_dict', end='\r')
	all_rds_dict = {a['id']:a for a in cursor.e('SELECT * FROM subscriber_data')}
	print(f'Loading (2/5), launch_info_dict', end='\r')
	launch_info_dict = {l['launch']:l for l in cursor.e('SELECT * FROM launch_info')}
	launch_dict = {}
	print(f'Loading (3/5), launch_dict', end='\r')
	for l in cursor.e(f'SELECT * FROM launch_data'):
		launch_dict.setdefault(l['subscriber_id'], []).append(l['launch'])
	print(f'Loading (4/5), purchase_dict', end='\r')
	purchase_dict = {p['subscriber_id']:p for p in cursor.e(f'SELECT * FROM purchase_data') if p['subscriber_id']}
	print(f'Loading (5/5), emails_dict', end='\r')
	emails_dict = {}
	for e in cursor.e(f'SELECT * FROM email_data'):
		emails_dict.setdefault(e['subscriber_id'], []).append(e)
	return all_rds_dict, launch_info_dict, launch_dict, purchase_dict, emails_dict
# }}}

# QUALITY {{{

# course_email_ids last updated 9/25/19
course_email_ids = [1024762, 1033254, 1024765, 1024766, 1024767, 1024768, 1024769, 1088185, 1088196, 1088197, 1088198, 1088199, 1088200, 1191656, 1191657, 1191658, 1191659, 1191660, 1191661, 1191662, 1313872, 1314658, 1313641, 1313860, 1313861, 1313863, 1866878, 1866873, 1866874, 1866875, 1866876, 1866877, 1866879, 2014727, 2014728, 2014729, 2014730, 2016262, 2014731, 2014732, 2014743, 2014744, 2014745, 2014746, 2177600, 2177601, 2177602, 2177603, 2177604, 2177605, 2177606, 2208048, 2208049, 2208050, 2208051, 2208052, 2208053, 2208054]
averages = {'open_rate': 0.4015185973149601, 'click_rate': 0.08394148324934214, 'rec_open_rate': 0.5778960726689762, 'rec_click_rate': 0.2633010380622846, 'course_open_rate': 0.44535748106058454, 'course_click_rate': 0.22035260930888007, 'past_course_open_rate': 0.4693398692810468, 'past_course_click_rate': 0.1440803621958105}

def delivered_before(x, ref_date): return x['deliver_date'] < ref_date
def delivered_after(x, ref_date): return x['deliver_date'] > ref_date
def opened_before(x, ref_date): return bool((x['open_date'] and x['open_date'] < ref_date) or (x['click_date'] and x['click_date'] < ref_date))
def clicked_before(x, ref_date): return bool(x['click_date'] and x['click_date'] < ref_date)

def sub_quality(cursor, sub_id):
	emails = cursor.e(f'SELECT * FROM email_data WHERE subscriber_id={sub_id}')
	sub_data = next(iter(cursor.e(f'SELECT * FROM subscriber_data WHERE id={sub_id}')), None)
	sub_date = sub_data['subscribe_date']
	if sub_data['state'] != 'active': return 0
	return sub_quality_from_emails(emails, sub_date)
		
def sub_quality_from_emails(emails, sub_date):
	"""
	emails (list:dict): should come directly from cursor.e('SELECT * FROM email_data WHERE subscriber_id={}')
	"""
	if not sub_date: return 0
	if type(sub_date)==str: sub_date = conv_time_1(sub_date) # convert to datetime
	sub_length = (datetime.today() - sub_date).days
	
	open_rate = get_rate(emails) or averages['open_rate']
	click_rate = get_rate(emails, _open=False) or averages['click_rate']
	
	rec_emails = [e for e in emails if delivered_before(e, datetime.today() - timedelta(days=30))]
	rec_open_rate = get_rate(rec_emails) or averages['rec_open_rate']
	rec_click_rate = get_rate(rec_emails, _open=False) or averages['rec_click_rate']
	
	past_course_emails = [e for e in emails if e['email_id'] in course_email_ids]
	past_course_open_rate = get_rate(past_course_emails) or averages['past_course_open_rate']
	past_course_click_rate = get_rate(past_course_emails, _open=False) or averages['past_course_click_rate']
	
	quality_value = -0.006333131 + \
	sub_length * 1.54689e-05 + \
	open_rate * 0.017475117 + \
	click_rate * 0.06696054 + \
	rec_open_rate * 0.012927518 + \
	rec_click_rate * 0.035823455 + \
	past_course_open_rate * -0.012705248 + \
	past_course_click_rate * 0.05237587
	return quality_value		

def get_rate(email_list, act_before=None, del_before=None, _open=True):
	"""
	act_before: only count the emails in the rate (open or click) if the action was taken before this date
	del_before: only evaluate emails that were delivered before this date
	"""
	fn = opened_before if _open else clicked_before
	if not act_before: act_before = datetime.today()
	if not del_before: del_before = datetime.today()
	actions = [fn(e, act_before) for e in email_list if delivered_before(e, del_before)] # opens or clicks
	return round(sum(actions)/len(actions), 2) if actions else None

# }}}

# CRM {{{

def include(item, _all=True, **attrs):
	""" 
	Parameters
	==========
	item (dict): is purchase or launch, must have subscriber dict included in the data
	attr (kwarg): the aspect to evaluate (e.g. lead_opt-in = Style Mini-course)
	_all (bool): If true, use "all", otherwise use "any"
	"""
	bool_fn = all if _all else any

	attrs = {k:v if type(v) in [list, tuple] else [v] for k,v in attrs.items()}; # ensure all the values to evaluate are all lists
	
	return bool_fn(item.get(k, object) in v for k,v in attrs.items())

def get_detailed_purchase_and_launch_data(cursor):
	"""
	Returns launch and purchase data with subscriber data included by grabbing sub, purchase, and launch data
	from MYSQL and then combining them. Adds country data by reverse geocode lookup
	
	Returns
	=======
	purchases (dict): MYSQL purchase data with subscriber_id data included (one dict with all data)
	launches (dict): MYSQL launch data with subscriber_id data included (one dict with all data)
	"""
	d = lambda x: datetime.strptime(x, '%Y-%m-%d') if x else None # Date conversion

	subs = cursor.e("SELECT * FROM subscriber_data") # Get all subscribers
	locs = [[s['id'], (float(s['loc_lat']), float(s['loc_lng']))] for s in subs if s['loc_lat'] and s['loc_lng']]
	ids, coords = zip(*locs)
	countries = [iso3166.countries_by_alpha2.get(r['cc'], None).name for r in rg.search(coords, mode=1)]; len(countries)
	country_dict = {a[0]:a[1] for a in zip(ids,countries)} #dictionary of countries for subscriber id
	subs = [{**s, 'country': country_dict.get(s['id'], None)} for s in subs]
	sub_dict = {s['id']:s for s in subs}; len(sub_dict) # Dictionary by id

	purchases = [{**sub_dict.get(p['subscriber_id'], {}), **p} for p in cursor.e("SELECT * FROM purchase_data")]
	launches = [{**sub_dict.get(l['subscriber_id'], {}), **l} for l in cursor.e("SELECT * FROM launch_data")]
	
	return purchases, launches

def specific_purchase_rate(purchases, launches, _all=True, cursor=None, **attrs):
	"""
	Parameters
	==========
	**attrs (kw dict): key-value pairs that specify the subscribers to select. {'lead_source':'FB Ad'} would limit
		the subscribers to those that came into the list from a FB Ad
	_all (boolean): Where to use "all" (True) or "any" (False) when evaluating all attrs
	purchases (dict): The purchase data to use in evalution. Should come from "get_detailed_purchase_and_launch_data"
	launches (dict): The launch data to use in evalution. Should come from "get_detailed_purchase_and_launch_data"
	cursor: Used to get the purchases and launches if not provided
	"""
	if not launches or not purchases:
		logging.warning(f'Manually gathering the purchase and launch data. This is inefficient for multiple calls')
		purchases, launches = get_detailed_purchase_and_launch_data(cursor)

	join_str = " AND " if _all else " OR "
	logging.info(f'Evaluating: {join_str.join([f"({k} IN {v})" for k,v in attrs.items()])}')
	
	specific_purchases = [p for p in purchases if include(p, _all=_all, **attrs)]
	specific_launches = [l for l in launches if include(l, _all=_all, **attrs)]
	
	return specific_purchases, specific_launches

def transfer_good_mismatches_to_rds(cursor, session, rds_data, ck_data):
		"""
		==========
		Parameters
		==========
		Mismatches (list:int): list of subscriber ids that have a mismatch between CK and RDS
		
		Move all mismatch data to rds unless it is None, "null" in CK
		"""
		nones = [None, 'null', '']
		if rds_data['id'] != ck_data['id']: raise Exception(f'ids did not match! {rds_data["id"]}, {ck_data["id"]}')

		ck_copied_data = copy.deepcopy(ck_data)
		for mm_k, mm_v in array_mismatch(rds_data, ck_copied_data).items():
			if mm_v[1] in nones: del ck_copied_data[mm_k]
		# print(ck_sub_data)
		update_subscriber_in_rds(rds_data['id'], cursor, ck_copied_data, session=session)

# }}}

# GENERAL CONVERTKIT FUNCTIONS {{{

# Error for when a subscriber does not exist (possibly due to deletion)
class SubscriberDNE(Exception): pass

def update_subscriber_in_rds(sub_id, cursor, sub_data=None, session=None):
	"""
	Updates subscriber in RDS
	Does not autocommit
	"""

	# sub_data from CK is the default
	if not sub_data:
		if not session: session = requests.session()
		sub_data = get_single_subscriber(session=session, subscriber_id=sub_id) # Get the data from CK

	if 'last_name' in sub_data: del sub_data['last_name'] # last name is not held in the database
	if sub_data.get('loc_lat', None) == 'null': sub_data['loc_lat'] = None
	if sub_data.get('loc_lng', None) == 'null': sub_data['loc_lng'] = None

	results = cursor.dict_insert([sub_data], 'subscriber_data') # Must provide a list of dictionaries
	logging.info(f'Updated RDS with {results} new rows')
	return results

def update_single_subscriber(sub_id, cursor, ck_app_session=None, do_location=False, geolocator=None, overwrite=False):
	"""
	Updates subscriber in CK (location if not complete and subscriber_date if empty) and RDS. Gets CK data and scrapes for city and state
	Does not autocommit

	Parameters
	==========
	sub_id (int): the subscriber to update
	conn: the connection to use to update 
	session: for reusing network connections
	geolocator: the connection to use to get lat and lng
	overwrite (bool): If true, the function will overwrite the city, state, lat, lng, and subscription date regardless of existing values
	"""
	if not ck_app_session: ck_app_session = get_ck_session()
	if not cursor: raise ReferenceError('No cursor provided')
	nones = [None, 'null', ''];
	
	fields = {}
	sub_data = get_single_subscriber(session=ck_app_session, subscriber_id=sub_id) # Get the data from CK

	if do_location:
		if not geolocator: raise ReferenceError('No geolocator provided, try geolocator = Nominatim(user_agent="dgn_app")')
		# If loc_city, loc_state, loc_lat, or loc_lng are not completed, then update location
		if overwrite or (sub_data['loc_city'] in nones or sub_data['loc_state'] in nones or sub_data['loc_lat'] in nones or sub_data['loc_lng'] in nones):
			loc_city, loc_state = scrape_loc_from_app(sub_id, ck_app_session=ck_app_session)
			latlng = get_lat_and_lng(loc_city=loc_city, loc_state=loc_state, geolocator=geolocator)
			sub_data['loc_city'] = loc_city
			sub_data['loc_state'] = loc_state
			sub_data['loc_lat'] = latlng['lat']
			sub_data['loc_lng'] = latlng['lng']
	
	# If subscribe_date is None then update it
	if overwrite or sub_data['subscribe_date'] in nones:
		if sub_data['state'] in ['active', 'cancelled']:
			if 'mailchimp_sub_date' in sub_data and sub_data['mailchimp_sub_date']:
				subscribe_date = mailchimp_conv(sub_data['mailchimp_sub_date'])
			else:
				subscribe_date = created_at_conv(sub_data['created_at'])
		else: raise Exception('non-active and non-cancelled states not designed for yet')
		sub_data['subscribe_date'] = subscribe_date

	update_ck_sub_with_misc(sub_id, session=ck_app_session, **sub_data) # Make the update on the CK page (shouldn't be any unless loc or subscription date need update)
	logging.info(f'Updated {sub_id} with {fields}')

	results = update_subscriber_in_rds(sub_id, cursor, sub_data=sub_data)
	logging.debug(f'Updated {sub_id} with {fields}')
	logging.info(f'Updated RDS with {results} rows')

	return results

def get_ck_session():
	"""Handle the initial page load and session creation
	Returns an authenticated session"""

	session_requests = requests.session()
	login_url = "https://app.convertkit.com/users/login"

	result = session_requests.get(login_url)
	login_tree = bs4.BeautifulSoup(result.text, 'html.parser'); login_tree
	if not result.ok: raise ConnectionError(result.status_code)

	payload = {
		"user[email]": os.environ['CK_USER'],
		"user[password]": os.environ['CK_PASSWORD'],
		"authenticity_token": login_tree.find("input", {"name":"authenticity_token"})['value']
	};

	login_result = session_requests.post(login_url, data = payload, headers = dict(referer=login_url));
	if not login_result.ok: raise ConnectionError(login_result.status_code)
	return session_requests


def array_mismatch(array1, array2):
	"""
	Determine if one Subscriber array is different than another Subscriber array

	Parameters
	==========
	array1, array2 (list): Flat subscriber arrays
	"""
	get_date = lambda x : datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%f%z').date()
	today = datetime.now().strftime("%Y-%m-%d")
	nones = [None, 'null', ''];
	mismatches = {} # store the mismatches
	for key in array1:
		if key == 'created_at': continue #ignore "created_at"
		if key == 'first_name': continue
		if key == 'unsubscribe_date' and array1[key] == datetime.now().date().strftime("%Y-%m-%d"): continue
		if key == 'unsubscribe_date' and array2[key] == datetime.now().date().strftime("%Y-%m-%d"): continue

		# Test equality; use str and rstrip to compare Decimals (this could be its own function)
		if key not in array2:
			mismatches[key] = [array1[key], '#DNE#']
			continue
		if array1[key] in nones and array2[key] in nones: continue # Skip if both are none
		if str(array1[key]).rstrip('0') != str(array2[key]).rstrip('0'): # and (array1[key] and array2[key]): # This part was causing me to miss None=Anything
			# Skip if it is just because it just happened
			if key == 'state' and (array1['unsubscribe_date'] == today or array2['unsubscribe_date'] == today): continue
			mismatches[key] = [array1[key], array2[key]]
	return mismatches

# }}}

# LOCATION FUNCTIONS {{{

def scrape_loc_from_app(sub_id, ck_app_session=None):
	"""
	Scrapes the page to find loc of city and state

	Returns
	=======
	[loc_city, loc_state] (list(str, str)): returns the string of the city and state, or else None

	"""
	
	# Sub function
	has_city_and_state = lambda tag: tag.has_attr('data-city') and tag.has_attr('data-state') # Determine if the tag has city and state

	# Creating a session with Convertkit app (not API)
	if not ck_app_session: ck_app_session = get_ck_session()
	
	# Scrape the location data
	url = f'https://app.convertkit.com/subscribers/{sub_id}'
	sub_result = ck_app_session.get(url, headers = dict(referer = url))

	if sub_result.status_code!=200: raise ConnectionError
		
	sub_tree = bs4.BeautifulSoup(sub_result.text, 'html.parser'); sub_tree
	div = sub_tree.find(has_city_and_state); div
	loc_city = div['data-city'] if div['data-city'] != 'null' else None
	loc_state = div['data-state'] if div['data-state'] != 'null' else None
	return [loc_city, loc_state]

def update_subs_with_scraped_locations(notify=False, smart_stop=True, ck_app_session=None):
	"""
	Uses the API to get subs without location fields, then scrapes the page to fill that data in
	Variables: Notify - slack notifications on success and errors
	"""
	print('Gathering locationless ids...')
	ids = get_locless_subs(smart_stop=smart_stop)
	print(f'Found {len(ids)} subs without locations in fields')
	
	if not ck_app_session: 
		print('Creating a session with Convertkit...')
		ck_app_session = get_ck_session()
	
	print('Looping through the subs...')
	for i, sub_id in enumerate(ids): # Loop through all ids without location
		try:
			loc_city, loc_state = scrape_loc_from_app(sub_id=sub_id, ck_app_session=ck_app_session)
			
			# Update the subscriber through the API
			update_ck_sub_with_loc(sub_id, loc_city=loc_city, loc_state=loc_state)

			# Status print
			print(f'Updated {i} subscriber with loc {loc_city}, {loc_state}', end='\r')
		except Exception as e:
			print(f'error at {i}, on {sub_id}') # Error notification
			if notify: notify(f'error at {i}, on {sub_id}') # Error notification

		time.sleep(1) # Delay to prevent breaking the site

	print('Success!')
	if notify: notify('Success!') # Slack notification at the end

def get_lat_and_lng(loc_city:str=None, loc_state:str=None, geolocator=None):
	"""
	Uses the geolocator to determine the lat and lng

	Returns
	=======
	{"lat": loc_lat (float), "lng": loc_lng (float)}
	returns a dictionary of "lat" and "lng"
	"""
	if not geolocator: raise ReferenceError('No geolocator provided, try geolocator = Nominatim(user_agent="dgn_app")')

	loc_city = loc_city if loc_city and loc_city != "null" else ""
	loc_state = loc_state if loc_state and loc_state != "null" else ""
	loc_and = ", " if loc_city and loc_state else ""
	query=f'{loc_city + loc_and + loc_state}'; query
	
	if not loc_city and not loc_state: return {'lat': None, 'lng': None} 
	if loc_city == 'Aryanah' and loc_state == 'Gouvernorat de l\'Ariana': loc_city, loc_state = ['Ariana','Tunisia']

	location = geolocator.geocode(query); location

	# Catch lack of results
	if not location and loc_state: location = geolocator.geocode(loc_state) # Try just the state
	if not location and loc_city: location = geolocator.geocode(loc_city) # Try just the city
	if not location: raise LookupError(f'Nothing found for city:{loc_city} and state:{loc_state}')

	# Results with location
	return {'lat': float(str(round(location.latitude,3))), 'lng': float(str(round(location.longitude,3)))}

def get_sub_data_for_locless(last_sub=None, test=False):
	"""
	Get the subs that need to be updated in RDS (lat and lng exist in CK but not in RDS)
	TODO: This should be replaced with get_api_data or whatever
	"""

	subs=[]; page=1; total_pages=2
	url = f'https://api.convertkit.com/v3/subscribers'

	while page <= total_pages:
		print(f'Loading page {page}, len(subs) is {len(subs)}', end='\r')
		
		req_data = json.dumps({'api_secret': CK_API_SECRET, 'page': page, 'sort_order': 'desc'}); req_data
		resp = requests.get(url=url, data=req_data, headers={'content-type': 'application/json'}); resp.status_code
		json_resp = json.loads(resp.text); json_resp
		
		new_subs = [[s['id'], s['fields']['loc_city'], s['fields']['loc_state']] for s in json_resp['subscribers']]
		
		if last_sub and last_sub in [s['id'] for s in json_resp['subscribers']]: break
		else: subs += new_subs
		
		total_pages = json_resp['total_pages'] if not test else 1;

		page += 1
	return subs

def get_locless_subs(test=False, smart_stop=True):
	"""
	Get the subs that need to be updated in RDS (lat and lng exist in CK but not in RDS)
	TODO: This should be replaced with get_api_data or whatever
	"""
	locless_subs=[]
	page=1; total_pages=2
	url = f'https://api.convertkit.com/v3/subscribers'
	no_loc_data_in_fields = lambda x: not x['loc_city'] or (x['loc_city']=='null' and x['loc_state']=='null')

	while page <= total_pages:
		print(f'Loading page {page}/{total_pages}, len(locless_subs) is {len(locless_subs)}', end='\r')
		
		req_data = json.dumps({'api_secret': CK_API_SECRET, 'page': page, 'sort_order': 'desc'}); req_data
		resp = requests.get(url=url, data=req_data, headers={'content-type': 'application/json'}); resp.status_code
		json_resp = json.loads(resp.text); json_resp
		subs = json_resp['subscribers']
		
		new_locless_subs = [s['id'] for s in subs if no_loc_data_in_fields(s['fields'])]
		
		if smart_stop and len(new_locless_subs) == 0: break
		else: locless_subs += new_locless_subs
		
		total_pages = json_resp['total_pages'] if not test else 1;

		page += 1
	return locless_subs

def manually_add_location(sub_id=None, city=None, state=None, rds_only=True, gc=None, conn=None):
	if not sub_id: raise Exception('Must provide a subscriber_id')
	if not rds_only: raise Exception('Have not implemented CK update yet')
	if not city or not state: raise Exception('Must provide city and state')
	if not gc: gc = Nominatim(user_agent='dgn_locator')
	if not conn: raise Exception('Must provide an SQL connection')
		
	# Take a city and state and add that and lat, lng to the person
	latlng = get_lat_and_lng(loc_city='San Francisco', loc_state="California", geolocator=gc)
	lat=latlng['lat']; lng=latlng['lng']
	
	# Update RDS
	with conn.cursor() as cursor:
		sql_stmt = 'UPDATE subscriber_data SET loc_city=%s, loc_state=%s, loc_lat=%s, loc_lng=%s WHERE id=%s'
		sql_data = [city, state, lat, lng, sub_id]
		cursor.execute(sql_stmt, sql_data)
	print('Updated', sql_data, 'DID NOT COMMIT')

# }}}

# EMAIL FUNCTIONS {{{

def mysql_email_list(sub_id, conn):
	"""
	Get the email dictionary for this sub from MYSQL instead of via scraping
	The dictionary will have the key "subscriber_id" which scraping does not include
	"""
	tz = pytz.timezone("US/Eastern")
	cursor = conn.cursor()
	
	cursor.execute(f'SELECT * FROM email_data WHERE subscriber_id={sub_id}'); 
	raw = cursor.fetchall()
	email_dict = [{k:v if type(v) != datetime else v.astimezone(tz) for k,v in c.items()} for c in raw]
	return email_dict

def extract_emails_from_soup(soup):
	"""
	Gets the email data from soup (beautiful_soup)
	"""
	tz = pytz.timezone('US/Eastern')
	bs4_email_table = soup.find('table', {'class': 'emails'}); bs4_email_table
	if not bs4_email_table: raise ConnectionError('Could not find email table. Is session logged in?')
	
	email_results = []
	
	# If the email table is empty return the empty array
	if not bs4_email_table or not bs4_email_table.tbody or not bs4_email_table.tbody.findAll('tr'):
		return email_results
	
	for i, bs4_email in enumerate(bs4_email_table.tbody.findAll('tr')):
		# delivered?, opened?, clicked?, deliver datetime, open datetime, click datetime, email id, email name
		email_result = {
			'delivered': False,
			'opened': False, 
			'clicked': False, 
			'bounced': False,
			'failed': False,
			'deliver_date': None, 
			'open_date': None, 
			'click_date': None, 
			'email_id': None, 
			'email_name': None
		}
		bs4_action_span = bs4_email.find('span', {'class': 'email-status'})
		bs4_delivery_span = bs4_email.find('span', {'class': 'date-pointer'}) # could use regex (^\w*-pointer)
		if not bs4_delivery_span: bs4_delivery_span = bs4_email.find('span', {'class': 'cursor-pointer'})

		if 'delivered' in bs4_action_span['class']: email_result['delivered']=True
		if 'opened' in bs4_action_span['class']: email_result['opened']=True
		if 'clicked' in bs4_action_span['class']: email_result['clicked']=True
		if 'failed' in bs4_action_span['class']: email_result['failed']=True
		if 'bounced' in bs4_action_span['class']: email_result['bounced']=True
			 
		#pdb.set_trace()
		act_title = bs4_action_span['title']
		del_title = bs4_delivery_span['title']

		if not email_result['bounced'] and email_result['clicked']: 
			email_result['click_date'] = datetime.strptime(act_title, 'Clicked %b %d, %Y at %I:%M%p %Z').astimezone(tz)
		elif not email_result['bounced'] and email_result['opened']: 
			email_result['open_date'] = datetime.strptime(act_title, 'Opened %b %d, %Y at %I:%M%p %Z').astimezone(tz)

		email_result['deliver_date'] = datetime.strptime(del_title, '%b %d, %Y at %I:%M%p %Z').astimezone(tz)
		email_result['email_id'] = re.match('[\D]*(\d+)', bs4_email.div.a['href']).group(1)
		email_result['email_name'] = bs4_email.div.a.text

		email_results.append(email_result.copy())

	return email_results

def scrape_email_list(sub_id, ck_app_session=None):
	"""
	Scrapes emails from a subscriber
	An alternative option is mysql_email_list()
	
	Returns
	=======
	List of dictionaries. Each is an email object (sub_id, *email_info)
	"""

	# Creating a session with Convertkit app (not API)
	if not ck_app_session: ck_app_session = get_ck_session()

	# Scrape the location data
	url = f'https://app.convertkit.com/subscribers/{sub_id}'
	sub_result = ck_app_session.get(url, headers = dict(referer = url))
	if sub_result.status_code!=200: raise ConnectionError

	return extract_emails_from_soup(bs4.BeautifulSoup(sub_result.text, 'html.parser'))

async def get_async_ck_session(connections):
	"""Handle the initial page load and session creation
	Returns an authenticated session"""

	session_requests = asks.Session(persist_cookies=True, connections=connections)
	login_url = "https://app.convertkit.com/users/login"

	result = await session_requests.get(login_url)
	if result.status_code != 200: raise ConnectionError(result.status_code)

	login_tree = bs4.BeautifulSoup(result.text, 'html.parser'); login_tree

	payload = {
		"user[email]": os.environ['CK_USER'],
		"user[password]": os.environ['CK_PASSWORD'],
		"authenticity_token": login_tree.find("input", {"name":"authenticity_token"})['value']
	};

	login_result = await session_requests.post(login_url, data = payload, headers = dict(referer=login_url));
	if login_result.status_code != 200: raise ConnectionError(login_result.status_code)
	return session_requests

async def scrape_email_list_executor_async(subscriber_ids, async_session):
	results=[]
	async with trio.open_nursery() as nursery:
		for sub_id in subscriber_ids:
			nursery.start_soon(scrape_email_list_async, sub_id, async_session, results)
	return results

async def scrape_email_list_async(sub_id, ck_app_session=None, results=[]):
	"""
	Scrapes emails from a subscriber
	An alternative option is mysql_email_list()
	
	Returns
	=======
	List of dictionaries. Each is an email object (sub_id, *email_info)
	"""

	# Creating a session with Convertkit app (not API)
	if not ck_app_session: ck_app_session = get_ck_session()

	# Scrape the location data
	url = f'https://app.convertkit.com/subscribers/{sub_id}'
	sub_result = await ck_app_session.get(url, headers = dict(referer = url))
	try:
		if sub_result.status_code!=200: raise ConnectionError(sub_result, sub_id)
		emails = extract_emails_from_soup(bs4.BeautifulSoup(sub_result.text, 'html.parser')) 
	except ConnectionError as c:
		emails = []
		print(f'Error on sub_id {sub_id}')
	results += [{**e, 'subscriber_id': sub_id} for e in emails]

def save_email_data(email_data:list, conn):
	"""
	Saves email data into RDS based on the conn parameter. Autocommits
	
	Parameters
	==========
	email_data (list): list of dictionaries of email_data. Should come straight from scrape_email_list
	conn: mysql connection for commit and cursor
	
	Return
	======
	Number of rows created in RDS database
	
	"""
	cursor = conn.cursor()
	columns = [
		'subscriber_id', 
		'email_id', 
		'email_name', 
		'delivered', 
		'opened', 
		'clicked', 
		'bounced',
		'failed',
		'deliver_date', 
		'open_date', 
		'click_date'
	]
	
	subscribers = set(e['subscriber_id'] for e in email_data)
	values = [[e[c] for c in columns] for e in email_data]
	logging.debug(f'Produced {len(values)} rows of email_data for {len(subscribers)} subscribers')
	logging.debug(f'Example: {next(iter(values), None)}')

	col_str = ", ".join(["`"+col+"`" for col in columns]); col_str
	var_str = ", ".join(["%s"]*len(columns)); var_str
	dup_str = "ON DUPLICATE KEY UPDATE "+", ".join(["`"+col+"`=VALUES(`"+col+"`)" for col in columns]); dup_str

	# # Prep statement
	sql_str = f'INSERT INTO email_data ({col_str}) VALUES ({var_str}) {dup_str};'; sql_str
	logging.debug(f'sql_str: {sql_str}')

	# # Add those subscribers to the database
	result = cursor.executemany(sql_str, values)
	logging.debug(f'Inserted {result} pieces of information for email_data')

	# Same for fresh table
	fresh_columns = [
		'subscriber_id', 
		'last_checked'
	]
	f_col_str = ", ".join(["`"+col+"`" for col in fresh_columns]); f_col_str
	f_var_str = ", ".join(["%s"]*len(fresh_columns)); f_var_str
	f_dup_str = "ON DUPLICATE KEY UPDATE "+", ".join(["`"+col+"`=VALUES(`"+col+"`)" for col in fresh_columns]); 
	fresh_sql_str = f'INSERT INTO email_data_freshness ({f_col_str}) VALUES ({f_var_str}) {f_dup_str};'
	logging.debug(f'fresh_sql_str: {fresh_sql_str}')
	fresh_values = [[s, datetime.now().astimezone(pytz.timezone('US/Eastern'))] for s in subscribers]
	temp_result = cursor.executemany(fresh_sql_str, fresh_values)
	logging.debug(f'Inserted {temp_result} pieces of information for email_data_freshness')
	
	conn.commit()
	return result

def scrape_all_email_data(conn, skip_to=None, exclude_freshness:int=None, only_active=True, connections=20, step_by=100):
	"""
	Scrapes email data from all active subscribers (in RDS with id > 20000)
	
	Parameters
	==========
	skip_to(int): If the process was running and failed or stopped, this will restart at that enumerated index (not sub_id)
	exclude_freshness(int): remove subscribers that have freshness (i.e. had emails updated) in the last x days
	"""
	cursor = conn.cursor()
	if skip_to: raise Exception('Not currently implemented')

	# TODO: Need to get recent unsubscribers
	sql_stmt = f"""\
		SELECT id FROM subscriber_data s
		LEFT JOIN email_data_freshness e
			ON s.id = e.subscriber_id 
		WHERE s.id > 20000 
		AND s.state != "deleted" 
		"""
	if only_active: sql_stmt += """ AND s.state = "active" """
	
	# If provided an exclude_freshness variable, remove subscribers that don't fit inside
	if exclude_freshness and type(exclude_freshness)==int:
		now = datetime.strftime(datetime.now(), "%Y-%m-%d %T")
		sql_stmt += f"""\
			AND (
				e.last_checked < DATE_ADD("{now}", INTERVAL - {exclude_freshness} DAY)
				OR e.last_checked IS NULL
			)
			"""
	else:
		logging.debug(f'Not included exclude_freshness addendum since exclude_freshness = {exclude_freshness}')
		
	# cursor.execute(sql_stmt); subs_temp = cursor.fetchall(); len(subs_temp)
	subscribers = [c['id'] for c in cursor.e(sql_stmt)];
	# step_by = 100 # Set by instance variable
	async_session = trio.run(get_async_ck_session, connections) # Make a session with 20 connections

	for i in range(len(subscribers)//step_by + 1): # Go through all subscribers 500 (or step_by) at a time
		print(f'loading for subscribers {i*step_by} to {(i+1)*step_by} out of {len(subscribers)} ', end='\r')
		try:
			email_data = trio.run(scrape_email_list_executor_async, subscribers[ i*step_by : (i+1)*step_by ], async_session)	# Get the email data for these subscribers
			updated_rows = save_email_data(email_data, conn)
		except KeyboardInterrupt as k:
			break
		except ConnectionError as c:
			print(f'Possible subscriber deletion on {i}, {subscriber["id"]}') 
		except Exception as e:
			raise e
			print(f'Failed on subscriber {i}, {subscriber["id"]}')

def get_subscribe_date_from_email_click(sub_id, conn, force_date=False):
	"""
	Parameters
	==========
	sub_id (int): the subscriber id to get the date for
	force_date (boolean): If there is no click, then setting this true will set the subscribe date equal to the created_at date
	"""
	cursor = conn.cursor()
	
	sql_stmt = f"""\
	SELECT click_date 
	FROM email_data 
	WHERE subscriber_id = {sub_id} and clicked = 1 
	ORDER BY click_date ASC
	"""
	
	cursor.execute(sql_stmt)
	click = cursor.fetchall(); click
	if click:
		click_date = click[0]['click_date']
		return datetime.strftime(click_date, '%Y-%m-%d')
	else: 
		if force_date:
			sql_stmt = f"""\
			SELECT STR_TO_DATE(created_at, "%Y-%m-%dT%T.%fZ") as d
			FROM subscriber_data 
			WHERE id = {sub_id}
			"""
			cursor.execute(sql_stmt); 
			return datetime.strftime(cursor.fetchall()[0]['d'], "%Y-%m-%d")
		else: 
			raise Exception(f'No click_date for {sub_id}')

# }}}

# UNSUBSCRIBERS {{{ 

def manual_ck_unsub_update(sub_id, cursor, ck_app_session=None, session=None):
	"""
	Performs the unsub process if the webhook fails
	Returns
	=======
	unsub_date_str (str): the string of the date that the person unsubscribed
	"""
	if not ck_app_session: ck_app_session = get_ck_session()
	if not session: session = requests.session()

	unsub_date_str = scrape_unsub_datetime(sub_id).strftime('%Y-%m-%d')
	ck_output = update_ck_sub_with_misc(sub_id, session=session, unsubscribe_date=unsub_date_str)
	rds_output = cursor.e('UPDATE subscriber_data SET unsubscribe_date=%s, state=%s WHERE id=%s', values=[unsub_date_str, "cancelled", sub_id])

	logging.info(f'Subscriber {sub_id} updated in CK with unsub_date {ck_output["unsubscribe_date"]} and RDS with {rds_output} rows for {unsub_date_str}')
	return unsub_date_str

def scrape_unsub_datetime(sub_id, ck_app_session=None):
	"""
	Scrapes the datetime of an unsubscription from a subscriber
	Returns
	=======
	A datetime of the unsubscription
	"""

	tz = pytz.timezone('US/Eastern')

	# Creating a session with Convertkit app (not API)
	if not ck_app_session: ck_app_session = get_ck_session()

	# Scrape the location data
	url = f'https://app.convertkit.com/subscribers/{sub_id}'
	sub_result = ck_app_session.get(url, headers = dict(referer = url))
	if sub_result.status_code!=200:
		raise ConnectionError

	sub_tree = bs4.BeautifulSoup(sub_result.text, 'html.parser'); sub_tree

	bs4_email_table = sub_tree.find('table', {'class': 'emails'}); bs4_email_table
	if not bs4_email_table: raise ConnectionError('Could not find email table. Is session logged in?')

	unsub_elem = bs4_email_table.find(lambda tag: tag.name == 'em' and tag.findParent('td'))
	if not unsub_elem: raise ConnectionError('Could not find unsub element. Did this person actually unsubscribe?')

	unsub_datetime = datetime.strptime(unsub_elem.text, 'Unsubscribed on %b %d, %Y').astimezone(tz)

	return unsub_datetime

# }}}

# SEQUENCES {{{

def get_sequence_end_date(seq_dict, email_dict, create_date=None, sub_id=None, show=False, seq_id=None, ignore_warnings=False):
	"""
		Get the end date of a sequence based on a dictionary
		Base the date on the create_date or the last sent sequence email, from email_dict
		 
		
		Parameters
		==========
		seq_dict: the dictionary of the sequence that includes the email templates
		email_dict: the dictionary of the emails that the subscriber has received
		create_date: the date that the subscriber subscribed. Should come from the seq subscription api
		sub_id: the subscriber, used for debugging
		show: provides the information on the actual dates of the sequence emails
		seq_id: used for debugging
		ignore_warnings: Whether or not to display warnings through the logger
	"""

	# Sub function for get_sequence_end_dates
	def day_bools(day_dict):
		"""Returns an array of T/F based on which days the emails can be sent on
		param day_dict: the dictionary of days from the sequence email template
		"""
		return_dict = [True, True, True, True, True, True, True]
		days = ['mon', 'tue', 'wed', 'thr', 'fri', 'sat', 'sun']
		for i, day in enumerate(days):
			return_dict[i] = day_dict[day]
		return return_dict

	# Sub function for get_sequence_end_dates
	def get_single_email_delay(email_dict, start_date, seq_dict, current_email=0):
		"""Get the delay for a single email based on the start date"""
		end_date = start_date

		#Increment for the offset
		offset = {email_dict['offset_units']: email_dict['send_offset']}
		end_date += timedelta(**offset) # e.g. days=1

		if email_dict['offset_units'] == 'days':
			end_date = end_date.replace(hour=seq_dict['send_time'], minute=0, second=0)

		#Increment for nonallowable day
		allowable_days = [c[0]&c[1] for c in zip(day_bools(seq_dict), day_bools(email_dict))]
		while not allowable_days[end_date.weekday()]: 
			end_date += timedelta(days=1)
			if start_date + timedelta(days=1000) < end_date: break # avoid infinite loop

		return end_date

	# Sub function for get_sequence_end_dates
	def get_recent_received_email_pos_from_seq(seq_dict, email_dict):
		"""
		Gets the position and date of the most recent email that someone received of a sequence
		seq_dict: is the information about the sequence straight from the seq_dict function
		email_dict: is the information about the subscriber's emails straight from the scrape_email function
		returns the position or -1 if no emails received
		"""
		rec_email_dict = {int(ed['email_id']): ed['deliver_date'] for ed in email_dict}; rec_email_dict
		et = seq_dict['course']['email_templates']; et
		max_pos = max([et_i['position'] for et_i in et if et_i['id'] in rec_email_dict.keys()] + [-1]) #add -1 if empty
		delivery_date = [rec_email_dict[et_i['id']] for et_i in et if et_i['position'] == max_pos]
		if delivery_date: delivery_date = delivery_date[0]
		return max_pos, delivery_date

	# et is the list of emails from this sequence's template
	et = seq_dict['course']['email_templates']

	# Get the last delivered email from this sequence and the date of its delivery
	last_email_pos, last_delivery_date = get_recent_received_email_pos_from_seq(seq_dict, email_dict)
	
	# Pick a date to start calculating the end of the sequence based on email history
	if last_email_pos >= 0: # If there are any that were delivered (-1 means there were no emails)
		current_date = last_delivery_date
		logging.info(f'Start (Email {last_email_pos}): {str(current_date)}')
	else: # Otherwise, the subscription date given is used (could have been yesterday)
		if type(create_date) == str:
			current_date = datetime.strptime(create_date, '%Y-%m-%dT%H:%M:%S.%f%z').astimezone(pytz.timezone('US/Eastern'))
		elif type(create_date) == datetime:
			current_date = create_date
		elif not create_date:
			if not ignore_warnings: logging.warn(f'create date was null for {sub_id}')
			current_date = datetime.now(tz=pytz.timezone('US/Eastern'))
		
		logging.info(f'Start (Given time): {str(current_date)}')
	
	# Add the delay for each remaining email
	for step, i in enumerate(range(last_email_pos + 1, len(et))):
		this_et = [e for e in et if e['position']==i][0]
		if this_et['state'] != 'active': continue
		current_date = get_single_email_delay(this_et, current_date, seq_dict['course'])
		logging.debug(f'email {i}: {str(current_date)}')
		
		# If this is the first step, and the projected email date is before today, then there was an issue sending
		if step == 0 and current_date < datetime.now(pytz.timezone('US/Eastern')):
			if not ignore_warnings: logging.warn(f'{sub_id} should have received email {i} of sequence {seq_dict["course"]["name"]} by {datetime.strftime(current_date, "%Y-%m-%d")}, but hadnt as of now');
		
	logging.info('End: '+ str(current_date))
	return current_date

def get_seq_dict(seq_id, session = None):
	if not session: session = requests.session()
	headers={'content-type': 'application/json'}
	data = {'api_secret': CK_API_SECRET}; data
	url = f'https://api.convertkit.com/v3/sequences/{seq_id}'
	with session.get(url, data=json.dumps(data), headers=headers) as resp:
		if resp.status_code != 200:
			print("FAILURE::{0}".format(url))
		return json.loads(resp.text)
	
def get_all_seq_sub_data(seq_id, test=False, session=None, **kwargs):
	"""Use CK API to get all subscriber data for a sequence
	Returns a list of subscriber data lists """

	return_data=[]
	page=1; total_pages=2
	url = f'https://api.convertkit.com/v3/sequences/{seq_id}/subscriptions'
	
	if not session: session=session()

	while page <= total_pages:
		print(f'Gathering Seq Sub data. Loading page {page}, len(return_data) is {len(return_data)}', end='\r')

		req_data = json.dumps({'api_secret': CK_API_SECRET, 'page': page, **kwargs}); req_data
		resp = session.get(url=url, data=req_data, headers={'content-type': 'application/json'}); resp.status_code
		json_resp = json.loads(resp.text); json_resp

		return_data += [[s['subscriber']['id'], s['created_at']] for s in json_resp['subscriptions']]
		total_pages = json_resp['total_pages'] if not test else 1;

		page += 1
	return return_data

# Function to get the upcoming launch dates
def get_upcoming_launch_completions(conn=None, scrape_emails=False, test=False):	
	"""
	Get the subscribers who arent in the sequence, but do have the tag. Assuming that means they finished
	By default this will use email data from mysql
	
	Parameters
	==========
	conn: Mysql connection for cursor and commits
	scrape_emails (boolean): Whether to use Mysql emails or scrape. Mysql may be outdated but is faster
	test (boolean): Whether or not to do an abreviated test
	"""
	nurturing_seq_id = 383238
	launch_seq_id = 375580
	email_dict = {} # To store the dictionary of email results for each subscriber
	results = {} # To store the dictionary of end dates for each subscriber
	
	if scrape_emails: 
		email_fn = scrape_email_list
		session = get_ck_session()
		email_fn_vars = {"ck_app_session": session}
		
	else:
		email_fn = mysql_email_list
		session = requests.session() #Dont need ck session since using mysql
		email_fn_vars = {"conn": conn}
		

	# Go through the nurturing subs first
	nurturing_subs = get_all_seq_sub_data(nurturing_seq_id, session=session, test=test) 
	nurturing_seq_dict = get_seq_dict(nurturing_seq_id, session=session) 
	for i, [sub_id, create_date] in enumerate(nurturing_subs):
		print(f'Getting nurturing finish dates. On sub {sub_id}, #{i}, len(subs) is {len(nurturing_subs)}', end='\r')
		email_dict[sub_id] = email_fn(sub_id, **email_fn_vars)
		#Assumes nurturing sub date is their subscription date
		end_date = get_sequence_end_date(nurturing_seq_dict, email_dict[sub_id], create_date=create_date, sub_id=sub_id) 
		results[sub_id] = end_date
	print()

	# Go through the launch and add the nurturing people as well
	launch_subs = get_all_seq_sub_data(launch_seq_id, session=session, test=test) 
	launch_seq_dict = get_seq_dict(launch_seq_id, session=session)

	# Add the nurturing people (their create date will be their nurture end date +6)
	results = {k: v + timedelta(days=6) for k,v in results.items()} # Add 6 days for automation interval
	launch_subs += [[k, v] for k, v in results.items()] 

	for i, [sub_id, create_date] in enumerate(launch_subs):
		print(f'Getting launch finish dates. On sub {sub_id}, #{i}, len(subs) is {len(launch_subs)}', end='\r')
		if sub_id not in email_dict:
			email_dict[sub_id] = email_fn(sub_id, **email_fn_vars)
		end_date = get_sequence_end_date(launch_seq_dict, email_dict[sub_id], create_date=create_date, sub_id=sub_id)
		results[sub_id] = end_date
	return results

def get_past_launch_completions(conn = None, scrape_emails=False, include_unsubscribers=True, test=False):
	"""
	Get the subscribers who arent in the sequence, but do have the tag. Assuming that means they finished
	By default this will use email data from mysql
	
	Parameters
	==========
	conn: Mysql connection for cursor and commits
	scrape_emails (boolean): Whether to use Mysql emails or scrape. Mysql may be outdated but is faster
	test (boolean): Whether or not to do an abreviated test
	"""
	
	if scrape_emails: 
		email_fn = scrape_email_list
		session = get_ck_session()
		email_fn_vars = {"ck_app_session": session}
	else:
		email_fn = mysql_email_list
		session = requests.session() #Dont need ck session since using mysql
		email_fn_vars = {"conn": conn}
	
	LAUNCH_TAG = 375580

	# Get subs of the tag for the launch
	logging.info('Getting tag subscribers')
	tag_subs = get_api_data(CK_Api.TAG_SUBS, 899142, include_unsubscribers=True, test=test); tag_subs
	tag_subs_ids = [i['subscriber']['id'] for i in tag_subs]; tag_subs_ids

	# remove the subs in the sequence
	logging.info('Getting seq subscribers')
	seq_subs = get_api_data(CK_Api.SEQ_SUBS, 375580, include_unsubscribers=True, test=test); seq_subs
	seq_subs_ids = [i['subscriber']['id'] for i in seq_subs]; seq_subs_ids

	## TODO REMOVE THE SEQ PEOPLE
	tag_subs_ids = [t for t in tag_subs_ids if t not in seq_subs_ids]

	seq_dict = get_seq_dict(LAUNCH_TAG, session=session)

	# see when the sequence will end for each of them
	end_date = {}
	for i, sub_id in enumerate(tag_subs_ids):
		print(f'Getting {i}/{len(tag_subs_ids)}. Subscriber #{sub_id}			',end='\r')
		email_dict = email_fn(sub_id, **email_fn_vars)
		end_date[sub_id] = get_sequence_end_date(seq_dict, email_dict, create_date=None, sub_id=sub_id, ignore_warnings=True)

	return end_date

# }}}

# INACTIVE FUNCTIONS {{{

def extract_totalpages_from_soup(soup):
	total_pages = int(soup.find(name='ul', attrs={'class': 'pagination'}).find_all('li')[-2].text)
	return total_pages
		
def extract_subscribers_from_soup(soup, status_type):
	"""
	Parameters
	==========
	soup (bs4 soup): the BeautifulSoup(sub_result.text, 'html.parser')
	status_type (str): The status type to differentiate and to help get the tr items from the table 
	
	Returns
	=======
	subscribers (dict): The dict of subscribers {id: {id, first_name, email, created_at_datetime}}
	"""
	subscribers = {}
	sub_table = soup.find(name='table', attrs={'class': 'subscribers'})
	subs = sub_table.tbody.find_all(name='tr', attrs={'class': status_type})
	tz = lambda x: datetime.strptime(x, '%b %d, %Y at %I:%M%p %Z').astimezone(pytz.timezone('US/Eastern'))
	
	for sub in subs:
		sub_id = int(sub.input['data-subscriber-id'])
		subscribers[sub_id] = {
			'id': sub_id, 
			'first_name': sub.find(name='span', attrs={'class': 'first-name'}).text.strip(), 
			'email': sub.find(name='span', attrs={'class': 'first-name'}).next_sibling.next_sibling.text.strip(),
			'created_at_datetime': tz(sub.find(name='span', attrs={'class': 'cursor-pointer'})['uib-tooltip'].strip())
		}
	return subscribers

def scrape_all_non_active_subscribers(ck_app_session=None, only_new=False, limit_to_past_days=None, cursor=None, test=False):
	"""
	Scrapes the main page to get all inactive subscribers
	Use "update_rds_with_non_actives" to store non_active subscribers

	Parameters
	==========
	only_new (boolean): tells the scraper to stop once it hits an inactive subscriber that it already knows of. Requires conn. Only works on the "inactive"s
	limit_to_past_days (int): tells the scraper to stop once it has checked the last limit_to_past_days days
	cursor: MYSQL cursor for use of "only_new" to know when a repeat is hit
	"""

	# Get existing inactive subs to match if only_new is True
	if only_new and not cursor: 
		raise Exception('Must provide mysql connection if only_new=True')
	elif only_new:
		existing_ids = [c['id'] for c in cursor.e('SELECT id FROM subscriber_data WHERE state = "inactive"')]

	if limit_to_past_days: cutoff = datetime.now().astimezone(pytz.timezone('US/Eastern')) - timedelta(days=limit_to_past_days)

	# Creating a session with Convertkit app (not API)
	if not ck_app_session: ck_app_session = get_ck_session()

	all_subs = {}
	for status_type in ['inactive', 'complained', 'bounced']: # ignoring 'cancelled' and 'cold'
		page=1; total_pages=1
		while (page <= total_pages) and (not test or page==1):
			url = f'https://app.convertkit.com/?page={page}&status={status_type}'
			print(f'Loading page {page}/{total_pages} for {status_type}', end='\t\t\t\r') #Extra spaces to ensure no overwrite
			try:
				with ck_app_session.get(url, headers={'referer': url}) as sub_result:

					if sub_result.status_code!=200: raise ConnectionError

					soup = bs4.BeautifulSoup(sub_result.text, 'html.parser')
					total_pages = extract_totalpages_from_soup(soup)
					new_subs = extract_subscribers_from_soup(soup, status_type)
					new_subs = {k:{**v, 'state':status_type} for k,v in new_subs.items()}
					all_subs.update(new_subs)

					# return with current data if this sub is already known if only_new is True (Only inactive)
					if only_new and status_type=='inactive' and [n for n in new_subs if n in existing_ids]: 
							page = sys.maxsize # to end while loop

					# Return with current data if this sub is outside the limit_to_past_days
					oldest = min([a['created_at_datetime'] for a in new_subs.values()])
					if type(limit_to_past_days)==int and oldest < cutoff: 
							page = sys.maxsize

				page += 1; time.sleep(1)
			except Exception as e:
				print(f'Received error on page {page}. Waiting 5 minutes, then continuing on the same page')
				print(f'Status code was {sub_result.status_code}')
				print(e)
				time.sleep(300)

	return all_subs

# Optional testing
#status_type = 'bounced' # complained
#ck_app_session = get_ck_session()
#sub_result = ck_app_session.get(f'https://app.convertkit.com/?page=1&status={status_type}'); sub_result
#soup = bs4.BeautifulSoup(sub_result.text, 'html.parser'); soup
#extract_subscribers_from_soup(soup, status_type)

def update_rds_with_non_actives(cursor, non_actives=None, ck_app_session=None, only_new=False, limit_to_past_days=None):
	"""
	Scrapes (via "scrape_all_non_active_subscribers") the main page to get all non-active subscribers, then stores them
	in RDS (complained, bounced, inactive)
	Does not autocommit

	Parameters
	==========
	non_actives (dict): The non-actives from scrape_all_non_active_subscribers if using it directly
	only_new (boolean): tells the scraper to stop once it hits an inactive subscriber that it already knows of. Requires conn. Only works on inactive
	limit_to_past_days (int): tells the scraper to stop once it has checked the last limit_to_past_days days
	cursor: MYSQL cursor for use of "only_new" to know when a repeat is hit
	
	Returns
	=======
	updated_rows (int): the number of rows updated in mysql 
	"""
	if not non_actives:
		non_actives = scrape_all_non_active_subscribers(
			ck_app_session=ck_app_session,
			only_new=only_new, 
			limit_to_past_days=limit_to_past_days,
			cursor=cursor
		) 

	values = [
		[
			k,
			v['state'],
			v['first_name'],
			v['email'],
			datetime.strftime(v['created_at_datetime'], '%Y-%m-%dT%H:%M:%S.000Z')
		] 
	for k,v in non_actives.items()]; len(values), values[:2]

	columns = ['id', 'state', 'first_name', 'email_address', 'created_at']

	column_string = ", ".join(["`"+col+"`" for col in columns]); column_string
	values_string = ", ".join(["%s"]*len(columns)); values_string
	duplicate_string = " ON DUPLICATE KEY UPDATE " + ", ".join(["`"+col+"`=VALUES(`"+col+"`)" for col in columns]); duplicate_string

	sql_stmt = f"""INSERT INTO subscriber_data ({column_string}) VALUES ({values_string}) {duplicate_string}"""; sql_stmt

	updated_rows = cursor.executemany(sql_stmt, values)
	return updated_rows

# }}}

# LEGACY FUNCTIONS {{{

def update_unsub_with_day(unsub_id: int, day: str):
	raise Exception('depreciated, use update_ck_sub_with_misc')

def update_sub_with_loc(sub_id:int, loc_city:str, loc_state:str): 
	raise Exception('depreciated, use update_ck_sub_with_misc')

def get_single_subscriber(session=None, email_address:str = None, subscriber_id:int = None, cancelled=False):
	"""Returns flat list of subscriber details (i.e. replaces fields field with the data)"""
	
	if subscriber_id: return get_api_data(CK_Api.SUB, subscriber_id, include_unsubscribers=cancelled)
	elif email_address: return get_api_data(CK_Api.SUB_EMAIL, email_address, include_unsubscribers=cancelled)
	else: raise Exception('Must provide subscriber id or email address')
	
def prep_ck_csv(csv_path):
	"""
	Take a directory and return a prepared dataframe for use
	"""
	ck_csv = pd.read_csv(csv_path)
	ck_csv = ck_csv.where((pd.notnull(ck_csv)), None); ck_csv.columns
	ck_csv.columns = [c.lower().replace(' ', '_') if c!='status' else 'state' for c in ck_csv.columns]
	return ck_csv

def get_all_sub_data(test=False, session=None, **kwargs): 
	logging.warn('Use of deprecated get_all_sub_data. Use get_api_data(CK_Api.ALL_SUBS, None, session=session, test=test, **kwargs) instead')
	return get_api_data(CK_Api.ALL_SUBS, None, session=session, test=test, **kwargs)

# }}}

# vim:foldmethod=marker:foldlevel=0
