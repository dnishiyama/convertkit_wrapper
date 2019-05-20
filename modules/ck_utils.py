import requests, time, json, bs4, os, nbslack, pandas as pd, pymysql
from enum import Enum, auto
from geopy.geocoders import Nominatim
from urllib import request, parse
from datetime import timedelta, date
CK_API_SECRET = os.environ['CK_API_SECRET']
API_KEY = os.environ['CK_API_KEY']
PLACES_KEY = os.environ['PLACES_API_KEY']

sub_loc = 137632665
sub_no_loc = 467247086

# Slack notification code
slack_webhook='https://hooks.slack.com/services/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'
nbslack.notifying('dnishiyama',slack_webhook,error_handle=False)
def notify(text='Work'): nbslack.notify(f"{text}")

def update_subs_with_scraped_locations(notify=False, smart_stop=True):
    """Uses the API to get subs without location fields, then scrapes the page to fill that data in
    Variables: Notify - slack notifications on success and errors"""
    print('Gathering locationless ids...')
    ids = get_locless_subs(smart_stop=smart_stop)
    print(f'Found {len(ids)} subs without locations in fields')
    
    print('Creating a session with Convertkit...')
    session_requests = get_ck_session()
    
    print('Looping through the subs...')
    try:
        for i, sub_id in enumerate(ids): # Loop through all ids without location

            # Scrape the location data
            url = f'https://app.convertkit.com/subscribers/{sub_id}'
            sub_result = session_requests.get(url, headers = dict(referer = url))
            if sub_result.status_code!=200: 
                print(f'Issue on {sub_id}, status_code={sub_result.status_code}')
                continue
            sub_tree = bs4.BeautifulSoup(sub_result.text, 'html.parser'); sub_tree
            div = sub_tree.find(has_city_and_state); div
            loc_city = div['data-city']; loc_state = div['data-state']

            # Update the subscriber through the API
            update_sub_with_loc(sub_id, loc_city=loc_city, loc_state=loc_state)

            # Status print
            print(f'Updated {i} subscriber with loc {loc_city}, {loc_state}', end='\r')

            time.sleep(1) # Delay to prevent breaking the site

        print('Success!')
        if notify: notify('Success!') # Slack notification at the end
    except Exception as e:
        print(f'error at {i}, on {sub_id}') # Error notification
        if notify: notify(f'error at {i}, on {sub_id}') # Error notification
        raise e
   
def has_city_and_state(tag): return tag.has_attr('data-city') and tag.has_attr('data-state')

def get_lat_and_lng(loc_city:str="null", loc_state:str="null", geolocator=None):
    if not geolocator: raise ReferenceError('No geolocator provided, try geolocator = Nominatim(user_agent="my_app")')
    # Manual corrections: err_city, err_state, cor_city, cor_state
#     cor_data = [
#                 ['Tokyo', 'Tokyo', 'Tokyo', 'Japan'],
#                 ['Hamilton', 'Hamilton city', 'Hamilton', 'Ohio'],
#                 ['Hanoi', 'Hanoi', 'Hanoi', 'Vietnam'],
#                 ['Algiers', 'Algiers', 'Algiers', 'Algeria'],
#                ];
#     cor = [[c[2], c[3]] for c in cor_data if loc_city==c[0] and loc_state==c[1]]
#     if cor:
#         loc_city=cor[0][0]
#         loc_state=cor[0][1]

    loc_city = loc_city if loc_city and loc_city != "null" else ""
    loc_state = loc_state if loc_state and loc_state != "null" else ""
    loc_and = ", " if loc_city and loc_state else ""
    query=f'{loc_city+loc_and+loc_state}'; query

    location = geolocator.geocode(query); location

    # Catch lack of results
    if not location and loc_state: location = geolocator.geocode(loc_state) # Try just the state
    if not location and loc_city: location = geolocator.geocode(loc_city) # Try just the city
    if not location: raise LookupError(f'Nothing found for city:{loc_city} and state:{loc_state}')

    # Results with location
    loc_lat = float(str(round(location.latitude,3)))
    loc_lng = float(str(round(location.longitude,3)))
#     print(loc_lat + ", " + loc_lng)
    return {'lat': loc_lat, 'lng': loc_lng}

# Get Location
def get_subscriber_location(subscriber_id:int):
#     subscriber_id = 458592022
    url = f'https://app.convertkit.com/subscribers/{subscriber_id}'
    resp = requests.get(url=url, headers={'content-type': 'application/json'}); resp.status_code
    json_data = json.loads(resp.text)
    return json_data

def get_sub_data_for_locless(last_sub=None, test=False):
    """Get the subs that need to be updated in the df (lat and lng exist in CK but not in df)"""
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
    locless_subs=[]
    page=1; total_pages=2
    url = f'https://api.convertkit.com/v3/subscribers'
    no_loc_data_in_fields = lambda x: not x['loc_city'] or (x['loc_city']=='null' and x['loc_state']=='null')

    while page <= total_pages:
        print(f'Loading page {page}/{total_pages}, len(locless_subs) is {len(locless_subs)}', end='\r')
        
        req_data = json.dumps({'api_secret': CK_API_SECRET, 'page': page, 'sort_order': 'desc'}); req_data #parse.quote_plus(
        resp = requests.get(url=url, data=req_data, headers={'content-type': 'application/json'}); resp.status_code
        json_resp = json.loads(resp.text); json_resp
        subs = json_resp['subscribers']
        
        new_locless_subs = [s['id'] for s in subs if no_loc_data_in_fields(s['fields'])]
        
        if smart_stop and len(new_locless_subs) == 0: break
        else: locless_subs += new_locless_subs
        
        total_pages = json_resp['total_pages'] if not test else 1;

        page += 1
    return locless_subs

class CK_Column(Enum):
    ID = auto()
    FIRST_NAME = auto()
    EMAIL_ADDRESS = auto()
    STATE = auto()
    CREATED_AT = auto()
    LAST_NAME = auto()
    FB_AUDIENCE_SOURCE = auto()
    LEAD_OPT0IN = auto()
    LEAD_SOURCE = auto()
    LOC_CITY = auto()
    LOC_STATE = auto()
    MAILCHIMP_SUB_DATE = auto()
    STSBC_EVG_LAUNCH_DEADLINE = auto()
    UNSUBSCRIBE_DATE = auto()

    @property
    def value(self): return self.name.lower().replace(' ', '_').replace('0', '-')

def get_all_sub_data(columns:list=[CK_Column.ID], test=False, dictionary=False, **kwargs):
    """Use CK API to get data for every subscriber
    Returns a list of subscriber data lists """

    columns = [column.value for column in columns] # Convert from Enum to value that CK recognizes
    if not columns: columns = [col.value for col in CK_Column] # If there were not columns, then get them all

    return_data=[]
    page=1; total_pages=2
    url = f'https://api.convertkit.com/v3/subscribers'

    while page <= total_pages:
        print(f'Loading page {page}, len(return_data) is {len(return_data)}', end='\r')

        req_data = json.dumps({'api_secret': CK_API_SECRET, 'page': page, **kwargs}); req_data #parse.quote_plus(
        resp = requests.get(url=url, data=req_data, headers={'content-type': 'application/json'}); resp.status_code
        json_resp = json.loads(resp.text); json_resp

        if not dictionary: # Return as an array of values
            return_data += [[s[c] if c in s else s['fields'][c] for c in columns] for s in json_resp['subscribers']]
        else: #Return as array of dictionaries
            return_data += [{c:s[c] if c in s else s['fields'][c] for c in columns} for s in json_resp['subscribers']]
        total_pages = json_resp['total_pages'] if not test else 1;

        page += 1
    return return_data

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

def get_new_unsub_for_day(day: str, exclude_updates=True):
    """ Return list of subscribers that were last updated on a particular day  
    Checks to see if the unsubscribe date is empty"""
    url = f'https://api.convertkit.com/v3/subscribers?api_secret={CK_API_SECRET}&sort_field=cancelled_at&page=1&updated_from={day}&updated_to={day}'
    req = request.Request(url); req
    req.add_header('Content-Type', 'application/json')
    resp = request.urlopen(req); resp
    data = resp.read().decode(); data
    subscribers = json.loads(data)['subscribers']; subscribers;
    
    if exclude_updates: subscribers = [s for s in subscribers if s['fields']['unsubscribe_date'] is None]
        
    return subscribers

def update_unsub_with_day(unsub_id: int, day: str):
    url = f'https://api.convertkit.com/v3/subscribers/{unsub_id}'
    data = json.dumps({'api_secret': CK_API_SECRET,'fields':{'unsubscribe_date': day }})
    headers = {'content-type': 'application/json'}
    r = requests.put(url=url, data=data, headers=headers)
    if r.status_code != 200: raise Exception
    return r.text

def update_sub_with_loc(sub_id:int, loc_city:str, loc_state:str):
    url = f'https://api.convertkit.com/v3/subscribers/{sub_id}'
    data = json.dumps({'api_secret': CK_API_SECRET,'fields':{'loc_city': loc_city, 'loc_state': loc_state}})
    headers = {'content-type': 'application/json'}
    r = requests.put(url=url, data=data, headers=headers)
    if r.status_code != 200: raise Exception
    return r.text

# Single Subscriber
# subscriber_id = 458592022
def get_single_subscriber(subscriber_id:int = None):
    if None: raise Exception('Must provide subscriber id')
    url = f'https://api.convertkit.com/v3/subscribers/{subscriber_id}'
    data = json.dumps({'api_secret': CK_API_SECRET}); data #parse.quote_plus(
    resp = requests.get(url=url, data=data, headers={'content-type': 'application/json'}); resp.status_code
    json_resp = json.loads(resp.text); json_resp
    return json_resp
