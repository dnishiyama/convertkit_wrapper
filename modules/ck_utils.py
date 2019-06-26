import requests, time, json, bs4, os, nbslack, pandas as pd, pymysql, re, pytz, logging
from enum import Enum, auto
from geopy.geocoders import Nominatim
from urllib import request, parse
from datetime import timedelta, date, datetime
CK_API_SECRET = os.environ['CK_API_SECRET']
CK_API_KEY = os.environ['CK_API_KEY']
API_KEY = os.environ['CK_API_KEY']
PLACES_KEY = os.environ['PLACES_API_KEY']

sub_loc = 137632665
sub_no_loc = 467247086

# Slack notification code
slack_webhook='https://hooks.slack.com/services/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'
nbslack.notifying('dnishiyama',slack_webhook,error_handle=False)
def notify(text='Work'): nbslack.notify(f"{text}")
    
def scrape_loc_from_app(sub_id, ck_app_session=None):
    """Scrapes the page to find loc of city and state"""
    
    # Creating a session with Convertkit app (not API)
    if not ck_app_session: ck_app_session = get_ck_session()
    
    # Scrape the location data
    url = f'https://app.convertkit.com/subscribers/{sub_id}'
    sub_result = ck_app_session.get(url, headers = dict(referer = url))
    if sub_result.status_code!=200:
        raise ConnectionError
        
    sub_tree = bs4.BeautifulSoup(sub_result.text, 'html.parser'); sub_tree
    div = sub_tree.find(has_city_and_state); div
    loc_city = div['data-city'] if div['data-city'] != 'null' else None
    loc_state = div['data-state'] if div['data-state'] != 'null' else None
    return [loc_city, loc_state]

def update_subs_with_scraped_locations(notify=False, smart_stop=True):
    """Uses the API to get subs without location fields, then scrapes the page to fill that data in
    Variables: Notify - slack notifications on success and errors"""
    print('Gathering locationless ids...')
    ids = [435385229]#get_locless_subs(smart_stop=smart_stop)
    print(f'Found {len(ids)} subs without locations in fields')
    
    print('Creating a session with Convertkit...')
    ck_app_session = get_ck_session()
    
    print('Looping through the subs...')

    for i, sub_id in enumerate(ids): # Loop through all ids without location
        try:
            loc_city, loc_state = scrape_loc_from_app(sub_id=sub_id, ck_app_session=ck_app_session)
            
            # Update the subscriber through the API
            update_sub_with_loc(sub_id, loc_city=loc_city, loc_state=loc_state)

            # Status print
            print(f'Updated {i} subscriber with loc {loc_city}, {loc_state}', end='\r')
        except Exception as e:
            print(f'error at {i}, on {sub_id}') # Error notification
            if notify: notify(f'error at {i}, on {sub_id}') # Error notification

        time.sleep(1) # Delay to prevent breaking the site

    print('Success!')
    if notify: notify('Success!') # Slack notification at the end

def has_city_and_state(tag): return tag.has_attr('data-city') and tag.has_attr('data-state')

def get_lat_and_lng(loc_city:str="null", loc_state:str="null", geolocator=None):
    if not geolocator: raise ReferenceError('No geolocator provided, try geolocator = Nominatim(user_agent="my_app")')

    loc_city = loc_city if loc_city and loc_city != "null" else ""
    loc_state = loc_state if loc_state and loc_state != "null" else ""
#     print(loc_city); print(loc_state)
    loc_and = ", " if loc_city and loc_state else ""
    query=f'{loc_city+loc_and+loc_state}'; query
    
    if not loc_city and not loc_state: return {'lat': None, 'lng': None}
    
    if loc_city == 'Aryanah' and loc_state == 'Gouvernorat de l\'Ariana': loc_city, loc_state = ['Ariana','Tunisia']

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
    FB_AUDIENCE_SOURCE = auto()
    LEAD_OPT0IN = auto()
    LEAD_SOURCE = auto()
    LOC_CITY = auto()
    LOC_STATE = auto()
    MAILCHIMP_SUB_DATE = auto()
    STSBC_EVG_LAUNCH_DEADLINE = auto()
    SUBSCRIBE_DATE = auto()
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

def update_sub_with_misc(sub_id:int, session=None, **fields):
    if not session: session = requests.session()
    url = f'https://api.convertkit.com/v3/subscribers/{sub_id}'
    data = json.dumps({'api_secret': CK_API_SECRET, 'fields':fields})
    headers = {'content-type': 'application/json'}
    r = session.put(url=url, data=data, headers=headers)
    if r.status_code != 200: raise Exception
    return r.text

# Single Subscriber
# subscriber_id = 458592022
def get_single_subscriber(session=None, email_address:str = None, subscriber_id:int = None, cancelled=False):
    """Returns flat list of subscriber details (i.e. replaces fields field with the data)"""
    if not session: session = requests.session()
        
    headers={'content-type': 'application/json'}
    data = {'api_secret': CK_API_SECRET}; data
    
    if cancelled: data['sort_field']='cancelled_at'
    
    if subscriber_id:
        url = f'https://api.convertkit.com/v3/subscribers/{subscriber_id}'
    elif email_address:
        url = f'https://api.convertkit.com/v3/subscribers'
        data['email_address'] = email_address
    else:
        raise Exception('Must provide subscriber id or email address')
        
    with session.get(url, data=json.dumps(data), headers=headers) as resp:
        if resp.status_code != 200:
            print("FAILURE::{0}".format(url))
        json_resp = json.loads(resp.text); json_resp
        if subscriber_id:
            if 'subscriber' not in json_resp or not json_resp['subscriber']: raise Exception('no result')
            json_resp.update(json_resp['subscriber']); del json_resp['subscriber']
        elif email_address:
            if 'subscribers' not in json_resp or not json_resp['subscribers']: raise Exception('no result')
            json_resp = json_resp['subscribers'][0]
      
        json_resp.update(json_resp['fields']); del json_resp['fields']
        return json_resp
    
    raise Exception()
    
def conv_created_at_to_subscribe_date(created_at:str):
    return datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%S.000Z').strftime('%Y-%m-%d')
    
def update_subscriber_in_rds(sub_id, conn):
    """Updates subscriber in CK and RDS"""
    created_at_conv = lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.000Z').strftime('%Y-%m-%d')
    mailchimp_conv = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
    
    sub_data = get_single_subscriber(session=None, subscriber_id=sub_id) # Get the data from CK
    
     # Prep columns and values
    columns = list(sub_data.keys()); columns.remove('last_name')
    values = [sub_data[c] for c in columns]
    column_string = ", ".join(["`"+col+"`" for col in columns]); column_string
    variable_string = ", ".join(["%s"]*len(columns)); variable_string
    duplicate_string = f'ON DUPLICATE KEY UPDATE {", ".join(["`"+c+"`=VALUES(`"+c+"`)" for c in columns])}'; 
    duplicate_string #update for existing

    # Prep statement
    sql_string = f'insert into subscriber_data ({column_string}) values ({variable_string}) {duplicate_string};'; sql_string

    # Add those subscribers to the database
    cursor = conn.cursor()
    cursor.execute(sql_string, values)
    cursor.execute(f'SELECT * FROM subscriber_data WHERE id={sub_id}'); 
    results = cursor.fetchall()
    print('Updated RDS with', results)
    conn.commit()
    return results

def update_single_subscriber(sub_id, conn):
    """Updates subscriber in CK and RDS"""
    created_at_conv = lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.000Z').strftime('%Y-%m-%d')
    mailchimp_conv = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
    
    sub_data = get_single_subscriber(session=None, subscriber_id=sub_id) # Get the data from CK
    loc_city, loc_state = scrape_loc_from_app(sub_id)
    geolocator=Nominatim(user_agent='dgn')
    latlng = get_lat_and_lng(loc_city=loc_city, loc_state=loc_state, geolocator=geolocator)
    
    if sub_data['state'] in ['active', 'cancelled']:
        if 'mailchimp_sub_date' in sub_data and sub_data['mailchimp_sub_date']:
            subscribe_date = mailchimp_conv(sub_data['mailchimp_sub_date'])
        else:
            subscribe_date = created_at_conv(sub_data['created_at'])
    else:
        raise Exception('non-active and non-cancelled states not designed for yet')

    # Update local sub_data variable
    sub_data['loc_city'] = loc_city
    sub_data['loc_state'] = loc_state
    sub_data['loc_lat'] = latlng['lat']
    sub_data['loc_lng'] = latlng['lng']
    sub_data['subscribe_date'] = subscribe_date

    fields = {}
    fields['loc_city'] = loc_city if loc_city else 'null'
    fields['loc_state'] = loc_state if loc_state else 'null'
    fields['loc_lat'] = latlng['lat'] if latlng['lat'] else 'null'
    fields['loc_lng'] = latlng['lng'] if latlng['lng'] else 'null'
    fields['subscribe_date'] = subscribe_date

    update_sub_with_misc(sub_id, **fields) # Make the update on the CK page
    print(f'Updated {sub_id} with {fields}')

     # Prep columns and values
    columns = list(sub_data.keys()); columns.remove('last_name')
    values = [sub_data[c] for c in columns]
    column_string = ", ".join(["`"+col+"`" for col in columns]); column_string
    variable_string = ", ".join(["%s"]*len(columns)); variable_string
    duplicate_string = f'ON DUPLICATE KEY UPDATE {", ".join(["`"+c+"`=VALUES(`"+c+"`)" for c in columns])}'; 
    duplicate_string #update for existing

    # Prep statement
    sql_string = f'insert into subscriber_data ({column_string}) values ({variable_string}) {duplicate_string};'; sql_string

    # Add those subscribers to the database
    cursor = conn.cursor()
    cursor.execute(sql_string, values)
    cursor.execute(f'SELECT * FROM subscriber_data WHERE id={sub_id}'); 
    results = cursor.fetchall()
    print('Updated RDS with', results)
    conn.commit()
    return results
    
def prep_ck_csv(csv_path):
    """
    Take a directory and return a prepared dataframe for use
    """
    ck_csv = pd.read_csv(csv_path)
    ck_csv = ck_csv.where((pd.notnull(ck_csv)), None); ck_csv.columns
    ck_csv.columns = [c.lower().replace(' ', '_') if c!='status' else 'state' for c in ck_csv.columns]
    return ck_csv

def convert_time(time:str):
    """
    Convert a string from mailchimp (probably excel) into the format stored in ConvertKit
    """
    return_time = datetime.strptime(time, '%m/%d/%y %H:%M').strftime('%Y-%m-%dT%H:%M:%S.000Z')
    return return_time

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

    def day_bools(day_dict):
        """Returns an array of T/F based on which days the emails can be sent on
        param day_dict: the dictionary of days from the sequence email template
        """
        return_dict = [True, True, True, True, True, True, True]
        days = ['mon', 'tue', 'wed', 'thr', 'fri', 'sat', 'sun']
        for i, day in enumerate(days):
            return_dict[i] = day_dict[day]
        return return_dict

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
            if not ignore_warnings: logging.warn(f'{sub_id} should have received email {i} by {datetime.strftime(current_date, "%Y-%m-%d")}, but hadnt as of now');
        
    logging.info('End: '+ str(current_date))
    return current_date
    
    
def mysql_email_list(sub_id):
    """
    Get the email dictionary for this sub from MYSQL instead of via scraping
    The dictionary will have the key "subscriber_id" which scraping does not include
    """
    tz = pytz.timezone("US/Eastern")
    cursor.execute(f'SELECT * FROM email_data WHERE subscriber_id={sub_id}'); 
    raw = cursor.fetchall()
    email_dict = [{k:v if type(v) != datetime else v.astimezone(tz) for k,v in c.items()} for c in raw]
    return email_dict

def scrape_email_list(sub_id, ck_app_session=None):
    """
    Scrapes emails from a subscriber
    An alternative option is mysql_email_list()
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
    
    email_results = []
    
    # If the email table is empty return the empty array
    if not bs4_email_table or not bs4_email_table.tbody or not bs4_email_table.tbody.findAll('tr'):
        return email_results
    
    for i, bs4_email in enumerate(bs4_email_table.tbody.findAll('tr')):
        # delivered?, opened?, clicked?, deliver datetime, open datetime, click datetime, email id, email name
        email_result = {'delivered': False,
                        'opened': False, 
                        'clicked': False, 
                        'bounced': False,
                        'failed': False,
                        'deliver_date': None, 
                        'open_date': None, 
                        'click_date': None, 
                        'email_id': None, 
                        'email_name': None}
        bs4_action_span = bs4_email.find('span', {'class': 'email-status'})
        bs4_delivery_span = bs4_email.find('span', {'class': 'date-pointer'})

        if 'delivered' in bs4_action_span['class']: email_result['delivered']=True
        if 'opened' in bs4_action_span['class']: email_result['opened']=True
        if 'clicked' in bs4_action_span['class']: email_result['clicked']=True
        if 'failed' in bs4_action_span['class']: email_result['failed']=True
        if 'bounced' in bs4_action_span['class']: email_result['bounced']=True
            
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

def get_seq_dict(seq_id, ck_app_session = None):
    if not ck_app_session: ck_app_session = get_ck_session()
    headers={'content-type': 'application/json'}
    data = {'api_secret': CK_API_SECRET}; data
    url = f'https://api.convertkit.com/v3/sequences/{seq_id}'
    with ck_app_session.get(url, data=json.dumps(data), headers=headers) as resp:
        if resp.status_code != 200:
            print("FAILURE::{0}".format(url))
        return json.loads(resp.text)
    
def get_all_seq_sub_data(seq_id, test=False, ck_app_session=None, **kwargs):
    """Use CK API to get all subscriber data for a sequence
    Returns a list of subscriber data lists """

    return_data=[]
    page=1; total_pages=2
    url = f'https://api.convertkit.com/v3/sequences/{seq_id}/subscriptions'
    
    if not ck_app_session: ck_app_session=get_ck_session()

    while page <= total_pages:
        print(f'Gathering Seq Sub data. Loading page {page}, len(return_data) is {len(return_data)}', end='\r')

        req_data = json.dumps({'api_secret': CK_API_SECRET, 'page': page, **kwargs}); req_data #parse.quote_plus(
        resp = ck_app_session.get(url=url, data=req_data, headers={'content-type': 'application/json'}); resp.status_code
        json_resp = json.loads(resp.text); json_resp

        return_data += [[s['subscriber']['id'], s['created_at']] for s in json_resp['subscriptions']]
        total_pages = json_resp['total_pages'] if not test else 1;

        page += 1
    return return_data

# Function to get the upcoming launch dates
def get_upcoming_launch_completions():    
    nurturing_seq_id = 383238
    launch_seq_id = 375580
    email_dict = {} # To store the dictionary of email results for each subscriber
    results = {} # To store the dictionary of end dates for each subscriber
    session = get_ck_session()

    # Go through the nurturing subs first
    nurturing_subs = get_all_seq_sub_data(nurturing_seq_id, ck_app_session=session) 
    nurturing_seq_dict = get_seq_dict(nurturing_seq_id, ck_app_session=session) 
    for i, [sub_id, create_date] in enumerate(nurturing_subs):
        print(f'Getting nurturing finish dates. On sub {sub_id}, #{i}, len(subs) is {len(nurturing_subs)}', end='\r')
        email_dict[sub_id] = scrape_email_list(sub_id, ck_app_session=session)
        #Assumes nurturing sub date is their subscription date
        end_date = get_sequence_end_date(nurturing_seq_dict, email_dict[sub_id], create_date=create_date, sub_id=sub_id) 
        results[sub_id] = end_date
    print()

    # Go through the launch and add the nurturing people as well
    launch_subs = get_all_seq_sub_data(launch_seq_id, ck_app_session=session) 
    launch_seq_dict = get_seq_dict(launch_seq_id, ck_app_session=session)

    # Add the nurturing people (their create date will be their nurture end date +6)
    results = {k: v + timedelta(days=6) for k,v in results.items()} # Add 6 days for automation interval
    launch_subs += [[k, v] for k, v in results.items()] 

    for i, [sub_id, create_date] in enumerate(launch_subs):
        print(f'Getting launch finish dates. On sub {sub_id}, #{i}, len(subs) is {len(launch_subs)}', end='\r')
        if sub_id not in email_dict:
            email_dict[sub_id] = scrape_email_list(sub_id, ck_app_session=session)
        end_date = get_sequence_end_date(launch_seq_dict, email_dict[sub_id], create_date=create_date, sub_id=sub_id)
        results[sub_id] = end_date
    return results

def get_api_data(url, include_unsubscribers=False, test=False, session=None):
    """
    Currently only accepts the following APIs
        Tag Subscriptions: e.g. https://api.convertkit.com/v3/tags/899142/subscriptions
        Sequence Subscriptions: e.g. https://api.convertkit.com/v3/sequences/375580/subscriptions
    Currently only works with GET commands

    Parameters
    ----------
    url (string): CK API string that will be used for the API function 
    include_unsubscribers (boolen): Determines whether unsubscribers should be included
    test (boolean): provides only the first page for multi-page responses
    session (session obj): allows all requests to be passed through a session (use get_ck_session)
    
    
    """  

    return_data = []; page=1; total_pages=1
    if not session: session = requests.session()
    headers={'content-type': 'application/json'}
    data = {'api_secret': CK_API_SECRET, 'api_key': CK_API_KEY}; data
    if not include_unsubscribers: data['subscriber_state'] = 'active'

    while page <= total_pages:
        print(f'Loading page {page}', end='\r')
        with session.get(url, data=json.dumps(data), headers=headers) as resp:
            if resp.status_code != 200:
                print("FAILURE::{0}".format(url))
            json_resp = json.loads(resp.text); json_resp
            total_pages = json_resp['total_pages'] if not test else 1;

            # Remove the keys that don't need to be in the data
            data_keys = [i for i in json_resp.keys() if not re.match('^total', i) and not i in ['page']]
            if len(data_keys) != 1: raise Exception('Too many data_keys in json_resp')
            return_data += json_resp[data_keys[0]]

        page += 1
    return return_data