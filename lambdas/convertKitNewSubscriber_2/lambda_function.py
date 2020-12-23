import json, os, logging, bs4, requests
from geopy.geocoders import Nominatim
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Global counters to see if there is excessive session creation
new_session_counter = 0;
context_reuse_counter = 0;

# global connection session to maintain 
session_requests = requests.session();

### Handler ###
def lambda_handler(event, context):
    
    # Get global variables from context
    global new_session_counter, context_reuse_counter
    
    logger.debug("## EVENT INFO ##")
    logger.debug(event)
    
    context_reuse_counter += + 1
    logger.info(f'context_reuse_counter {context_reuse_counter}')
    logger.info(f'new_session_counter {new_session_counter}')
    
    # TODO: DEV remove this
    # loc_city = 'null'; loc_state = 'null';
    # return
    
    try:
        # Grab the id, do a scraping for the location, then get the coord and country code, add that to the subscriber
        
        sub_id = event.get('subscriber', {}).get('id', {})
        
        logger.debug(f'subscriber_id is {sub_id}')
        
        # Scrape the location data
        loc_city, loc_state = scrape_city_and_state(sub_id)# session_requests is global
        if loc_city=='null' and loc_state=='null': raise SyntaxError('No city and state')
        
        geolocator = Nominatim(user_agent="com.mightcouldstudios.dgn_ck_geolocator")
        lat_lng = get_lat_and_lng(loc_city=loc_city, loc_state=loc_state, geolocator=geolocator)
        
        # Update the subscriber through the API
        update_sub_with_loc(
            sub_id, 
            loc_city=loc_city, 
            loc_state=loc_state, 
            loc_lat=lat_lng['lat'],
            loc_lng=lat_lng['lng']
        )
        
        # Status print
        logger.info(f'Updated subscriber {sub_id} with loc {loc_city}, {loc_state}')

        # TODO: Do the pop-up task for the world map webpage
    
        responseBody = {"Success": f'Updated subscriber {sub_id} with city:{loc_city} and state:{loc_state}'}
        responseCode = 200
    
    except ValueError:
        print("Error while decoding event!")
        raise e
    
    except ConnectionError as e:
        print("Error with connection, possibly the session!")
        raise e
        
    except SyntaxError as e:
        print("City and State dont exist yet")
        raise e
        
    except Exception as e:
        print("Exception!", str(e))
        raise e


    # Proxy Lambda must be in this format
    # Will be sent regardless of exception above
    returnBody = {
        "statusCode": responseCode,
        "body": json.dumps(responseBody),
        "loc_city": loc_city,
        "loc_state": loc_state,
        "loc_lat": lat_lng['lat'],
        "loc_lng": lat_lng['lng']
    }
    event.update(returnBody) # Update with sent event information
    
    return event;
    
    
### Functions ###

# Helper for bs4 to scrape the right data
def has_city_and_state(tag): return tag.has_attr('data-city') and tag.has_attr('data-state')

# Update the subscriber with the city and state from scraping
def update_sub_with_loc(sub_id:int, loc_city:str, loc_state:str, loc_lat:str, loc_lng:str):
    url = f'https://api.convertkit.com/v3/subscribers/{sub_id}'
    fields = {'loc_city': loc_city, 'loc_state': loc_state, 'loc_lat': loc_lat, 'loc_lng': loc_lng}
    data = {'api_secret': os.environ['CK_API_SECRET'], 'fields':fields }
    headers = {'content-type': 'application/json'}
    r = requests.put(url=url, data=json.dumps(data), headers=headers)
    if r.status_code != 200: raise Exception
    return r.text

def scrape_city_and_state(sub_id:int):
    global session_requests, new_session_counter; # Get globals for session management
    
    url = f'https://app.convertkit.com/subscribers/{sub_id}'
    sub_result = session_requests.get(url, headers = dict(referer = url))
    
    if sub_result.status_code!=200: raise ConnectionError({"statusCode": sub_result.status_code, "body": "session_error"})

    sub_tree = bs4.BeautifulSoup(sub_result.text, 'html.parser'); sub_tree
    
    if sub_tree.find('h2'): # if there is an h2 (should only be for login page)
        # Increment new session counter to show that there was a new one
        new_session_counter += 1
        logger.info(f'new_session_counter: {new_session_counter}')
        
        session_requests = get_ck_session()
        sub_result = session_requests.get(url, headers = dict(referer = url))
        
        if sub_result.status_code!=200: raise ConnectionError({"statusCode": sub_result.status_code, "body": "session_error"})
        
        sub_tree = bs4.BeautifulSoup(sub_result.text, 'html.parser'); sub_tree
    
    # Back to normal process
    div = sub_tree.find(has_city_and_state); div
    loc_city = div['data-city']; loc_state = div['data-state']
    return loc_city, loc_state
    
def get_lat_and_lng(loc_city:str="null", loc_state:str="null", geolocator=None):
    if not geolocator: raise ReferenceError('No geolocator provided, try geolocator = Nominatim(user_agent="my_app")')

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

def get_ck_session():
    """Handle the initial page load and session creation
    Returns an authenticated session"""
    
    session = requests.session()
    login_url = "https://app.convertkit.com/users/login"

    result = session.get(login_url)
    login_tree = bs4.BeautifulSoup(result.text, 'html.parser'); login_tree
    if not result.ok: raise ConnectionError(result.status_code)

    payload = {
        "user[email]": os.environ['CK_USER'], 
        "user[password]": os.environ['CK_PASSWORD'],
        "authenticity_token": login_tree.find("input", {"name":"authenticity_token"})['value']
    };

    login_result = session.post(login_url, data = payload, headers = dict(referer=login_url)); 
    if not login_result.ok: raise ConnectionError(login_result.status_code)
    return session

