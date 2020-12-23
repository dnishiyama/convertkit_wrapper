import json, os, logging, re, requests, urllib.parse as urlparse

# import json, os, logging, requests, datetime
logger = logging.getLogger()
logger.setLevel(logging.INFO)

session_requests = requests.session();

### Handler ###
def lambda_handler(event, context):
    global session_requests
    
    logger.info("## EVENT INFO ##")
    logger.info(event)

    try:
        # Get the parameters from the post
        urlparams = event.get('body', '')
        parsed_urlparams = urlparse.parse_qs(urlparams)
        
        # since urlparse.parse_qs returns an array
        get_param = lambda x: parsed_urlparams[x][0] if x in parsed_urlparams else None
        
        first_name = get_param('7ad557e1b4ecaa31cc945b87f6366cdb')
        email = get_param('a583bfe142f87e3c25b418f2bf60a515')
        source = get_param('7f19195a536f8bbc85951f5e19b8a546') # State from drop downs
        optin = get_param('5a7b17ea80f8af83b8e4e4482d592eb7') # City from drop downs
        lead_form = get_param('df2a292f42e2ff932fab665ee65547f8') # Form for exact detail
        
        # Clean up the source and optin
        source = re.sub(r'\?.*?$', '',  source)
        optin = re.sub(r'\?.*?$', '',  optin)
        if source: source = source.replace("+", " ")
        if optin: optin = optin.replace("+"," ")
        if not optin: optin = "emails"
        if not source: source = "unknown"
        
        # get the tags to add
        tags = get_tags(optin=optin, source=source) 
        
        # Could replace this with a search to make it more robust
        if optin and optin.lower() == 'book making guide': form_id = 272097
        elif optin and optin.lower() == 'emails': form_id = 949995
        elif optin and optin.lower() == 'influence map': form_id = 458062
        elif optin and optin.lower() == 'mcdt': form_id = 341476
        elif optin and optin.lower() == 'mcdt (single opt-in)': form_id = 462329
        elif optin and optin.lower() == 'style mini-course': form_id = 459136
        elif optin and optin.lower() == 'style mini-course (single opt-in)': form_id = 462513
        elif optin and optin.lower() == 'sketchbook mini-course': form_id = 295490
        elif optin and optin.lower() == 'stsbc wl': form_id = 330122
        elif optin and optin.lower() == 'studiomates wl': form_id = 387829
        elif optin and optin.lower() == 'unknown': form_id = 949995 # Emails
        elif optin and optin.lower() == 'waf activity guide': form_id = 274068
        elif optin and optin.lower() == 'waf ebook': form_id = 272096
        elif optin and optin.lower() == 'tools': form_id = 272065
        elif optin and optin.lower() == 'test': form_id = 459136
        else:
            logging.error('Optin: ' + str(optin) + ' does not exist')
            logging.info(f'first_name: {first_name}, email: {email}, source: {source}, optin: {optin}')
            raise AttributeError('optin: ' + str(optin) + ' does not have an associated form')
        
        # See if the subscriber already exists, and if they have optin and source already.
        try: 
            sub = get_single_subscriber(session=session_requests, email_address=email)
            # If so, then ignore those fields
            if 'lead_opt-in' in sub and sub['lead_opt-in']: optin = None
            if 'lead_source' in sub and sub['lead_source']: source = None
        except Exception as e:
            # No subscriber (or unable to id), so continue as normal
            print(f'No subscriber, {email}', e)
            pass
        
        #Source and optin here are for custom field data
        subscribe_to_form(
            form_id=form_id, 
            email=email, 
            first_name=first_name, 
            source=source, 
            optin=optin, 
            lead_form=lead_form, 
            tags=tags, 
            session=session_requests
        )

        responseBody = {"Success": f'Form submitted successfully', "event":event}
        responseCode = 200

    except ValueError as e:
        logging.error(f'ValueError {e}')
        raise e
        print("Error while decoding event!")
        responseBody = {
            "Error": "Error while decoding event!",
            "Error_desc": "Bad payload",
        }
        responseCode = 400

    except AttributeError as e:
        logging.error(f'AttributeError {e}')
        raise e
        responseCode = 406 # Not acceptable
        responseBody = {
            "Error": "This optin is not supported",
            "ErrorDetail": str(a)
        }
    
    except Exception as e:
        logging.error(f'Exception: {e}')
        raise e
        print("Exception!", e)
        responseBody = {
            "Error": "Exception",
        }
        responseCode = 400
        
    # Proxy Lambda must be in this format
    returnBody = {
        "headers":{'Access-Control-Allow-Origin': '*' }, #'Content-Type': 'application/json', 
        "statusCode": responseCode,
        "body": json.dumps(responseBody)
    }

    return returnBody;


### Functions ###
def subscribe_to_form(
        form_id:int, 
        email:str, 
        first_name:str, 
        source:str, 
        optin:str, 
        lead_form:str, 
        tags:list, 
        session=None
    ):
        
    if not session: session = requests.session()
    
    logging.info(f'subscribing- form:{form_id}, name:{first_name}, email:{email}, source:{source}, optin:{optin}')
    url = f'https://api.convertkit.com/v3/forms/{form_id}/subscribe'
    fields = {'lead_form': lead_form}
    if source: fields.update({'lead_source': source}) # Only include if they aren't None
    if optin: fields.update({'lead_opt-in': optin}) # Only include if they aren't None
    
    data = {
        'api_key': os.environ['CK_API_KEY'], 
        'email': email, 
        'fields':fields, 
        'tags': tags,
        'first_name': first_name
    }
    headers = {'content-type': 'application/json'}
    logging.info(f'post url:{url}, data:{json.dumps(data)}, headers:{json.dumps(headers)}')
    r = session.post(url=url, data=json.dumps(data), headers=headers)
    if r.status_code != 200: raise Exception(f'Status Code: {r.status_code}, text: {r.text}')
    return r.text
    
def get_single_subscriber(session=None, email_address:str = None, subscriber_id:int = None, cancelled=False):
    """Returns flat list of subscriber details (i.e. replaces fields field with the data)"""
    if not session: session = requests.session()
        
    headers={'content-type': 'application/json'}
    data = {'api_secret': os.environ['CK_API_SECRET']}; data
    
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
    
    raise Exception("Unable to get subscriber")
    
# TODO: Could get evergreen tags to make this more robust. Maybe a 1-time process
def get_tags(optin=None, source=None):
    """Provide optin and source and return the tags"""
    
    tags=[  
            {'id': 291886, 'name': 'Opt-in: Book Making Guide'}, 
            {'id': 969850, 'name': 'Opt-in: Emails'}, 
            {'id': 291286, 'name': 'Opt-in: Influence Map'},
            {'id': 399666, 'name': 'Opt-in: MCDT'}, 
            {'id':1066736, 'name': 'Opt-in: MCDT (Single Opt-in)'},
            {'id': 880901, 'name': 'Opt-in: Style Mini-course'},
            {'id':1054340, 'name': 'Opt-in: Style Mini-course (Single Opt-in)'},
            {'id': 989221, 'name': 'Opt-in: Sketchbook Mini-course'},
            {'id': 880904, 'name': 'Opt-in: Studiomates WL'},
            {'id': 881005, 'name': 'Opt-in: StSBC WL'},
            {'id': 291875, 'name': 'Opt-in: Tools'},
            {'id': 881851, 'name': 'Opt-in: Unknown'},
            {'id': 294908, 'name': 'Opt-in: WAF Activity Guide'},
            {'id': 291279, 'name': 'Opt-in: WAF ebook'},
            {'id': 294894, 'name': 'Source: MailChimp'},
            {'id': 323271, 'name': 'Source: WAF Book Reading'},
            {'id': 880718, 'name': 'Source: Medium'},
            {'id': 880719, 'name': 'Source: Skillshare'},
            {'id': 880724, 'name': 'Source: Instagram'},
            {'id': 1122867, 'name': 'Source: Google Ad'},
            {'id': 880728, 'name': 'Source: FB Ad'},
            {'id': 880916, 'name': 'Source: Facebook'},
            {'id': 880917, 'name': 'Source: MCS Page'},
            {'id': 880918, 'name': 'Source: Unknown'},
            {'id': 880919, 'name': 'Source: Twitter'},
            {'id': 928112, 'name': 'Source: Manual'}
        ]
    
    optin_tag = [t['id'] for t in tags if optin and 'Opt-in: ' in t['name'] and optin.lower() in t['name'].lower()]
    source_tag = [t['id'] for t in tags if source and 'Source: ' in t['name'] and source.lower() in t['name'].lower()]
    
    return optin_tag + source_tag # Should be a list of ids
