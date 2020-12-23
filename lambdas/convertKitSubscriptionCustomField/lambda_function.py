import json, os, logging, requests

# When the location is scraped, this lambda will be triggered
# need to location, and id from the payload
# Need to get all subscriber information from the site (fields will have been updated)
# Need to store all that data in the RDS data

# import json, os, logging, requests, datetime
logger = logging.getLogger()
logger.setLevel(logging.INFO)

### Handler ###
def lambda_handler(event, context):
    logger.info("## EVENT INFO ##")
    logger.info(event)
    
    try:
        loc_city = event.get('loc_city', {})
        loc_state = event.get('loc_state', {})
        logger.info(f"Location: {loc_city}, {loc_state}")
        
        #  
        
        unsub_date = store_data_(unsub_id)
        
        responseBody = {"Success": f'Updated subscriber {unsub_id} with unsub date {unsub_date}'}
        responseCode = 200
    
    except ValueError:
        print("Error while decoding event!")
        responseBody = {
            "Error": "Error while decoding event!",
            "Error_desc": "Bad payload",
        }
        responseCode = 400
    
    except Exception as e:
        raise e
        print("Exception!", e)
        responseBody = {
            "Error": "Exception",
        }
        responseCode = 400

    # Proxy Lambda must be in this format
    returnBody = {
        "statusCode": responseCode,
        "body": json.dumps(responseBody)
    }
    
    return returnBody;
    
    
### Functions ###
def update_unsub_with_day(unsub_id):
    day = datetime.datetime.now().strftime("%Y-%m-%d")
    url = f'https://api.convertkit.com/v3/subscribers/{unsub_id}'
    data = json.dumps({'api_secret': os.environ['CK_API_SECRET'],'fields':{'unsubscribe_date': day }})
    headers = {'content-type': 'application/json'}
    resp = requests.put(url=url, data=data, headers=headers)
    
    if resp.status_code != 200: raise ConnectionError()
    
    return day
    
def get_single_subscriber(subscriber_id:int = None):
    if None: raise LookupError('Must provide subscriber id')
    url = f'https://api.convertkit.com/v3/subscribers/{subscriber_id}'
    data = json.dumps({'api_secret': os.environ['CK_API_SECRET']}); data #parse.quote_plus(
    resp = requests.get(url=url, data=data, headers={'content-type': 'application/json'}); resp.status_code
    json_resp = json.loads(resp.text); json_resp
    return json_resp
