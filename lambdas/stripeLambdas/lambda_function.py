import json, stripe, os, logging, requests
logger = logging.getLogger()
logger.setLevel(logging.INFO)

fulfillment_products = [
        os.environ['PROD_STSBC_MP_FA'], #  Monthly plan Financial aid
        os.environ['PROD_STSBC_MP']    #  Monthly plan
        # os.environ['PROD_STSBC'],       #  Normal Course THIS SHOULD ALREADY BE FULFILLED
        # os.environ['PROD_TEST']         #  Test LIVE TESTING
    ]
    
# Function to iterate the payments count (from 12 -> 0, 1 for each time)
def fulfill_product(event):
    
    # Try to get email address from event object
    customer_email = event.data.object.customer_email
    
    # If not, get it from the Stripe servers
    if not customer_email:
        customer_id = event.data.object.customer
        customer_data = stripe.Customer.retrieve(customer_id)
        customer_email = customer_data.get("email", None)
        
        logger.debug(customer_data)
        
    # Add tag to user
    if not customer_email:
        return [400, {"Error":"Could not retrieve customer email"}]
        
    # Else, add tag to customer
    try:
        TAG_ID = os.environ["STSBC_TAG_ID"]
        CK_PUBLIC_KEY = os.environ['CK_PUBLIC_KEY']
        
        url = f'https://api.convertkit.com/v3/tags/{TAG_ID}/subscribe'; url
        data = json.dumps({'api_key': CK_PUBLIC_KEY, 'email': customer_email}); data #parse.quote_plus(
        resp = requests.post(url=url, data=data, headers={'content-type': 'application/json'})
        if resp.status_code != 200: 
            return [resp.status_code, {"Error": "Unable to add tag to customer"}]
        return [resp.status_code, {"Success": "Added tag to customer"}]
        
    except Exception as e:
        print("Exception!", e)
        return [400, {"Error": "Exception during fulfillment"}]
    
###
# Main function to handle the request
###
def lambda_handler(event, context):
    logger.debug("## EVENT INFO ##")
    logger.debug(event)
    
    stripe.api_key = os.environ['STRIPE_KEY']
    webhook_secret = os.environ['WEBHOOK_KEY']
    
    payload = event['body'] # The actual payment data
    received_sig = event['headers'].get("Stripe-Signature", None) # the stripe signature
    test_webhook_event = event.get("test_webhook_event", None) # Test webhook for lambda troubleshooting
    
    try:
        if test_webhook_event and test_webhook_event['test_key'] == os.environ['TEST_KEY']:
            webhook_event = stripe.Event.construct_from(test_webhook_event, stripe.api_key)
        else: # If it is live!
            webhook_event = stripe.Webhook.construct_event(
                payload, received_sig, webhook_secret
            )
            
            logger.debug("## WEBHOOK EVENT INFO ##")
            logger.debug(webhook_event)
        
        if webhook_event.type == "checkout.session.completed":
            # print("Checkout.")
            
            plan = webhook_event.data.object.display_items[0].get("plan", None)
            product = plan.get('product', None) if plan else None
            
            # Make sure that it is a fulfillment product
            if product in fulfillment_products:
                responseCode, responseBody = fulfill_product(webhook_event)

            else:
                responseBody = {"Defer": f"{product} is not a fulfillable product"}
                responseCode = 200
        else:
            # Shouldn't ever happen
            responseBody = {"Error": "webhook called for type other than checkout.session.completed"}
            responseCode = 400
            
    except ValueError:
        print("Error while decoding event!")
        responseCode = 400
        responseBody = {
            "Error": "Error while decoding event!",
            "Error_desc": "Bad payload",
        }
        
    except stripe.error.SignatureVerificationError as e:
        print("Invalid signature!", e)
        responseCode = 400
        responseBody = {
            "Error": "SignatureVerificationError",
            "Error_desc": "Invalid signature!",
        }
        
    except Exception as e:
        print("Exception!", e)
        responseCode = 400
        responseBody = {
            "Error": "Exception, possibly subscription doesn't exist",
        }

    # Proxy Lambda must be in this format
    returnBody = {
        "statusCode": responseCode,
        "body": json.dumps(responseBody)
    }
    
    logger.info(returnBody)
    
    return returnBody;

