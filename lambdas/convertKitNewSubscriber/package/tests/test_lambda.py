from lambdas.lambda_function import *
import logging

def test_python_function():
    logging.getLogger().setLevel(logging.DEBUG);
    event = { "subscriber": { "id": 542004913 } }
    resp = lambda_handler(event, None)
    assert resp['statusCode'] == 200
    assert "Success" in resp['body']
    assert resp['loc_state'] == 'North Carolina' 
