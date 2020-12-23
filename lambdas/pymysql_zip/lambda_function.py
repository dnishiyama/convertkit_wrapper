import json, os, logging, pymysql
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get RDS connection
def get_connection():
	return pymysql.connect(
		user=os.environ['RDS_CK_USER'], 
		password=os.environ['RDS_CK_PASSWORD'], 
		host=os.environ['RDS_CK_HOST'],
		database=os.environ['RDS_CK_DATABASE'],
		read_timeout=2, # 2 second timeout
		write_timeout=2, # 2 second timeout
		cursorclass=pymysql.cursors.DictCursor,
		autocommit=True
	)

conn = get_connection()

### Handler ###
def lambda_handler(event, context):
	logger.info("## EVENT INFO ##")
	logger.info(event)
	
	try:
		link_id = event.get('link_id', '');
		result = delete_from_rds(link_id);
	
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
	
	return result;
	
	
### Functions ###
def delete_from_rds(link_id):
	global conn
	
	if not conn.open: conn = get_connection()
	
	with conn.cursor() as cursor:
		sql_string = f'SELET * FROM {os.environ["RDS_CK_TABLE"]}'; 
		cursor.execute(sql_string);
		return cursor.fetchall();
