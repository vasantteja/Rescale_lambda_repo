from __future__ import print_function

import json
import urllib
import boto3
import pymysql


print('Loading function')

s3 = boto3.client('s3')

rds_host  = "rescalespotpricesaurora.c2rogujylj0b.us-east-1.rds.amazonaws.com"
name = 'rescale'
password = 'Rescaleaws1'
db_name = 'pricedb'


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        #return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e


    try:
        conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
    except:
        print("ERROR: Unexpected error: Could not connect to MySql instance.")
        sys.exit()

    print("SUCCESS: Connection to RDS mysql instance succeeded")

    s3_path = 's3://' + bucket + '/' + key
    with conn.cursor() as cur:
        sql = "LOAD DATA FROM S3 %s INTO TABLE pricedb.spot_prices FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (Product_Description, Instance_Type, Availability_zone, Spot_Price, Time_Stamp);"
        cur.execute(sql,(s3_path))
        conn.commit()

    with conn.cursor() as cur:
        sql = 'select count(*) from spot_prices'
        cur.execute(sql)
        res = cur.fetchone()
        conn.commit()

    return res