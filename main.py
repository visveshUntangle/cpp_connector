from flask import Flask, render_template, request
# import key_config as keys
# import boto3 
import pandas as pd
from flask_cors import CORS
import json
import snowflake.connector

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import pandas as pd
from smart_open import open

app = Flask(__name__)
CORS(app)


from boto3.dynamodb.conditions import Key, Attr

@app.route('/')
def index():
    return "Hello"

def connect():
    print("opening key...")
 
    with open('C:\\Users\\Mind-Graph\\OneDrive - Mindgraph Solutions Sdn Bhd\\Desktop\\cpp_connector\\cpp_sfdev_rsa_key.p8', "rb") as key:

        p_key= serialization.load_pem_private_key(
        key.read(),
        password='cCpD3vp@ss$F'.encode(),
        backend=default_backend()
    )


    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

        
    print("connecting to Snowflake:")
    ctx = snowflake.connector.connect(
        user='SF_DB_DEV_ALL_CPP',
        account='pe68509.ap-southeast-1.privatelink',
        private_key=pkb,
        warehouse='PRJ_VWH_UUP_CPP',
        database='DEV_DB_UUP',
        schema='CPP_CORE',
        role='SA_FR_SF_DB_DEV_ALL_CPP'
    )
    cs = ctx.cursor()

    return cs



@app.route('/check',methods = ['post'])
def check():
    if request.method=='POST':
        record = json.loads(request.data)
        audienceColumns = record['audienceColumns']
        audienceExpression = record['audienceExpression']
        metricsAttributes = record['metricsAttributes']
        segmentsHierarchy = record['segmentsHierarchy']

        # xwe = metricsAttributes.split(",")
        print(metricsAttributes)
        str3=""

        for i in metricsAttributes:
            # print(i)
            str3 += i
            str3 += "," 
            

        # print(str3)

        # print(audienceColumns , audienceExpression, metricsAttributes, segmentsHierarchy)
        str1 = "select " + str3 +" from DEV_DB_UUP.CPP_CORE.subscriber_profile where "
        str2 = str1 + audienceExpression[1:-1]

        print(str2)
        cs = connect()
        cs.execute(str2)
        one_row = cs.fetchone()
        print(cs)
        print(one_row)
        rows = 0
        while True:
            dat = cs.fetchmany(50000)
            if not dat:
                break
            df = pd.DataFrame(dat, columns=cs.description)
            rows += df.shape[0]
        print(df.head())






if __name__ == "__main__":
    
    app.run(debug=True)