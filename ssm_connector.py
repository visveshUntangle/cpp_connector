import boto3
import os 

def download_pem_key():
    try:
        # check if file exists
        # if os.path.exists("snowflake_conn.p8"):
        #     print("exists")
        #     return True
        
        
        # download from aws parameter store
        ssm = boto3.client('ssm')
        response = ssm.get_parameter(
            Name='snowflake_pem',
            WithDecryption=True
        )
        pem_key = response['Parameter']['Value']
        # save as snowflake_conn.p8 file
        with open("snowflake_conn.p8", "wb") as f:
            f.write(pem_key)
        return True
    except Exception as e:
        print(e)
        return False
    


download_pem_key()