from prefect import flow ,task
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import pandas as pd
from smart_open import open




@task 
def connect_sf():
    print("opening key...")
    with open('C:\\Users\\Mind-Graph\\OneDrive - Mindgraph Solutions Sdn Bhd\\Desktop\\Globe\\cpp_connector\\cpp_sfdev_rsa_key.p8', "rb") as key:

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

    
    try:
        sql2 ="select affluence, count(affluence) from DEV_DB_UUP.CPP_CORE.SUBSCRIBER_PROFILE group by affluence"
        sql1 = "select lifestage, age_bracket_name count(age_bracket_name) as age_bracket_name_19_count, count(globeone_user_indicator) as globeone_user_indicator_count, affluence, count(affluence) as affluence_count from DEV_DB_UUP.CPP_CORE.SUBSCRIBER_PROFILE where lifestage = '5: RETIREE' and age_bracket_name = '19 and below' group by lifestage, age_bracket_name, affluence"
        sql = "select lifestage ,affluence from DEV_DB_UUP.CPP_CORE.subscriber_profile where (aspiring_chef_bucket = 'HIGH') "
        cs.execute(sql)
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
    finally:
        cs.close()
    ctx.close()


@flow(log_prints=True)
def main_flow():
    connect_sf()
if __name__ == "__main__":
    main_flow()

