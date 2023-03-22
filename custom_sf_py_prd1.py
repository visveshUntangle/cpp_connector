import snowflake.connector

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from smart_open import open




print("opening key...")
    
with open('C:\\Users\\Mind-Graph\\OneDrive - Mindgraph Solutions Sdn Bhd\\Desktop\\Globe\\cpp_connector\\cpp_sfprod_rsa_key.p8', "rb") as key:

    p_key= serialization.load_pem_private_key(
    key.read(),
    password='cCpPr0Dp@ss$F'.encode(),
    backend=default_backend()
)


pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())

    
print("connecting to Snowflake:")
ctx = snowflake.connector.connect(
    user='SF_DB_PRD_ALL_CPP',
    account='pe68509.ap-southeast-1.privatelink',
    private_key=pkb,
    warehouse='PRJ_VWH_UUP_CPP',
    database='PRD_DB_UUP',
    schema='UUP_EXT',
    role='SA_FR_SF_DB_PRD_ALL_CPP'
)

cs = ctx.cursor()

try:
    
    cs.execute("select distinct  from PRD_DB_UUP.UUP_EXT.subscriber_profile_vw  ")
    df = cs.fetch_pandas_all()
    print(df)
    df.to_csv('output_prd_2.csv')
finally:
    cs.close()
ctx.close()