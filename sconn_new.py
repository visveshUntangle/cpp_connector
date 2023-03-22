
import sqlalchemy.pool as pool

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from smart_open import open
from datetime import datetime
print("opening key...", datetime.now().strftime("%H:%M:%S"))

class Database:
    def __init__(self): 
        self.connection = None
        with open('C:\\Users\\Mind-Graph\\OneDrive - Mindgraph Solutions Sdn Bhd\\Desktop\\Globe\\cpp_connector\\cpp_sfdev_rsa_key.p8', "rb") as key:

            p_key= serialization.load_pem_private_key(
            key.read(),
            password='cCpD3vp@ss$F'.encode(),
            backend=default_backend()
        )

        print("key opened...", datetime.now().strftime("%H:%M:%S"))
        self.pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())
        
    
    def getconn(self):

        c = snowflake.connector.connect(
            user='SF_DB_DEV_ALL_CPP',
            account='pe68509.ap-southeast-1.privatelink',
            private_key=self.pkb,
            warehouse='PRJ_VWH_UUP_CPP',
            database='DEV_DB_UUP',
            schema='CPP_CORE',
            role='SA_FR_SF_DB_DEV_ALL_CPP'
        )
        self.connection = pool.QueuePool(c, max_overflow=10, pool_size=5,)
        

    
    def make_connection(self):
        if self.connection is None:
            self.getconn()
        return self.connection

    def shutdown(self):
        self.connection.dispose()

database_instance = Database()



    # use it

#     connections = [mypool.connect() for _ in range(10)]

#     for i,c in enumerate(connections):

#     for rec in c.cursor().execute("select {}".format(i)):

#         print(rec)

    

#     for c in connections:

#     c.close()

 

    