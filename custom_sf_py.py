from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
# import database_instance from master_fast/master_fast/snowflakeconn/sconn_new.py
from sconn_new import database_instance
from datetime import datetime
app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# on startup
@app.on_event("startup")
async def startup_event():
    c = database_instance.make_connection()
    # execute a dummy query
    print("!!!!!!!!!!!!",dir(c))
    connection = c.connect
    df_response = connection.cursor().execute("select 1")


@app.get("/test")
async def test():
    print("start", datetime.now().strftime("%H:%M:%S"))
    c = database_instance.make_connection()
    connection = c.connect()
    df_response = connection.cursor().execute("select 1")
    print("end", datetime.now().strftime("%H:%M:%S"))
    return df_response

@app.on_event("shutdown")
async def shutdown_event():
    database_instance.shutdown()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("custom_sf_py:app", port=8000, reload=True)