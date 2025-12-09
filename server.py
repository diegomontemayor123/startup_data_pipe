import os, uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from snowflake.connector import connect
from datetime import datetime

key = os.getenv("API_KEY", "test123")
ctx = connect( user='DIEGOMONTEMAYOR',password='Uwt9CbCTMy4QHA9',account='dj90916.us-east-2.aws',warehouse='mini_crust_wh',database='mini_crust_db')
app = FastAPI()

def auth(req: Request):
    if req.headers.get("X-API-KEY") != key: raise HTTPException(401)

def serialize_row(row, cols):
    out = []
    for v in row:
        if isinstance(v, datetime): out.append(v.isoformat())
        elif hasattr(v, "to_dict"):  out.append(dict(v))
        else: out.append(v)
    return dict(zip(cols, out))

@app.get("/company_profile")
async def company_profile(req:Request):
    auth(req)
    q="""SELECT * FROM CUSTOMER.COMPANY_PROFILE"""
    with ctx.cursor() as c:
        c.execute(q)
        cols = [d[0].lower() for d in c.description]
        rows = [serialize_row(r, cols) for r in c.fetchall()]
        return JSONResponse(rows)

@app.post("/webhook/fund")
async def fund(req:Request):
    auth(req)
    d=await req.json()
    with ctx.cursor() as c: c.execute("INSERT INTO RAW.FUNDING_EVENTS_RAW VALUES(CURRENT_TIMESTAMP,%s)",(d,))
    return {"ok":True}

if __name__ == "__main__": uvicorn.run("server:app", host="0.0.0.0", port=8001, reload=True)
