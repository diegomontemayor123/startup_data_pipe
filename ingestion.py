import os,json,time,tempfile,requests,uvicorn,asyncio,hmac,hashlib
from fastapi import FastAPI,Request,HTTPException
from snowflake.connector import connect

secret=os.getenv("FUNDING_SECRET","test123")
ctx=connect(user='DIEGOMONTEMAYOR',password='Uwt9CbCTMy4QHA9',account='dj90916.us-east-2.aws',
            warehouse='mini_crust_wh',database='mini_crust_db')
q=lambda s: ctx.cursor().execute(s)
q("CREATE DATABASE IF NOT EXISTS mini_crust_db")
q("CREATE SCHEMA IF NOT EXISTS mini_crust_db.RAW")
q("CREATE STAGE IF NOT EXISTS RAW.HN_JOBS_STAGE")
q("CREATE TABLE IF NOT EXISTS RAW.HN_JOBS_RAW(ingest_ts TIMESTAMP,data VARIANT)")
app=FastAPI()

def sig_ok(req,raw):
    s=req.headers.get("X-FUNDING-SIG")
    if not s:return False
    mac=hmac.new(secret.encode(),raw,hashlib.sha256).hexdigest()
    return hmac.compare_digest(s,mac)

@app.post("/funding")
async def funding(req:Request):
    raw=await req.body()
    if not sig_ok(req,raw): raise HTTPException(401)
    body=json.loads(raw)
    if not body["id"]: body["id"]=body["name"].lower().replace(" ","")
    with ctx.cursor() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS RAW.FUNDING_EVENTS_RAW(ingest_ts TIMESTAMP,payload VARIANT)""")
        c.execute("""INSERT INTO RAW.FUNDING_EVENTS_RAW SELECT CURRENT_TIMESTAMP,PARSE_JSON(%s)""",[json.dumps(body)])
        ctx.commit()
    return {"ok":True}

@app.on_event("startup")
def init_raw():
    with ctx.cursor() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS RAW.HN_JOBS_RAW(ingest_ts TIMESTAMP,data VARIANT)""")
        c.execute("""CREATE STAGE IF NOT EXISTS RAW.HN_JOBS_STAGE FILE_FORMAT=(TYPE=JSON)""")
        ctx.commit()

def poll_hn():
    seen=set()
    while True:
        print("Polling HN...")
        ids=requests.get("https://hacker-news.firebaseio.com/v0/jobstories.json").json()[:10]
        new=[i for i in ids if i not in seen]
        if new:
            recs=[requests.get(f"https://hacker-news.firebaseio.com/v0/item/{i}.json").json() for i in new]
            seen.update(new)
            with tempfile.NamedTemporaryFile(mode='w',delete=False,suffix='.json') as f: f.writelines(json.dumps(r)+"\n" for r in recs); fn=f.name
            with ctx.cursor() as c:
                c.execute(f"PUT file://{fn} @RAW.HN_JOBS_STAGE AUTO_COMPRESS=FALSE")
                c.execute("""COPY INTO RAW.HN_JOBS_RAW(ingest_ts,data)
                             FROM (SELECT CURRENT_TIMESTAMP,$1 FROM @RAW.HN_JOBS_STAGE) FILE_FORMAT=(TYPE=JSON)""")
                ctx.commit()
            os.remove(fn)
            print(f"Loaded {len(recs)} new HN jobs:", [r.get("title") for r in recs]) 
        else: print("No new jobs found.")
        print("Sleeping 10 min...\n")
        time.sleep(600)


async def runner():
    asyncio.get_event_loop().run_in_executor(None,poll_hn)
    await uvicorn.Server(uvicorn.Config(app,host="0.0.0.0",port=8000)).serve()

if __name__=="__main__": asyncio.run(runner())
