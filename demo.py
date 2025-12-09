import requests,json,hmac,hashlib,time,model

API="http://localhost:8001/company_profile"
FUND="http://localhost:8000/funding"
KEY="test123"
SECRET="test123"

def sig(b): return hmac.new(SECRET.encode(),b,hashlib.sha256).hexdigest()

def post_fund(p): 
    r=requests.post(FUND,headers={"X-FUNDING-SIG":sig(json.dumps(p).encode())},data=json.dumps(p).encode()); 
    print("fund →",r.status_code,r.text)

def get_profiles(): 
    r=requests.get(API,headers={"X-API-KEY":KEY}); 
    print("profile →",r.status_code)
    return r.json()

print("\n=== DEMO: ingest multiple funding rounds ===")
post_fund({"id":"clearspace","name":"clearspace","round":"Series A","amount_usd":28900000})
post_fund({"id":"infracost","name":"infracost","round":"Seed","amount_usd":3080000})
post_fund({"id":"infracost","name":"infracost","round":"Series A","amount_usd":15000000})
post_fund({"id":"converge","name":"converge","round":"Series A","amount_usd":22500000})
post_fund({"id":"legion health","name":"legion health","round":"Seed","amount_usd":2000000})
post_fund({"id":"legion health","name":"legion health","round":"Seed","amount_usd":6300000})
post_fund({"id":"emerge career","name":"emerge career","round":"Seed","amount_usd":3200000})
post_fund({"id":"infisical","name":"infisical","round":"Series A","amount_usd":16000000})
post_fund({"id":"saturn","name":"saturn","round":"Series A","amount_usd":12900000})

print("\n=== DEMO: run modelling pipeline ===")
for f in [("model",model.model),("scd_lite",model.scd_lite),("custview",model.custview)]:
    print(f"→ running {f[0]}()…")
    try: f[1](); print(f"{f[0]} complete")
    except Exception as e: print(f"{f[0]} failed:", e)

time.sleep(2)
profiles=get_profiles()
print(json.dumps(profiles,indent=2))

print("\n=== DEMO: highlight specific company (infracost) ===")
o=[p for p in profiles if p.get("normalized_name")=="infracost"]
print(json.dumps(o,indent=2))

if o:
    x=o[0]
    print(f"""
Company: {x['normalized_name']}
HN Jobs: {x.get('hn_job_count')}
Funding events: {x.get('funding_events')}
Largest round: {x.get('max_amt')}
Last round:    {x.get('last_round')}
Version(ts):   {x.get('version')}
""")
print("\n=== DEMO COMPLETE ===")
