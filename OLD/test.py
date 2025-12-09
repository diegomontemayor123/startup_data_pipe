import requests, json, hmac, hashlib

API="http://localhost:8001/company_profile"
KEY="test123"
SECRET="test123"

payload={"id":"c1","name":"OpenAI","amount":1000000}
raw=json.dumps(payload).encode()
sig=hmac.new(SECRET.encode(),raw,hashlib.sha256).hexdigest()

r=requests.post("http://localhost:8000/funding",headers={"X-FUNDING-SIG":sig},data=raw)

print("fund webhook:",r.status_code,r.text)
r=requests.get(API,headers={"X-API-KEY":KEY})
print("company_profile:",r.status_code)
for row in r.json(): print(row)
