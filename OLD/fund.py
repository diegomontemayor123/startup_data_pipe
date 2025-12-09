import hmac, hashlib, requests

secret = b"test123"
body = b'{"id":"stripe","name":"Stripe","domain":"stripe.com","round":"Series A","amount_usd":2000000}'

sig = hmac.new(secret, body, hashlib.sha256).hexdigest()
r = requests.post("http://localhost:8000/funding",headers={"X-FUNDING-SIG": sig},data=body)
print(r.status_code, r.text)
