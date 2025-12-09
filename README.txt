Momentum Tracker

Polls HN job posts + ingests signed funding events →
builds Snowflake models via model(), scd_lite(), custview() → serves unified company profiles via API.

Setup
1. Create a Snowflake account (free tier works).
2. `pip install -r requirements.txt`
3. Set env vars:
   FUNDING_SECRET=test123
   API_KEY=test123
   SNOWFLAKE creds…

Run
1. Start ingestion (webhook @ :8000 + HN poller):
   python ingestion.py
2. Build models / refresh tables:
   model.model(), model.scd_lite(), model.custview()
3. Start API server (company_profile @ :8001):
   python server.py
4. Demo:
   python demo.py

Notes
• Funding webhook requires header: X-FUNDING-SIG = HMAC-SHA256(raw_body, FUNDING_SECRET)  
• HN poller fetches latest 5 job stories every 10 minutes  
• Customer API returns normalized_name, HN activity, funding history (max round, last round), version timestamp
