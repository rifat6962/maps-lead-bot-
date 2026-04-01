import os, csv, io, threading
from dotenv import load_dotenv
from apify_client import ApifyClient
from flask import Flask, render_template_string, request, send_file, jsonify

load_dotenv()
APIFY_TOKEN = os.getenv("APIFY_API_TOKEN")

app = Flask(__name__)
jobs = {}  # job tracking

HTML = """
<!DOCTYPE html>
<html lang="bn">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Maps Lead Generator</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: sans-serif; background: #f0f4f8; min-height: 100vh; display: flex; flex-direction: column; align-items: center; padding: 2rem 1rem; }
  .card { background: white; border-radius: 12px; padding: 2rem; width: 100%; max-width: 500px; box-shadow: 0 2px 12px rgba(0,0,0,0.08); }
  h1 { font-size: 1.4rem; margin-bottom: 0.3rem; color: #1a1a2e; }
  p.sub { color: #666; font-size: 0.9rem; margin-bottom: 1.5rem; }
  label { font-size: 0.85rem; color: #444; display: block; margin-bottom: 0.3rem; }
  input { width: 100%; padding: 0.6rem 0.8rem; border: 1px solid #ddd; border-radius: 8px; font-size: 0.95rem; margin-bottom: 1rem; outline: none; }
  input:focus { border-color: #4f46e5; }
  button { width: 100%; padding: 0.75rem; background: #4f46e5; color: white; border: none; border-radius: 8px; font-size: 1rem; cursor: pointer; }
  button:hover { background: #4338ca; }
  button:disabled { background: #a5b4fc; cursor: not-allowed; }
  .status { margin-top: 1.2rem; padding: 0.8rem 1rem; border-radius: 8px; font-size: 0.9rem; display: none; }
  .status.running { background: #fef9c3; color: #854d0e; display: block; }
  .status.done { background: #dcfce7; color: #166534; display: block; }
  .status.error { background: #fee2e2; color: #991b1b; display: block; }
  .dl-btn { margin-top: 0.8rem; display: none; width: 100%; padding: 0.65rem; background: #16a34a; color: white; border: none; border-radius: 8px; font-size: 0.95rem; cursor: pointer; }
  .dl-btn:hover { background: #15803d; }
</style>
</head>
<body>
<div class="card">
  <h1>🗺️ Maps Lead Generator</h1>
  <p class="sub">Google Maps থেকে business leads বের করো</p>

  <label>📍 Location</label>
  <input id="loc" placeholder="যেমন: Gulshan Dhaka" />

  <label>🔍 Keyword</label>
  <input id="kw" placeholder="যেমন: restaurant" />

  <button id="btn" onclick="startScrape()">শুরু করো</button>

  <div class="status" id="status">⏳ Scraping চলছে... ৩–৫ মিনিট লাগবে</div>
  <button class="dl-btn" id="dl">📥 CSV Download করো</button>
</div>

<script>
let jobId = null;
let poll = null;

async function startScrape() {
  const loc = document.getElementById('loc').value.trim();
  const kw  = document.getElementById('kw').value.trim();
  if (!loc || !kw) { alert('Location আর Keyword দাও!'); return; }

  document.getElementById('btn').disabled = true;
  document.getElementById('dl').style.display = 'none';
  const st = document.getElementById('status');
  st.className = 'status running';
  st.textContent = '⏳ Scraping শুরু হয়েছে... ৩–৫ মিনিট লাগবে';

  const res = await fetch('/start', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({location: loc, keyword: kw})
  });
  const data = await res.json();
  jobId = data.job_id;

  poll = setInterval(checkStatus, 5000);
}

async function checkStatus() {
  if (!jobId) return;
  const res = await fetch(`/status/${jobId}`);
  const data = await res.json();
  const st = document.getElementById('status');

  if (data.status === 'done') {
    clearInterval(poll);
    st.className = 'status done';
    st.textContent = `✅ সম্পন্ন! ${data.count} টি lead পাওয়া গেছে।`;
    const dl = document.getElementById('dl');
    dl.style.display = 'block';
    dl.onclick = () => window.location = `/download/${jobId}`;
    document.getElementById('btn').disabled = false;
  } else if (data.status === 'error') {
    clearInterval(poll);
    st.className = 'status error';
    st.textContent = '❌ Error হয়েছে। আবার চেষ্টা করো।';
    document.getElementById('btn').disabled = false;
  } else {
    st.textContent = '⏳ Scraping চলছে... একটু অপেক্ষা করো';
  }
}
</script>
</body>
</html>
"""

def run_scrape(job_id, location, keyword):
    try:
        jobs[job_id] = {'status': 'running', 'leads': []}
        client = ApifyClient(APIFY_TOKEN)
        run = client.actor("compass/crawler-google-places").call(run_input={
            "searchStringsArray": [f"{keyword} in {location}"],
            "maxCrawledPlacesPerSearch": 50,
            "language": "en",
            "includeHistogram": False,
            "includeOpeningHours": False,
            "includePeopleAlsoSearchFor": False,
        })
        leads = []
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            leads.append({
                "name":      item.get("title", "N/A"),
                "phone":     item.get("phone", "N/A"),
                "email":     item.get("email", "N/A"),
                "address":   item.get("address", "N/A"),
                "category":  item.get("categoryName", "N/A"),
                "rating":    item.get("totalScore", "N/A"),
                "reviews":   item.get("reviewsCount", "N/A"),
                "website":   item.get("website", "N/A"),
                "maps_link": item.get("url", "N/A"),
            })
        jobs[job_id] = {'status': 'done', 'leads': leads, 'count': len(leads)}
    except Exception as e:
        jobs[job_id] = {'status': 'error', 'error': str(e)}

@app.route('/')
def index():
    return render_template_string(HTML)

@app.route('/start', methods=['POST'])
def start_job():
    data = request.json
    import uuid, time
    job_id = str(uuid.uuid4())[:8]
    t = threading.Thread(target=run_scrape, args=(job_id, data['location'], data['keyword']))
    t.daemon = True
    t.start()
    return jsonify({'job_id': job_id})

@app.route('/status/<job_id>')
def status(job_id):
    job = jobs.get(job_id, {'status': 'not_found'})
    return jsonify({'status': job['status'], 'count': job.get('count', 0)})

@app.route('/download/<job_id>')
def download(job_id):
    job = jobs.get(job_id)
    if not job or job['status'] != 'done':
        return "Not ready", 400
    leads = job['leads']
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=leads[0].keys())
    writer.writeheader()
    writer.writerows(leads)
    out.seek(0)
    return send_file(
        io.BytesIO(out.getvalue().encode('utf-8-sig')),
        mimetype='text/csv',
        as_attachment=True,
        download_name='leads.csv'
    )

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
```

---

### `requirements.txt` এ flask যোগ করো
```
python-telegram-bot==20.7
apify-client==1.7.0
python-dotenv==1.0.0
flask==3.0.0
