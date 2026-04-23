from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import re
import os
import csv
import io
import asyncio
import uuid
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from scraper import run_scrape

_executor = ThreadPoolExecutor(max_workers=2)
os.makedirs("static/thumbs", exist_ok=True)

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="static")
ADMIN_KEY = os.getenv("ADMIN_KEY", "123456")
TRACK_FILE = "tracking_log.json"

EXCLUDED_VISITOR_IDS = {
    "c60e2321-7899-433b-a932-94c744b27c01"
}

def read_tracking():
    if not os.path.exists(TRACK_FILE):
        return []
    with open(TRACK_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception:
            return []

def write_tracking(data):
    with open(TRACK_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

@app.get("/", response_class=HTMLResponse)
async def root():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.post("/track")
async def track(request: Request):
    try:
        body = await request.json()
        logs = read_tracking()

        visitor_id = body.get("visitor_id") or str(uuid.uuid4())

        if visitor_id in EXCLUDED_VISITOR_IDS:
            return JSONResponse({
                "ok": True,
                "ignored": True,
                "visitor_id": visitor_id
            })

        event = {
            "visitor_id": visitor_id,
            "event": body.get("event", "page_view"),
            "page": body.get("page", "/"),
            "detail": body.get("detail", ""),
            "timestamp": datetime.utcnow().isoformat(),
            "user_agent": request.headers.get("user-agent", ""),
            "referer": request.headers.get("referer", ""),
            "client_ip": request.client.host if request.client else ""
        }

        logs.append(event)
        write_tracking(logs)

        response = JSONResponse({"ok": True, "visitor_id": visitor_id})
        if not body.get("visitor_id"):
            response.set_cookie("visitor_id", visitor_id, max_age=60 * 60 * 24 * 365)
        return response

    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})

@app.get("/admin", response_class=HTMLResponse)
async def admin(request: Request, key: str = ""):
    if key != ADMIN_KEY:
        return HTMLResponse("<h2>403 Forbidden</h2><p>Invalid admin key.</p>", status_code=403)

    logs = [
        x for x in read_tracking()
        if x.get("visitor_id") not in EXCLUDED_VISITOR_IDS
    ]

    total_events = len(logs)
    unique_visitors = len(set(item.get("visitor_id", "") for item in logs if item.get("visitor_id")))
    page_views = len([x for x in logs if x.get("event") == "page_view"])
    scan_events = len([x for x in logs if x.get("event") == "scan_submit"])
    export_events = len([x for x in logs if x.get("event") == "export_csv"])

    recent = sorted(logs, key=lambda x: x.get("timestamp", ""), reverse=True)[:50]

    html = f"""
    <html>
    <head>
      <title>Bảng điều khiển WinnerSpy</title>
      <style>
        body {{
          font-family: Arial, sans-serif;
          background:#f6f8fb;
          padding:30px;
          color:#1f2937;
        }}
        .grid {{
          display:grid;
          grid-template-columns: repeat(4, 1fr);
          gap:16px;
          margin-bottom:24px;
        }}
        .card {{
          background:white;
          border-radius:12px;
          padding:20px;
          box-shadow:0 8px 24px rgba(0,0,0,.06);
        }}
        .num {{
          font-size:28px;
          font-weight:800;
        }}
        table {{
          width:100%;
          border-collapse: collapse;
          background:white;
        }}
        th, td {{
          padding:12px;
          border-bottom:1px solid #e5e7eb;
          text-align:left;
          font-size:14px;
        }}
        th {{
          background:#eef2ff;
        }}
        h1, h2 {{
          margin-bottom: 16px;
        }}
      </style>
    </head>
    <body>
      <h1>Bảng điều khiển WinnerSpy</h1>

      <div class="grid">
        <div class="card">
          <div class="num">{total_events}</div>
          <div>Tổng lượt hoạt động</div>
        </div>
        <div class="card">
          <div class="num">{unique_visitors}</div>
          <div>Số người truy cập</div>
        </div>
        <div class="card">
          <div class="num">{page_views}</div>
          <div>Số lượt xem trang</div>
        </div>
        <div class="card">
          <div class="num">{scan_events}</div>
          <div>Số lần bấm quét</div>
        </div>
      </div>

      <div class="card" style="margin-bottom:24px;">
        <div class="num">{export_events}</div>
        <div>Số lần tải CSV</div>
      </div>

      <h2>Hoạt động gần đây</h2>
      <table>
        <tr>
          <th>Thời gian</th>
          <th>Visitor ID</th>
          <th>Hành động</th>
          <th>Trang</th>
          <th>Chi tiết</th>
        </tr>
        {''.join(f"<tr><td>{x.get('timestamp','')}</td><td>{x.get('visitor_id','')}</td><td>{x.get('event','')}</td><td>{x.get('page','')}</td><td>{x.get('detail','')}</td></tr>" for x in recent)}
      </table>
    </body>
    </html>
    """
    return HTMLResponse(html)

NICHE_MAP = {
    "home": ["clean","cleaning","brush","drain","mold","scrub","scrubber","kitchen","bathroom","toilet","sink","tile","grout","storage","organizer","dust","odor","stain","spray","filter","strainer","repair","zipper","gap","window","fridge","lint","fresh","wipe","sponge","squeegee","plunger","clog","pipe","vent","curtain","shelf","rack","hook","hanger","mat","rug","cushion","pillow","blanket","towel","laundry","detergent","fabric","iron","vacuum","mop","broom","bucket"],
    "beauty": ["skin","face","hair","lip","nail","serum","cream","mask","glow","beauty","makeup","moisturizer","toner","cleanser","exfoliant","sunscreen","foundation","concealer","blush","eyeshadow","mascara","eyeliner","brow","lash","curl","straighten","dye","shampoo","conditioner","scalp","pore","acne","wrinkle","anti-aging","collagen","vitamin","retinol","hyaluronic","spf","derma","gua sha","jade","roller","microneedle","led","red light","facial","peel","scrub"],
    "fashion": ["shirt","dress","bag","shoe","jacket","pants","wear","fashion","cloth","clothing","outfit","style","tshirt","hoodie","sweater","coat","skirt","shorts","jeans","legging","sock","underwear","bra","lingerie","swimsuit","bikini","hat","cap","beanie","scarf","glove","belt","wallet","purse","handbag","backpack","tote","sneaker","boot","sandal","heel","loafer","slipper","watch","jewelry","necklace","bracelet","ring","earring","sunglasses"],
    "fitness": ["gym","workout","muscle","protein","fitness","yoga","stretch","band","weight","dumbbell","barbell","kettlebell","resistance","pull-up","push-up","squat","plank","cardio","hiit","treadmill","bike","rowing","jump rope","foam roller","massage gun","recovery","supplement","creatine","bcaa","pre-workout","shaker","bottle","mat","glove","strap","belt","knee","ankle","wrist","support","brace","compression","posture","back","spine","core"],
    "pet": ["dog","cat","pet","paw","fur","leash","collar","treat","feeder","bowl","bed","crate","cage","toy","chew","scratch","litter","grooming","brush","nail","shampoo","flea","tick","dewormer","vitamin","supplement","harness","carrier","stroller","gate","fence","training","clicker","whistle","fish","bird","hamster","rabbit","reptile","aquarium","terrarium"],
    "tech": ["phone","cable","charger","usb","gadget","tech","screen","stand","mount","case","cover","protector","earphone","headphone","speaker","bluetooth","wifi","router","keyboard","mouse","monitor","laptop","tablet","ipad","iphone","android","samsung","apple","gaming","controller","webcam","microphone","ring light","tripod","drone","camera","lens","memory","storage","ssd","hub","adapter","converter","power bank","solar"],
    "baby": ["baby","infant","toddler","diaper","stroller","pacifier","bottle","nipple","formula","breastfeed","pump","nursing","swaddle","onesie","romper","bib","teether","rattle","mobile","monitor","gate","safety","car seat","booster","high chair","crib","bassinet","playpen","toy","block","puzzle","book","bath","lotion","powder","wipe","thermometer","nasal","humidifier"],
    "outdoor": ["garden","plant","outdoor","camping","hiking","tent","seed","soil","pot","planter","watering","hose","sprinkler","fertilizer","pesticide","weed","lawn","mower","trimmer","rake","shovel","glove","boot","backpack","sleeping bag","hammock","lantern","flashlight","fire","grill","bbq","cooler","fishing","hunting","archery","climbing","kayak","paddle","surf","ski","snowboard","bike","scooter","skateboard"],
    "health": ["dental","teeth","tooth","denture","retainer","whitening","floss","mouthwash","breath","gum","braces","aligner","night guard","tongue","oral","health","medical","pain","relief","joint","knee","back","neck","shoulder","posture","sleep","snore","apnea","blood pressure","glucose","diabetes","heart","cholesterol","immune","probiotic","prebiotic","digestive","gut","liver","kidney","detox","cleanse","weight loss","diet","keto","intermittent","fasting","thyroid","hormone","menopause","fertility","pregnancy","prenatal","postnatal"],
}

NICHE_ICONS = {
    "home": "🏠", "beauty": "💄", "fashion": "👗", "fitness": "💪",
    "pet": "🐾", "tech": "📱", "baby": "👶", "outdoor": "🌿",
    "health": "🏥", "other": "🛍️",
}

def detect_niche(product, domain, slug, page="", raw_text=""):
    text = f"{product} {domain} {slug} {page} {raw_text[:200]}".lower()
    scores = {}
    for niche, words in NICHE_MAP.items():
        score = sum(1 for w in words if w in text)
        if score > 0:
            scores[niche] = score
    if not scores:
        return "other"
    return max(scores, key=scores.get)

def is_shopify(domain, raw_text=""):
    d = (domain or "").lower()
    if d.endswith(".myshopify.com"):
        return True
    if "shopify" in d:
        return True
    if re.search(r"\.(com|co|store|shop)$", d):
        return True
    return False

def get_better_product_name(product, slug, domain, page, raw_text=""):
    if slug and slug not in {"none","unknown","products","product","collections","shop","store","all","home","index","homepage"}:
        name = slug.replace("-"," ").replace("_"," ").strip()
        if len(name) > 3 and not name.isdigit():
            return name

    if product and product not in {"unknown","none"}:
        return product

    if raw_text:
        lines = [l.strip() for l in raw_text.split("\n") if l.strip()]
        for line in lines[:5]:
            if 5 < len(line) < 60 and not any(x in line.lower() for x in ["library id","id thư viện","started running","ngày bắt đầu","được tài trợ","sponsored","see ad","xem chi"]):
                return line

    return product or "unknown"

@app.post("/scan")
async def scan(
    keywords: str = Form(...),
    country: str = Form("US"),
    scroll_rounds: int = Form(6),
    top_n: int = Form(20),
    min_score: int = Form(0),
    niche_filter: str = Form("all"),
    shopify_only: str = Form(None),
):
    try:
        kw_list = [k.strip() for k in keywords.strip().splitlines() if k.strip()]
        if not kw_list:
            return JSONResponse({"ok": False, "error": "No keywords provided"})

        loop = asyncio.get_event_loop()
        winners, all_ads = await loop.run_in_executor(
            _executor,
            lambda: run_scrape(keywords=kw_list, country=country, scroll_rounds=scroll_rounds)
        )

        domain_to_ads = {}
        for ad in all_ads:
            d = ad.get("domain", "")
            if d not in domain_to_ads:
                domain_to_ads[d] = []
            domain_to_ads[d].append(ad)

        result_list = []
        for w in winners:
            score = w.get("win_score", 0)
            label = w.get("label", "weak")

            if score < min_score:
                continue

            domain = w.get("sample_domain", "")
            slug = w.get("sample_slug", "")
            raw_ads = domain_to_ads.get(domain, [])
            raw_text = raw_ads[0].get("raw_text", "") if raw_ads else ""
            page = w.get("pages", [""])[0] if w.get("pages") else ""

            product = get_better_product_name(
                w.get("product", ""),
                slug,
                domain,
                page,
                raw_text
            )

            niche = detect_niche(product, domain, slug, page, raw_text)
            shopify = is_shopify(domain)

            if niche_filter != "all" and niche != niche_filter:
                continue
            if shopify_only == "true" and not shopify:
                continue

            ad_ids = w.get("ad_ids", [])
            ad_link = f"https://www.facebook.com/ads/library/?id={ad_ids[0]}" if ad_ids else ""

            thumb = ""
            for ad in raw_ads:
                if ad.get("thumb_path"):
                    thumb = ad["thumb_path"]
                    break

            if not thumb:
                for ad in raw_ads:
                    if ad.get("media_url"):
                        thumb = ad["media_url"]
                        break

            ad_copy = ""
            if raw_text:
                lines = [l.strip() for l in raw_text.split("\n") if l.strip() and len(l.strip()) > 20]
                for line in lines:
                    if not any(x in line.lower() for x in ["library id","id thư viện","started running","ngày bắt đầu","được tài trợ","sponsored"]):
                        ad_copy = line[:150]
                        break

            result_list.append({
                "product": product.title(),
                "label": label,
                "win_score": score,
                "ads_count": w.get("ads_count", 0),
                "max_days": w.get("max_days", 0),
                "median_days": w.get("median_days", 0),
                "pages_count": w.get("pages_count", 0),
                "creative_count": w.get("creative_count", 0),
                "evidence_points": w.get("evidence_points", 0),
                "confidence": w.get("confidence", "low"),
                "sample_url": w.get("sample_url", ""),
                "sample_domain": domain,
                "niche": niche,
                "niche_icon": NICHE_ICONS.get(niche, "🛍️"),
                "shopify": shopify,
                "ad_link": ad_link,
                "thumb": thumb,
                "ad_copy": ad_copy,
                "keywords": w.get("keywords", []),
                "reasons": w.get("reasons", []),
                "ads_signal": w.get("ads_signal", 0),
                "durability_signal": w.get("durability_signal", 0),
                "page_signal": w.get("page_signal", 0),
                "creative_signal": w.get("creative_signal", 0),
            })

        result_list = result_list[:top_n]

        return JSONResponse({
            "ok": True,
            "total": len(winners),
            "winners": result_list,
        })

    except Exception as e:
        import traceback
        return JSONResponse({"ok": False, "error": str(e), "trace": traceback.format_exc()})

@app.post("/export-csv")
async def export_csv(
    keywords: str = Form(...),
    country: str = Form("US"),
    scroll_rounds: int = Form(6),
    top_n: int = Form(20),
    min_score: int = Form(0),
):
    try:
        kw_list = [k.strip() for k in keywords.strip().splitlines() if k.strip()]
        winners, all_ads = run_scrape(keywords=kw_list, country=country, scroll_rounds=scroll_rounds)

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Rank", "Product", "Label", "Score", "Ads", "Days",
            "Pages", "Creatives", "Domain", "Store URL", "Ad Link",
            "Niche", "Shopify"
        ])

        for i, w in enumerate(winners[:top_n], 1):
            if w.get("win_score", 0) < min_score:
                continue

            ad_ids = w.get("ad_ids", [])
            ad_link = f"https://www.facebook.com/ads/library/?id={ad_ids[0]}" if ad_ids else ""

            writer.writerow([
                i,
                w.get("product", ""),
                w.get("label", ""),
                w.get("win_score", 0),
                w.get("ads_count", 0),
                w.get("max_days", 0),
                w.get("pages_count", 0),
                w.get("creative_count", 0),
                w.get("sample_domain", ""),
                w.get("sample_url", ""),
                ad_link,
                detect_niche(w.get("product", ""), w.get("sample_domain", ""), w.get("sample_slug", "")),
                is_shopify(w.get("sample_domain", "")),
            ])

        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=winnerspy_results.csv"}
        )

    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})