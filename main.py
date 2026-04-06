from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from concurrent.futures import ThreadPoolExecutor
import asyncio
from scraper import run_scrape

app = FastAPI()
templates = Jinja2Templates(directory="templates")
executor = ThreadPoolExecutor(max_workers=2)

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request, "index.html")

@app.post("/scan")
async def scan(
    keywords: str = Form(...),
    country: str = Form("US"),
    scroll_rounds: int = Form(6),
    top_n: int = Form(20),
    min_score: int = Form(0),
):
    kw_list = [k.strip() for k in keywords.strip().splitlines() if k.strip()]

    def do_scrape():
        return run_scrape(
            keywords=kw_list,
            country=country,
            scroll_rounds=scroll_rounds,
        )

    try:
        loop = asyncio.get_event_loop()
        winners, all_ads = await loop.run_in_executor(executor, do_scrape)

        filtered = [w for w in winners if w["win_score"] >= min_score][:top_n]

        # attach thumbnail
        for w in filtered:
            sig = w.get("signature", "")
            media = [a.get("media_url", "") for a in all_ads
                     if a.get("product_signature") == sig and a.get("media_url")]
            w["thumb"] = media[0] if media else ""

        return {"ok": True, "winners": filtered, "total": len(winners)}
    except Exception as e:
        return {"ok": False, "error": str(e)}
