#!/usr/bin/env python3
import time
import re
import csv
import hashlib
import statistics
import urllib.parse
import os
from datetime import datetime
from collections import Counter, defaultdict
from playwright.sync_api import sync_playwright

COUNTRY = "US"
SCROLL_ROUNDS = 6
TOP_N = 20
DEBUG = False

THUMB_DIR = "static/thumbs"
os.makedirs(THUMB_DIR, exist_ok=True)

BAD_DOMAINS = {
    "facebook.com","instagram.com","amazon.com","tiktok.com","youtube.com",
    "youtu.be","pinterest.com","ebay.com","walmart.com","etsy.com","temu.com","aliexpress.com",
}
BAD_SLUGS = {
    "","none","products","product","collections","collection","shop","store",
    "catalog","search","all","item","items","home","homepage","index"
}
NICHE_WORDS = [
    "clean","cleaning","brush","scrub","scrubber","remover","organizer","storage",
    "drain","mold","lint","pet","hair","kitchen","sink","repair","zipper","gel",
    "filter","sealer","spray","fridge","tile","gap","groove","window","bathroom",
    "toilet","grout","stain","dust","odor",
]
BAD_DOMAIN_WORDS = [
    "novel","story","reader","fiction","drama","episode","movie","video","short",
    "tv","stream","comic","manga","webnovel","novelbox","soda",
]
BAD_PRODUCT_WORDS = [
    "synopsis","chapter","episode","watch","read","novel","story","drama",
    "movie","series","season","book",
]
GOOD_NICHE_WORDS = [
    "clean","cleaning","brush","scrub","scrubber","remover","dust","stain","mold",
    "drain","grout","toilet","bathroom","sink","spray","lint","pet","hair","kitchen",
    "bag","sealer","storage","organizer","filter","strainer","repair","zipper","tile",
    "gap","window","odor","fresh",
]
BAD_EXACT_DOMAINS = {"fb.me","walmart.com"}
BAD_DOMAIN_PARTS = ["buyerswiki","pagefly"]
BAD_SLUG_WORDS = {"unknown","none","pagefly","synopsis"}
SIGNATURE_STOPWORDS = {
    "the","and","for","with","from","this","that","pro","plus","set","kit","pack",
    "pcs","piece","pieces","new","best","shop","store","product","products","official",
    "sale","buy","get","all","home","item","items","v2","v3","page","landing","to",
    "a","an","of","in","on","by","at","up","ultra","max",
}
FREQUENCY_TOKEN_BLACKLIST = {
    "clean","cleaning","product","products","tool","gadget","hack","satisfying","bathroom","kitchen","home",
}

NICHE_MAP = {
    "beauty": ["beauty","skincare","makeup","cosmetic","serum","cream","lotion","face","skin","hair","nail","lip","eye","glow","anti-aging","moisturizer"],
    "home": ["home","kitchen","bathroom","bedroom","living","furniture","decor","organizer","storage","clean","cleaning","brush","scrub","drain","mold","tile","grout","toilet","sink","window","dust","odor","spray","filter"],
    "fashion": ["fashion","clothing","dress","shirt","pants","shoes","bag","wallet","jewelry","watch","accessory","outfit","style","wear","apparel"],
    "fitness": ["fitness","gym","workout","exercise","yoga","sport","muscle","protein","supplement","weight","training","running","cycling"],
    "pet": ["pet","dog","cat","puppy","kitten","animal","paw","fur","leash","collar","treat","grooming"],
    "tech": ["tech","gadget","phone","charger","cable","wireless","bluetooth","earphone","speaker","camera","laptop","tablet","usb","smart"],
    "baby": ["baby","infant","toddler","newborn","diaper","stroller","toy","kids","children","child","nursery"],
    "outdoor": ["outdoor","camping","hiking","fishing","garden","lawn","plant","tool","survival","adventure","travel"],
}

def detect_niche(text):
    text = text.lower()
    scores = {}
    for niche, words in NICHE_MAP.items():
        scores[niche] = sum(1 for w in words if w in text)
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "general"

NICHE_EMOJI = {
    "beauty": "💄", "home": "🏠", "fashion": "👗", "fitness": "💪",
    "pet": "🐾", "tech": "📱", "baby": "👶", "outdoor": "🌿", "general": "🛍️",
}

def debug(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)

def normalize_domain(hostname):
    hostname = (hostname or "").lower().strip()
    if hostname.startswith("www."):
        hostname = hostname[4:]
    return hostname or "unknown"

def normalize_text(text):
    text = (text or "").lower().strip()
    text = re.sub(r"[^a-z0-9\s\-_]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def numeric(value, default=0):
    try:
        return int(value)
    except Exception:
        return default

def slug_to_name(slug):
    slug = (slug or "").strip("-_ ")
    if not slug:
        return "unknown"
    return slug.replace("-", " ").replace("_", " ").strip()

def decode_facebook_redirect(href):
    if not href:
        return None
    try:
        if "l.facebook.com/l.php?u=" in href or "facebook.com/l.php?u=" in href:
            query = urllib.parse.urlparse(href).query
            real = urllib.parse.parse_qs(query).get("u", [None])[0]
            return urllib.parse.unquote(real) if real else None
        return href
    except Exception:
        return None

def clean_landing_url(url):
    if not url:
        return ""
    try:
        parsed = urllib.parse.urlparse(url)
        domain = normalize_domain(parsed.hostname)
        path = parsed.path or ""
        clean = f"{parsed.scheme}://{domain}{path}".rstrip("/")
        return clean
    except Exception:
        return url

def extract_info_from_url(url):
    if not url:
        return {"clean_url":"","domain":"unknown","slug":"none","product":"unknown","landing_type":"unknown"}
    try:
        parsed = urllib.parse.urlparse(url)
        domain = normalize_domain(parsed.hostname)
        path = (parsed.path or "").strip("/")
        parts = [p for p in path.split("/") if p.strip()]
        slug = parts[-1].lower() if parts else "none"
        slug = re.sub(r"[^a-z0-9\-_]+", "-", slug).strip("-")
        if not slug:
            slug = "none"
        product = slug_to_name(slug)
        clean_url = clean_landing_url(url)
        if not parts:
            landing_type = "homepage"
        elif slug in BAD_SLUGS:
            landing_type = "generic"
        else:
            landing_type = "product"
        return {"clean_url":clean_url,"domain":domain,"slug":slug,"product":product if product else "unknown","landing_type":landing_type}
    except Exception:
        return {"clean_url":"","domain":"unknown","slug":"none","product":"unknown","landing_type":"unknown"}

def has_low_impression(raw_text):
    text = (raw_text or "").lower()
    return "ít lượt hiển thị" in text or "low impressions" in text

def parse_start_days(text):
    text = text or ""
    m = re.search(r"Started running on\s+([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})", text, re.I)
    if m:
        date_str = m.group(1).strip()
        for fmt in ("%b %d, %Y", "%B %d, %Y", "%d %b %Y", "%d %B %Y"):
            try:
                start_date = datetime.strptime(date_str, fmt)
                return max((datetime.now() - start_date).days, 1)
            except Exception:
                pass
    m = re.search(r"Ngày bắt đầu chạy:?\s*(\d{1,2})\s+Tháng\s+(\d{1,2}),\s*(\d{4})", text, re.I)
    if m:
        try:
            day, month, year = map(int, m.groups())
            start_date = datetime(year, month, day)
            return max((datetime.now() - start_date).days, 1)
        except Exception:
            pass
    return 1

def niche_bonus(product, slug, domain):
    text = f"{product} {slug} {domain}".lower()
    score = sum(1 for w in NICHE_WORDS if w in text)
    return min(score, 4)

def relevance_score(*parts):
    text = " ".join([p for p in parts if p]).lower()
    hits = sum(1 for w in GOOD_NICHE_WORDS if w in text)
    if hits == 0: return -4
    if hits == 1: return -1
    if hits == 2: return 1
    if hits == 3: return 3
    return 5

def is_low_quality_product(domain, slug, product, sample_url=""):
    d = (domain or "").lower()
    s = (slug or "").lower()
    p = (product or "").lower()
    if d in BAD_EXACT_DOMAINS: return True
    if any(x in d for x in BAD_DOMAIN_PARTS): return True
    if s in BAD_SLUG_WORDS or p in BAD_SLUG_WORDS: return True
    if re.fullmatch(r"\d{6,}", s): return True
    if re.fullmatch(r"(adv|f)\d+", s): return True
    return False

def is_bad_candidate(product, domain, pages, sample_url):
    text = " ".join([product or "", domain or "", " ".join(pages or []), sample_url or ""]).lower()
    return any(w in text for w in BAD_DOMAIN_WORDS + BAD_PRODUCT_WORDS)

def build_search_url(keyword, country="US"):
    q = urllib.parse.quote(keyword)
    return f"https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country={country}&q={q}"

def check_shopify(domain):
    """Check if domain uses Shopify by looking for /cdn/shop/ or meta generator"""
    try:
        import urllib.request
        url = f"https://{domain}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            html = resp.read(8000).decode("utf-8", errors="ignore")
            if "cdn.shopify.com" in html or "Shopify.theme" in html or "/cdn/shop/" in html:
                return True
    except Exception:
        pass
    return False

def scroll_ads(page, scroll_rounds=6):
    last = 0
    stable_rounds = 0
    for i in range(scroll_rounds):
        page.mouse.wheel(0, 800)
        count = last
        for _ in range(10):
            time.sleep(0.5)
            count_en = page.locator("text=Library ID").count()
            count_vi = page.locator("text=ID thư viện").count()
            count = max(count_en, count_vi)
            if count > last:
                break
        if count == last:
            stable_rounds += 1
        else:
            stable_rounds = 0
        if stable_rounds >= 4:
            break
        last = count

def locate_cards(page):
    selectors = [
        "xpath=//*[contains(text(),'Library ID')]/ancestor::div[7]",
        "xpath=//*[contains(text(),'ID thư viện')]/ancestor::div[7]",
    ]
    best = None
    best_count = 0
    for sel in selectors:
        try:
            loc = page.locator(sel)
            cnt = loc.count()
            if cnt > best_count:
                best = loc
                best_count = cnt
        except Exception:
            pass
    return best if best_count else None

def get_card_text(card):
    try:
        return card.inner_text(timeout=5000)
    except Exception:
        return ""

def extract_ad_id(text):
    for pat in [r"ID thư viện:?\s*(\d+)", r"Library ID:?\s*(\d+)"]:
        m = re.search(pat, text, re.I)
        if m:
            return m.group(1)
    return None

def extract_page_name(card):
    try:
        # Try strong/b tag first (Facebook renders page name bold)
        for tag in ["strong", "b", "h2", "h3"]:
            els = card.locator(tag).all()
            for el in els:
                try:
                    txt = (el.inner_text() or "").strip()
                    if txt and 2 < len(txt) <= 80:
                        return txt
                except Exception:
                    pass
        # Fallback: links to facebook pages
        links = card.locator("a[href*='facebook.com/']").all()
        for a in links:
            try:
                href = a.get_attribute("href") or ""
                txt = (a.inner_text() or "").strip()
                if not txt:
                    continue
                if "facebook.com/" in href and len(txt) <= 80:
                    skip = {"xem chi tiết quảng cáo","mở menu thả xuống","see ad details","open dropdown menu"}
                    if txt.lower() not in skip:
                        return txt
            except Exception:
                pass
        # Fallback: img alt
        img = card.locator("img[alt]").first
        if img.count() > 0:
            alt = img.get_attribute("alt")
            if alt:
                return alt.strip()
    except Exception:
        pass
    return "unknown"

def extract_landing_link(card):
    try:
        links = card.locator("a[href]").all()
    except Exception:
        links = []
    candidates = []
    for a in links:
        try:
            href = a.get_attribute("href")
            if not href:
                continue
            real = decode_facebook_redirect(href)
            if not real or "facebook.com" in real:
                continue
            candidates.append(real)
        except Exception:
            pass
    if not candidates:
        return None
    candidates = sorted(list(set(candidates)), key=lambda x: len((urllib.parse.urlparse(x).path or "")), reverse=True)
    return candidates[0]

def extract_media_url(card):
    for selector in ["video[src]", "video[poster]", "img[src]"]:
        try:
            nodes = card.locator(selector).all()
        except Exception:
            nodes = []
        for node in nodes:
            try:
                for attr in ("src", "poster"):
                    value = node.get_attribute(attr)
                    if value and value.startswith("http"):
                        return value
            except Exception:
                pass
    return ""

def take_card_screenshot(card, ad_id):
    """Screenshot the ad card and save to static/thumbs/"""
    try:
        path = os.path.join(THUMB_DIR, f"{ad_id}.png")
        card.screenshot(path=path, timeout=5000)
        return f"/static/thumbs/{ad_id}.png"
    except Exception:
        return ""

def scrape_ads(page, keyword, search_url, country="US"):
    cards = locate_cards(page)
    if not cards:
        return []
    total = cards.count()
    ads_data = []
    seen_ids = set()
    for i in range(total):
        try:
            card = cards.nth(i)
            text = get_card_text(card)
            if not text:
                continue
            ad_id = extract_ad_id(text)
            if not ad_id or ad_id in seen_ids:
                continue
            seen_ids.add(ad_id)
            page_name = extract_page_name(card)
            landing = extract_landing_link(card)
            media_url = extract_media_url(card)
            thumb_path = take_card_screenshot(card, ad_id)
            info = extract_info_from_url(landing)
            days = parse_start_days(text)
            niche_text = f"{info['product']} {info['slug']} {info['domain']} {keyword}"
            niche = detect_niche(niche_text)
            ad_link = f"https://www.facebook.com/ads/library/?id={ad_id}"
            ads_data.append({
                "keyword": keyword,
                "search_url": search_url,
                "id": ad_id,
                "ad_link": ad_link,
                "page": page_name,
                "landing_url": landing or "",
                "clean_url": info["clean_url"],
                "domain": info["domain"],
                "slug": info["slug"],
                "product": info["product"],
                "landing_type": info["landing_type"],
                "days": days,
                "raw_text": text,
                "media_url": media_url,
                "thumb_path": thumb_path,
                "niche": niche,
                "shopify": False,  # checked later per domain
            })
        except Exception as e:
            debug(f"exception on card {i}: {e}")
    return ads_data

def tokenize_signature(text):
    text = normalize_text(text).replace("_", " ").replace("-", " ")
    tokens = [t for t in text.split() if len(t) >= 3 and t not in SIGNATURE_STOPWORDS and not t.isdigit()]
    return tokens

def strip_noise_from_raw_text(raw_text):
    text = raw_text or ""
    patterns = [
        r"Library ID:?\s*\d+", r"ID thư viện:?\s*\d+",
        r"Started running on[^\n]*", r"Ngày bắt đầu chạy:?[^\n]*",
        r"See ad details", r"Xem chi tiết quảng cáo",
        r"Low impressions", r"Ít lượt hiển thị",
    ]
    for pattern in patterns:
        text = re.sub(pattern, " ", text, flags=re.I)
    return re.sub(r"\s+", " ", text).strip()

def pick_display_name(ad):
    candidates = [ad.get("product", ""), slug_to_name(ad.get("slug", ""))]
    for item in candidates:
        item = normalize_text(item)
        if item and item not in {"unknown", "none"}:
            return item
    return "unknown"

def build_product_signature(ad):
    domain = normalize_domain(ad.get("domain", "unknown"))
    page = normalize_text(ad.get("page", ""))
    product = normalize_text(ad.get("product", ""))
    slug = normalize_text(ad.get("slug", ""))
    clean_landing = clean_landing_url(ad.get("clean_url", "") or ad.get("landing_url", ""))
    seed_parts = [product, slug]
    tokens = []
    for part in seed_parts:
        tokens.extend(tokenize_signature(part))
    domain_tokens = set(tokenize_signature(domain.replace(".", " ")))
    page_tokens = set(tokenize_signature(page))
    filtered = [t for t in tokens if t not in domain_tokens and t not in page_tokens]
    if not filtered:
        filtered = tokens[:]
    filtered = sorted(set(filtered), key=lambda t: (t in FREQUENCY_TOKEN_BLACKLIST, t))
    if filtered:
        return " ".join(filtered[:5])
    parsed = extract_info_from_url(clean_landing)
    fallback = normalize_text(parsed.get("product", ""))
    if fallback and fallback not in {"unknown", "none"}:
        return fallback
    return f"domain:{domain}"

def build_creative_fingerprint(ad):
    raw_text = strip_noise_from_raw_text(ad.get("raw_text", "") or ad.get("ad_copy", ""))
    media_url = ad.get("media_url", "")
    clean_landing = clean_landing_url(ad.get("clean_url", "") or ad.get("landing_url", ""))
    page = normalize_text(ad.get("page", ""))
    keyword = normalize_text(ad.get("keyword", ""))
    if raw_text or media_url:
        seed = " | ".join([raw_text[:500], media_url, clean_landing])
    else:
        seed = " | ".join([clean_landing, keyword, page])
    return hashlib.sha1(seed.encode("utf-8", errors="ignore")).hexdigest()[:16]

def has_true_creative_signal(ads):
    return any(a.get("raw_text") or a.get("media_url") for a in ads)

def score_threshold_points(value, thresholds):
    score = 0
    for threshold, points in thresholds:
        if value >= threshold:
            score += points
    return score

def score_group(ads, signature):
    sample = ads[0]
    pages = sorted({a.get("page","") for a in ads if a.get("page") and a.get("page") != "unknown"})
    keywords = sorted({a.get("keyword","") for a in ads if a.get("keyword")})
    urls = sorted({clean_landing_url(a.get("clean_url","") or a.get("landing_url","")) for a in ads if a.get("clean_url") or a.get("landing_url")})
    domains = sorted({normalize_domain(a.get("domain","unknown")) for a in ads if a.get("domain") and a.get("domain") != "unknown"})
    creatives = sorted({a.get("creative_fingerprint","") for a in ads if a.get("creative_fingerprint")})

    ads_count = len(ads)
    pages_count = len(pages)
    keywords_count = len(keywords)
    urls_count = len(urls)
    domain_count = len(domains)
    creative_count = len(creatives)

    day_values = sorted(max(numeric(a.get("days",0),0),0) for a in ads)
    max_days = max(day_values) if day_values else 0
    median_days = int(statistics.median(day_values)) if day_values else 0
    ads_7d_plus = sum(1 for d in day_values if d >= 7)
    ads_14d_plus = sum(1 for d in day_values if d >= 14)
    ads_30d_plus = sum(1 for d in day_values if d >= 30)

    page_freq = Counter(a.get("page","") for a in ads if a.get("page"))
    repeat_page_count = sum(1 for _,cnt in page_freq.items() if cnt >= 2)

    name_counter = Counter(pick_display_name(ad) for ad in ads)
    product_name = name_counter.most_common(1)[0][0] if name_counter else "unknown"
    slug = normalize_text(sample.get("slug",""))
    landing_type = sample.get("landing_type","unknown")
    sample_url = urls[0] if urls else clean_landing_url(sample.get("clean_url","") or sample.get("landing_url",""))

    # Niche detection
    niche_counter = Counter(a.get("niche","general") for a in ads)
    niche = niche_counter.most_common(1)[0][0]
    niche_icon = NICHE_EMOJI.get(niche, "🛍️")

    # Shopify check (per unique domain, max 3)
    shopify = False
    checked = 0
    for d in domains[:3]:
        if d not in BAD_DOMAINS and d != "unknown":
            if check_shopify(d):
                shopify = True
                break
            checked += 1

    # Thumbnail: pick first available
    thumb = next((a.get("thumb_path","") for a in ads if a.get("thumb_path")), "")
    ad_link = next((a.get("ad_link","") for a in ads if a.get("ad_link")), "")

    reasons = []
    ads_signal = score_threshold_points(ads_count, [(2,1),(3,2),(5,3),(10,4),(20,5)])
    if ads_signal: reasons.append(f"ads_signal={ads_signal} from {ads_count} ads")

    durability_signal = 0
    durability_signal += score_threshold_points(max_days, [(3,1),(7,2),(14,3),(30,4),(90,5)])
    durability_signal += score_threshold_points(median_days, [(3,1),(7,2),(14,3)])
    durability_signal += score_threshold_points(ads_14d_plus, [(2,1),(4,2)])
    if durability_signal: reasons.append(f"durability_signal={durability_signal} (max_days={max_days})")

    page_signal = 0
    page_signal += score_threshold_points(pages_count, [(2,2),(3,3),(5,4)])
    page_signal += score_threshold_points(repeat_page_count, [(2,1),(3,2)])
    if page_signal: reasons.append(f"page_signal={page_signal} from {pages_count} pages")

    creative_signal = 0
    creative_signal += score_threshold_points(creative_count, [(2,2),(3,3),(5,4),(8,5)])
    creative_ratio = (creative_count / ads_count) if ads_count else 0.0
    if creative_ratio >= 0.35: creative_signal += 2
    if creative_ratio >= 0.55: creative_signal += 2
    if creative_signal: reasons.append(f"creative_signal={creative_signal}")

    demand_proxy_signal = 0
    demand_proxy_signal += score_threshold_points(keywords_count, [(2,2),(3,3),(5,4)])
    demand_proxy_signal += score_threshold_points(domain_count, [(2,2),(3,3)])
    demand_proxy_signal += score_threshold_points(urls_count, [(2,1),(3,2),(5,3)])
    if landing_type == "product": demand_proxy_signal += 2
    elif landing_type in {"generic","homepage"}: demand_proxy_signal -= 2
    rel = relevance_score(product_name, slug, " ".join(domains), sample_url)
    demand_proxy_signal += rel
    if demand_proxy_signal: reasons.append(f"demand_proxy_signal={demand_proxy_signal}")

    penalty = 0
    if any(d in BAD_DOMAINS for d in domains):
        penalty += 5; reasons.append("penalty: marketplace/social domain")
    if is_low_quality_product(normalize_domain(sample.get("domain","unknown")), slug, product_name, sample_url):
        penalty += 12; reasons.append("penalty: low-quality slug/domain")
    if is_bad_candidate(product_name, normalize_domain(sample.get("domain","unknown")), pages, sample_url):
        penalty += 20; reasons.append("penalty: likely non-physical/content candidate")
    if rel < 0:
        penalty += abs(rel); reasons.append("penalty: low niche relevance")
    low_impression_count = sum(1 for a in ads if has_low_impression(a.get("raw_text","")))
    if low_impression_count >= 1: penalty += 3; reasons.append("penalty: has low-impression ads")
    if low_impression_count >= 3: penalty += 2; reasons.append("penalty: many low-impression ads")

    win_score = ads_signal + durability_signal + page_signal + creative_signal + demand_proxy_signal - penalty

    evidence_points = sum([
        ads_count >= 5, max_days >= 14, median_days >= 7, pages_count >= 2,
        creative_count >= 2, keywords_count >= 2, domain_count >= 2,
        landing_type == "product", rel > 0,
    ])

    if penalty >= 20 or rel <= -4:
        label = "weak"; confidence = "low"
    elif win_score >= 24 and evidence_points >= 6:
        label = "winner_candidate"; confidence = "high"
    elif win_score >= 16 and evidence_points >= 4:
        label = "watchlist"; confidence = "medium"
    elif win_score >= 10:
        label = "testing"; confidence = "low"
    else:
        label = "weak"; confidence = "low"

    return {
        "signature": signature,
        "product": product_name,
        "niche": niche,
        "niche_icon": niche_icon,
        "shopify": shopify,
        "thumb": thumb,
        "ad_link": ad_link,
        "sample_domain": normalize_domain(sample.get("domain","unknown")),
        "sample_slug": slug,
        "sample_url": sample_url,
        "ads_count": ads_count,
        "pages_count": pages_count,
        "pages": pages,
        "repeat_page_count": repeat_page_count,
        "keywords_count": keywords_count,
        "keywords": keywords,
        "urls_count": urls_count,
        "domain_count": domain_count,
        "domains": domains,
        "creative_count": creative_count,
        "creative_ratio": round(creative_ratio, 3),
        "has_true_creative_signal": has_true_creative_signal(ads),
        "max_days": max_days,
        "median_days": median_days,
        "ads_7d_plus": ads_7d_plus,
        "ads_14d_plus": ads_14d_plus,
        "ads_30d_plus": ads_30d_plus,
        "landing_type": landing_type,
        "ads_signal": ads_signal,
        "durability_signal": durability_signal,
        "page_signal": page_signal,
        "creative_signal": creative_signal,
        "demand_proxy_signal": demand_proxy_signal,
        "penalty": penalty,
        "relevance_score": rel,
        "low_impression_count": low_impression_count,
        "evidence_points": evidence_points,
        "win_score": win_score,
        "label": label,
        "confidence": confidence,
        "reasons": reasons,
        "ad_ids": [a.get("id","") for a in ads][:20],
    }

def dedupe_ads_by_id(all_ads):
    dedup = {}
    anonymous = []
    for ad in all_ads:
        ad_id = ad.get("id")
        if ad_id:
            dedup[ad_id] = ad
        else:
            anonymous.append(ad)
    return list(dedup.values()) + anonymous

def rank_products(all_ads):
    grouped = defaultdict(list)
    for ad in all_ads:
        signature = build_product_signature(ad)
        ad["product_signature"] = signature
        ad["creative_fingerprint"] = build_creative_fingerprint(ad)
        grouped[signature].append(ad)
    ranked = [score_group(ads, signature) for signature, ads in grouped.items()]
    ranked.sort(key=lambda x: (x["win_score"], x["confidence"]=="high", x["evidence_points"], x["creative_count"], x["pages_count"], x["ads_count"], x["max_days"]), reverse=True)
    return ranked

def run_scrape(keywords: list, country: str = "US", scroll_rounds: int = 6, progress_callback=None):
    global COUNTRY, SCROLL_ROUNDS
    COUNTRY = country
    SCROLL_ROUNDS = scroll_rounds

    all_ads = []
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox","--disable-setuid-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars","--window-size=1280,800",
            ]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
        )
        for idx, keyword in enumerate(keywords):
            if progress_callback:
                progress_callback(idx, len(keywords), keyword)
            page = context.new_page()
            try:
                url = build_search_url(keyword, country)
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(7)
                scroll_ads(page, scroll_rounds)
                ads = scrape_ads(page, keyword, url, country)
                all_ads.extend(ads)
            except Exception as e:
                debug(f"Error on keyword '{keyword}': {e}")
            finally:
                page.close()
        browser.close()

    all_ads = dedupe_ads_by_id(all_ads)
    winners = rank_products(all_ads)
    return winners, all_ads
