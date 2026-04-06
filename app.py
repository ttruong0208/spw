import streamlit as st
import pandas as pd
import io
from scraper import run_scrape

st.set_page_config(
    page_title="WinnerSpy — FB Ads Product Research",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
[data-testid="stAppViewContainer"] { background: #0d0d14 !important; }
[data-testid="stSidebar"] { background: #12121e !important; border-right: 1px solid #1e1e30; }
[data-testid="stHeader"] { background: transparent !important; }
#MainMenu, footer, header { visibility: hidden; }
.topbar { display:flex; align-items:center; justify-content:space-between; padding:18px 0 10px 0; margin-bottom:8px; }
.topbar-logo { font-size:28px; font-weight:900; color:#fff; letter-spacing:-1px; }
.topbar-logo span { color:#7c3aed; }
.topbar-sub { font-size:13px; color:#666; margin-top:2px; }
.search-box { background:#16162a; border:1px solid #2a2a45; border-radius:14px; padding:20px 24px; margin-bottom:20px; }
.stat-row { display:flex; gap:12px; margin-bottom:20px; }
.stat-card { flex:1; background:#16162a; border:1px solid #2a2a45; border-radius:12px; padding:16px 20px; }
.stat-card .val { font-size:28px; font-weight:800; color:#fff; }
.stat-card .lbl { font-size:12px; color:#666; margin-top:2px; }
.stat-card.green .val { color:#00e676; }
.stat-card.blue .val { color:#40c4ff; }
.stat-card.purple .val { color:#ce93d8; }
.card-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:16px; }
.pcard { background:#16162a; border:1px solid #2a2a45; border-radius:14px; overflow:hidden; transition:transform .18s,box-shadow .18s,border-color .18s; }
.pcard:hover { transform:translateY(-4px); box-shadow:0 12px 32px rgba(124,58,237,.18); border-color:#7c3aed; }
.pcard-thumb { width:100%; height:190px; object-fit:cover; display:block; background:#1e1e35; }
.pcard-thumb-ph { width:100%; height:190px; background:linear-gradient(135deg,#1e1e35 0%,#12122a 100%); display:flex; align-items:center; justify-content:center; font-size:52px; }
.pcard-body { padding:14px 16px 12px; }
.pcard-rank { font-size:11px; color:#555; font-weight:600; margin-bottom:4px; }
.pcard-title { font-size:14px; font-weight:700; color:#f0f0ff; margin-bottom:10px; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; line-height:1.4; }
.pcard-meta { font-size:12px; color:#888; margin-bottom:3px; }
.pcard-meta b { color:#bbb; }
.pcard-stats { display:flex; gap:8px; margin:10px 0 12px; flex-wrap:wrap; }
.pcard-stat { background:#1e1e35; border-radius:8px; padding:5px 10px; font-size:12px; color:#aaa; }
.pcard-stat b { color:#fff; font-size:14px; display:block; }
.pcard-footer { display:flex; align-items:center; justify-content:space-between; }
.badge { padding:4px 12px; border-radius:20px; font-size:11px; font-weight:700; letter-spacing:.3px; }
.badge-winner_candidate { background:#00e676; color:#000; }
.badge-watchlist { background:#40c4ff; color:#000; }
.badge-testing { background:#ffab40; color:#000; }
.badge-weak { background:#ff5252; color:#fff; }
.view-btn { background:#1e1e35; border:1px solid #3a3a55; color:#aaa; padding:4px 12px; border-radius:20px; font-size:11px; text-decoration:none; }
.sidebar-label { font-size:11px; color:#666; text-transform:uppercase; letter-spacing:.8px; margin-bottom:4px; }
.section-hdr { font-size:18px; font-weight:800; color:#fff; margin-bottom:16px; display:flex; align-items:center; gap:8px; }
.section-hdr .count { background:#7c3aed; color:#fff; padding:2px 10px; border-radius:20px; font-size:13px; }
</style>
""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.markdown('<div style="font-size:20px;font-weight:900;color:#fff;padding:12px 0 4px">🔥 WinnerSpy</div>', unsafe_allow_html=True)
    st.markdown('<div style="font-size:11px;color:#555;margin-bottom:20px">FB Ads Intelligence Tool</div>', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-label">Target Country</div>', unsafe_allow_html=True)
    country = st.selectbox("", ["US","GB","AU","CA","DE","FR"], label_visibility="collapsed")
    st.markdown('<div class="sidebar-label" style="margin-top:14px">Scroll Depth</div>', unsafe_allow_html=True)
    scroll_rounds = st.slider("", 3, 12, 6, label_visibility="collapsed")
    st.markdown('<div class="sidebar-label" style="margin-top:14px">Show Top N Results</div>', unsafe_allow_html=True)
    top_n = st.slider("", 5, 50, 20, label_visibility="collapsed", key="topn")
    st.markdown('<div class="sidebar-label" style="margin-top:14px">Min Win Score</div>', unsafe_allow_html=True)
    min_score = st.slider("", 0, 30, 0, label_visibility="collapsed", key="minscore")
    st.markdown('<div class="sidebar-label" style="margin-top:14px">Filter Labels</div>', unsafe_allow_html=True)
    label_filter = st.multiselect("", ["winner_candidate","watchlist","testing","weak"],
        default=["winner_candidate","watchlist","testing"], label_visibility="collapsed")
    st.markdown("---")
    st.markdown('''<div style="font-size:12px;color:#555;line-height:1.8">
    💡 3-6 keywords per niche<br>
    📈 Higher depth = more data<br>
    🏆 winner_candidate = best signal
    </div>''', unsafe_allow_html=True)

# Topbar
st.markdown("""
<div class="topbar">
  <div>
    <div class="topbar-logo">Winner<span>Spy</span></div>
    <div class="topbar-sub">Facebook Ads Intelligence — Find winning products before your competitors</div>
  </div>
</div>
""", unsafe_allow_html=True)

# Search area
st.markdown('<div class="search-box">', unsafe_allow_html=True)
col1, col2 = st.columns([4, 1])
with col1:
    keywords_input = st.text_area(
        "🔍 Keywords — one per line",
        placeholder="black mold remover\nmold remover spray\nsoap scum remover",
        height=110,
    )
with col2:
    st.markdown("<br><br>", unsafe_allow_html=True)
    run_btn = st.button("🚀 Find Winners", type="primary", use_container_width=True)
    st.markdown('<div style="font-size:11px;color:#555;text-align:center;margin-top:6px">⏱ ~30-60s / keyword</div>', unsafe_allow_html=True)
st.markdown('</div>', unsafe_allow_html=True)

# Run scraper
if run_btn:
    keywords = [k.strip() for k in keywords_input.strip().splitlines() if k.strip()]
    if not keywords:
        st.warning("Please enter at least one keyword!")
        st.stop()
    progress_bar = st.progress(0)
    status_text = st.empty()
    def update_progress(idx, total, keyword):
        progress_bar.progress(idx / total)
        status_text.markdown(f"🔍 Searching **{keyword}** ({idx+1}/{total})...")
    with st.spinner(""):
        try:
            winners, all_ads = run_scrape(
                keywords=keywords,
                country=country,
                scroll_rounds=scroll_rounds,
                progress_callback=update_progress,
            )
            progress_bar.progress(1.0)
            status_text.markdown("✅ Done!")
            st.session_state["winners"] = winners
            st.session_state["all_ads"] = all_ads
            st.session_state["keywords_used"] = keywords
        except Exception as e:
            st.error(f"❌ Error: {e}")
            st.stop()

# Results
if "winners" in st.session_state:
    winners = st.session_state["winners"]
    filtered = [w for w in winners if w["label"] in label_filter and w["win_score"] >= min_score][:top_n]
    all_ads = st.session_state.get("all_ads", [])
    n_win = sum(1 for w in winners if w["label"] == "winner_candidate")
    n_watch = sum(1 for w in winners if w["label"] == "watchlist")
    n_kw = len(st.session_state.get("keywords_used", []))
    st.markdown(f"""
    <div class="stat-row">
      <div class="stat-card"><div class="val">{len(winners)}</div><div class="lbl">Total Products</div></div>
      <div class="stat-card green"><div class="val">{n_win}</div><div class="lbl">🏆 Winner Candidates</div></div>
      <div class="stat-card blue"><div class="val">{n_watch}</div><div class="lbl">👀 Watchlist</div></div>
      <div class="stat-card purple"><div class="val">{n_kw}</div><div class="lbl">🔍 Keywords Searched</div></div>
    </div>
    """, unsafe_allow_html=True)
    vcol, dcol = st.columns([3,1])
    with vcol:
        view_mode = st.radio("", ["🃏 Cards", "📋 Table"], horizontal=True, label_visibility="collapsed")
    with dcol:
        if filtered:
            df_dl = pd.DataFrame([{
                "Rank": i+1, "Product": w["product"], "Label": w["label"],
                "Score": w["win_score"], "Ads": w["ads_count"],
                "Pages": w["pages_count"], "Max Days": w["max_days"],
                "Domain": w["sample_domain"], "URL": w["sample_url"],
            } for i, w in enumerate(filtered)])
            csv_buf = io.StringIO()
            df_dl.to_csv(csv_buf, index=False)
            st.download_button("⬇️ Export CSV", data=csv_buf.getvalue(),
                file_name="winner_products.csv", mime="text/csv", use_container_width=True)
    st.markdown(f'''
    <div class="section-hdr">Products Found <span class="count">{len(filtered)}</span></div>
    ''', unsafe_allow_html=True)
    if view_mode == "📋 Table":
        df = pd.DataFrame([{
            "Rank": i+1, "Product": w["product"], "Label": w["label"],
            "Score": w["win_score"], "Confidence": w["confidence"],
            "Ads": w["ads_count"], "Pages": w["pages_count"],
            "Max Days": w["max_days"], "Domain": w["sample_domain"], "URL": w["sample_url"],
        } for i, w in enumerate(filtered)])
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        COLS = 3
        for row_start in range(0, len(filtered), COLS):
            cols = st.columns(COLS)
            for col_idx, w in enumerate(filtered[row_start:row_start+COLS]):
                i = row_start + col_idx
                media_urls = [a.get("media_url","") for a in all_ads
                    if a.get("product_signature") == w.get("signature") and a.get("media_url")]
                thumb = media_urls[0] if media_urls else ""
                ad_ids = w.get("ad_ids", [])
                view_url = f"https://www.facebook.com/ads/library/?id={ad_ids[0]}" if ad_ids else (w.get("sample_url") or "#")
                page_name = w.get("domains", ["—"])[0] if w.get("domains") else "—"
                label = w["label"]
                thumb_html = f'<img class="pcard-thumb" src="{thumb}">' if thumb else '<div class="pcard-thumb-ph">📦</div>'
                badge_html = f'<span class="badge badge-{label}">{label.replace("_candidate","").upper()}</span>'
                card_html = f"""
                <div class="pcard">
                  {thumb_html}
                  <div class="pcard-body">
                    <div class="pcard-rank">#{i+1}</div>
                    <div class="pcard-title">{w["product"].title()}</div>
                    <div class="pcard-meta">🌐 <b>{page_name}</b></div>
                    <div class="pcard-meta">📅 Running: <b>{w["max_days"]} days</b></div>
                    <div class="pcard-stats">
                      <div class="pcard-stat"><b>{w["ads_count"]}</b>Ads</div>
                      <div class="pcard-stat"><b>{w["win_score"]}</b>Score</div>
                      <div class="pcard-stat"><b>{w["pages_count"]}</b>Pages</div>
                    </div>
                    <div class="pcard-footer">
                      {badge_html}
                      <a class="view-btn" href="{view_url}" target="_blank">🔗 View Ad</a>
                    </div>
                  </div>
                </div>
                """
                with cols[col_idx]:
                    st.markdown(card_html, unsafe_allow_html=True)

st.markdown('''
<div style="text-align:center;color:#333;font-size:11px;margin-top:48px;padding-bottom:20px">
  WinnerSpy © 2025 — Real FB Ads data. No fluff. 🔥
</div>
''', unsafe_allow_html=True)