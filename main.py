import streamlit as st
import pandas as pd
from playwright.sync_api import sync_playwright

# --- HELPER FUNCTIONS FOR SCRAPING ---
def safe_extract(page, selector, attribute="innerText"):
    """Safely extracts data from a webpage. Returns 'N/A' if not found."""
    try:
        # Timeout একদম কমিয়ে 500ms (0.5s) করা হয়েছে স্পিড বাড়ানোর জন্য
        element = page.locator(selector).first
        if element.count() > 0:
            if attribute == "innerText":
                return element.inner_text(timeout=500)
            else:
                return element.get_attribute(attribute, timeout=500)
    except Exception:
        pass
    return "N/A"

def scrape_google_maps(keyword, location, max_leads, status_text, progress_bar):
    """Fast scraping engine using Playwright."""
    leads_data = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        page = context.new_page()

        # 🚀 SPEED HACK: Block images, fonts, and media to load pages instantly!
        page.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "media", "font"] else route.continue_())

        try:
            # 1. Format the search URL
            search_query = f"{keyword} in {location}".replace(" ", "+")
            url = f"https://www.google.com/maps/search/{search_query}"
            
            status_text.info(f"🔍 Searching for '{keyword}' in '{location}'...")
            page.goto(url, timeout=60000, wait_until="domcontentloaded")
            page.wait_for_timeout(2000) # Short wait for initial load

            # 2. Scroll and collect business URLs fast
            status_text.info("📜 Scrolling to find leads... Please wait.")
            business_urls = set()
            
            page.hover('a[href*="https://www.google.com/maps/place/"]')
            
            scroll_attempts = 0
            while len(business_urls) < max_leads and scroll_attempts < 15:
                page.mouse.wheel(0, 5000)
                page.wait_for_timeout(800) # Fast scroll wait
                
                links = page.locator('a[href*="https://www.google.com/maps/place/"]').all()
                for link in links:
                    href = link.get_attribute('href')
                    if href:
                        business_urls.add(href)
                
                scroll_attempts += 1

            business_urls = list(business_urls)[:max_leads]
            total_found = len(business_urls)
            
            if total_found == 0:
                status_text.error("❌ No leads found. Try a different keyword or location.")
                return []

            status_text.info(f"✅ Found {total_found} leads. Extracting data at high speed...")

            # 3. Visit each URL and extract data FAST
            for index, biz_url in enumerate(business_urls):
                progress = (index + 1) / total_found
                progress_bar.progress(progress)
                status_text.text(f"⚡ Fast Scraping lead {index + 1} of {total_found}...")

                # wait_until="domcontentloaded" makes it skip waiting for heavy scripts
                page.goto(biz_url, timeout=60000, wait_until="domcontentloaded")
                page.wait_for_timeout(500) # Just 0.5 seconds wait!

                # Extract Data Points
                name = safe_extract(page, 'h1')
                category = safe_extract(page, 'button[jsaction="pane.rating.category"]')
                address = safe_extract(page, 'button[data-item-id="address"]')
                phone = safe_extract(page, 'button[data-item-id^="phone:tel:"]')
                website = safe_extract(page, 'a[data-item-id="authority"]', attribute="href")
                
                rating_text = safe_extract(page, 'div.F7nice')
                rating = "N/A"
                reviews = "N/A"
                if rating_text != "N/A" and "\n" in rating_text:
                    parts = rating_text.split("\n")
                    rating = parts[0]
                    reviews = parts[1].replace("(", "").replace(")", "")

                leads_data.append({
                    "Business Name": name,
                    "Category": category,
                    "Address": address,
                    "Phone Number": phone,
                    "Website": website,
                    "Rating": rating,
                    "Total Reviews": reviews,
                    "Google Maps Link": biz_url
                })

        except Exception as e:
            st.error(f"An error occurred: {e}")
        finally:
            browser.close()

    return leads_data


# --- STREAMLIT WEB INTERFACE ---
st.set_page_config(page_title="Fast G-Maps Scraper", page_icon="⚡", layout="centered")

st.title("⚡ Fast Google Maps Lead Generator")
st.markdown("Extract business leads directly from Google Maps at high speed!")

col1, col2 = st.columns(2)
with col1:
    keyword = st.text_input("Keyword (e.g., Agency, Plumbers)", placeholder="Agency")
with col2:
    location = st.text_input("Location (e.g., UK, Dhaka)", placeholder="UK")

max_leads = st.slider("Maximum Leads to Scrape", min_value=5, max_value=100, value=20, step=5)

if st.button("🚀 Start Fast Automation", type="primary"):
    if not keyword or not location:
        st.warning("⚠️ Please enter both a keyword and a location.")
    else:
        status_text = st.empty()
        progress_bar = st.progress(0)
        
        with st.spinner("Initializing Fast Scraper Engine..."):
            data = scrape_google_maps(keyword, location, max_leads, status_text, progress_bar)
        
        if data:
            status_text.success("🎉 Scraping Complete!")
            df = pd.DataFrame(data)
            st.dataframe(df)
            
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Leads as CSV",
                data=csv,
                file_name=f"{keyword}_{location}_leads.csv",
                mime="text/csv",
            )
