import streamlit as st
import pandas as pd
from playwright.sync_api import sync_playwright
import time
import random

# --- HELPER FUNCTIONS FOR SCRAPING ---
def safe_extract(page, selector, attribute="innerText"):
    """Safely extracts data from a webpage. Returns 'N/A' if not found."""
    try:
        element = page.locator(selector).first
        if element.count() > 0:
            if attribute == "innerText":
                return element.inner_text(timeout=2000)
            else:
                return element.get_attribute(attribute, timeout=2000)
    except Exception:
        pass
    return "N/A"

def scrape_google_maps(keyword, location, max_leads, status_text, progress_bar):
    """Main scraping engine using Playwright."""
    leads_data = []
    
    # Start Playwright
    with sync_playwright() as p:
        # Launch a headless browser (invisible browser)
        browser = p.chromium.launch(headless=True)
        
        # Create a new browser context with a realistic User-Agent to avoid getting blocked
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        page = context.new_page()

        try:
            # 1. Format the search URL and go to Google Maps
            search_query = f"{keyword} in {location}".replace(" ", "+")
            url = f"https://www.google.com/maps/search/{search_query}"
            
            status_text.info(f"🔍 Searching for '{keyword}' in '{location}'...")
            page.goto(url, timeout=60000)
            time.sleep(random.uniform(3, 5)) # Wait for page to load

            # 2. Scroll and collect business URLs
            status_text.info("📜 Scrolling to find leads... Please wait.")
            business_urls = set()
            
            # We hover over the results panel and scroll down to load more
            page.hover('a[href*="https://www.google.com/maps/place/"]')
            
            scroll_attempts = 0
            while len(business_urls) < max_leads and scroll_attempts < 15:
                page.mouse.wheel(0, 5000) # Scroll down
                time.sleep(random.uniform(1.5, 3.0)) # Random delay to mimic human
                
                # Find all links to places
                links = page.locator('a[href*="https://www.google.com/maps/place/"]').all()
                for link in links:
                    href = link.get_attribute('href')
                    if href:
                        business_urls.add(href)
                
                scroll_attempts += 1

            # Convert set to list and limit to max_leads
            business_urls = list(business_urls)[:max_leads]
            total_found = len(business_urls)
            
            if total_found == 0:
                status_text.error("❌ No leads found. Try a different keyword or location.")
                return []

            status_text.info(f"✅ Found {total_found} leads. Extracting detailed data...")

            # 3. Visit each URL and extract detailed data
            for index, biz_url in enumerate(business_urls):
                # Update progress bar
                progress = (index + 1) / total_found
                progress_bar.progress(progress)
                status_text.text(f"Scraping lead {index + 1} of {total_found}...")

                page.goto(biz_url, timeout=60000)
                time.sleep(random.uniform(1.5, 2.5)) # Stealth delay

                # Extract Data Points using robust selectors
                name = safe_extract(page, 'h1')
                category = safe_extract(page, 'button[jsaction="pane.rating.category"]')
                address = safe_extract(page, 'button[data-item-id="address"]')
                phone = safe_extract(page, 'button[data-item-id^="phone:tel:"]')
                website = safe_extract(page, 'a[data-item-id="authority"]', attribute="href")
                
                # Rating and Reviews usually sit together in a specific div
                rating_text = safe_extract(page, 'div.F7nice')
                rating = "N/A"
                reviews = "N/A"
                if rating_text != "N/A" and "\n" in rating_text:
                    parts = rating_text.split("\n")
                    rating = parts[0]
                    reviews = parts[1].replace("(", "").replace(")", "")

                # Save to our list
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
            st.error(f"An error occurred during scraping: {e}")
        finally:
            browser.close()

    return leads_data


# --- STREAMLIT WEB INTERFACE ---
st.set_page_config(page_title="Free G-Maps Lead Scraper", page_icon="📍", layout="centered")

st.title("📍 Free Google Maps Lead Generator")
st.markdown("Extract business leads directly from Google Maps. No paid APIs required!")

# Input Fields
col1, col2 = st.columns(2)
with col1:
    keyword = st.text_input("Keyword (e.g., IT Companies, Plumbers)", placeholder="IT Companies")
with col2:
    location = st.text_input("Location (e.g., Dhaka, New York)", placeholder="Dhaka")

max_leads = st.slider("Maximum Leads to Scrape", min_value=5, max_value=50, value=10, step=5)

# Start Button
if st.button("🚀 Start Automation", type="primary"):
    if not keyword or not location:
        st.warning("⚠️ Please enter both a keyword and a location.")
    else:
        # UI Elements for status
        status_text = st.empty()
        progress_bar = st.progress(0)
        
        # Run Scraper
        with st.spinner("Initializing Scraper Engine..."):
            data = scrape_google_maps(keyword, location, max_leads, status_text, progress_bar)
        
        if data:
            status_text.success("🎉 Scraping Complete!")
            
            # Convert to Pandas DataFrame
            df = pd.DataFrame(data)
            
            # Display Data
            st.dataframe(df)
            
            # Download Button
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Leads as CSV",
                data=csv,
                file_name=f"{keyword}_{location}_leads.csv",
                mime="text/csv",
            )
