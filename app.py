import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import streamlit as st

st.set_page_config(page_title="Web Scraper MVP", layout="wide")
st.title("Web Scraper MVP")

# URL input
st.subheader("URL Input")
url_text = st.text_area("Enter URLs (one per line):", height=150)

# Create columns for the selector inputs
col1, col2 = st.columns(2)

# Selector inputs
with col1:
    st.subheader("Data Selectors")
    title_selector = st.text_input("Title Selector:", value="h1")
    desc_selector = st.text_input("Description Selector:", value="meta[name='description']")

with col2:
    st.subheader("Additional Selectors")
    price_selector = st.text_input("Price Selector:", value=".price")
    custom_selector = st.text_input("Custom Selector:")
    custom_name = st.text_input("Custom Field Name:", value="Custom Data")

# Setup for storing results
if 'scraped_data' not in st.session_state:
    st.session_state.scraped_data = []

if 'is_scraping' not in st.session_state:
    st.session_state.is_scraping = False

# Scraping function
def scrape_urls(urls):
    scraped_data = []
    progress_text = st.empty()
    progress_bar = st.progress(0)
    log_container = st.container()
    
    for i, url in enumerate(urls):
        progress_text.text(f"Processing {i+1}/{len(urls)}: {url}")
        
        # Check URL format
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        try:
            # Fetch and parse page
            response = requests.get(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract data
            data = {"URL": url}
            
            # Title
            title_elem = soup.select_one(title_selector)
            if title_elem:
                if title_elem.name == "meta":
                    data["Title"] = title_elem.get("content", "")
                else:
                    data["Title"] = title_elem.get_text(strip=True)
            else:
                data["Title"] = ""
            
            # Description
            desc_elem = soup.select_one(desc_selector)
            if desc_elem:
                if desc_elem.name == "meta":
                    data["Description"] = desc_elem.get("content", "")
                else:
                    data["Description"] = desc_elem.get_text(strip=True)
            else:
                data["Description"] = ""
            
            # Price
            price_elem = soup.select_one(price_selector)
            if price_elem:
                data["Price"] = price_elem.get_text(strip=True)
            else:
                data["Price"] = ""
            
            # Custom field
            if custom_selector:
                custom_elem = soup.select_one(custom_selector)
                if custom_elem:
                    if custom_elem.name == "meta":
                        data[custom_name] = custom_elem.get("content", "")
                    else:
                        data[custom_name] = custom_elem.get_text(strip=True)
                else:
                    data[custom_name] = ""
            
            scraped_data.append(data)
            with log_container:
                st.success(f"✓ Successfully scraped: {data['Title'] or url}")
        
        except Exception as e:
            with log_container:
                st.error(f"✗ Error scraping {url}: {str(e)}")
            scraped_data.append({"URL": url, "Title": "", "Description": "", "Price": "", "Error": str(e)})
        
        # Update progress
        progress_bar.progress((i+1)/len(urls))
        
        # Be nice to servers
        time.sleep(1)
    
    progress_text.text(f"Completed scraping {len(urls)} URLs")
    return scraped_data

# Scrape button
scrape_button = st.button("Start Scraping", disabled=st.session_state.is_scraping)

if scrape_button:
    urls = [url.strip() for url in url_text.split("\n") if url.strip()]
    if not urls:
        st.error("No URLs provided. Please enter at least one URL.")
    else:
        st.session_state.is_scraping = True
        st.session_state.scraped_data = scrape_urls(urls)
        st.session_state.is_scraping = False

# Display results if available
if st.session_state.scraped_data:
    st.subheader("Scraped Data")
    df = pd.DataFrame(st.session_state.scraped_data)
    st.dataframe(df)
    
    # Export options
    col1, col2 = st.columns(2)
    
    with col1:
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "Download CSV",
            csv,
            "scraped_data.csv",
            "text/csv",
            key='download-csv'
        )
    
    with col2:
        buffer = pd.ExcelWriter('scraper_results.xlsx', engine='xlsxwriter')
        df.to_excel(buffer, index=False, sheet_name='Scraped Data')
        buffer.close()
        
        with open('scraper_results.xlsx', 'rb') as f:
            excel_data = f.read()
        
        st.download_button(
            "Download Excel",
            excel_data,
            "scraped_data.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key='download-excel'
        )

# Add instructions at the bottom
with st.expander("How to use this scraper"):
    st.markdown("""
    ### Instructions:
    
    1. **Enter URLs** in the text area above (one URL per line)
    2. **Configure selectors**:
       - Title Selector: CSS selector for the page title (default: `h1`)
       - Description Selector: CSS selector for descriptions (default: `meta[name='description']`)
       - Price Selector: CSS selector for prices (default: `.price`)
       - Custom Selector: Any additional element you want to capture
       - Custom Field Name: The name to give this custom data in the results
    
    3. **Click 'Start Scraping'** to begin processing the URLs
    4. **Download results** as CSV or Excel when complete
    
    ### Selector Examples:
    
    - `h1` - First level heading
    - `.product-price` - Element with class "product-price"
    - `#main-content` - Element with ID "main-content"
    - `div.description p` - Paragraph inside div with class "description"
    - `meta[property='og:title']` - Meta tag with property "og:title"
    """)
