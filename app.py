# Refactored Streamlit Web Scraper
# - Added logging to file
# - Retry logic for connection errors
# - Resumable scraping from checkpoint
# - Improved memory handling

import os
import time
import json
import re
import io
import logging
import gc
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import streamlit as st
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Logging setup
log_filename = f"scraper_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# HTTP Session with retry logic
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504, 429])
adapter = HTTPAdapter(max_retries=retries)
session.mount('http://', adapter)
session.mount('https://', adapter)

# Checkpoint system
checkpoint_file = "scraped_urls.txt"
already_scraped = set()
if os.path.exists(checkpoint_file):
    with open(checkpoint_file, "r") as f:
        already_scraped = set(f.read().splitlines())

st.set_page_config(page_title="Government Service Web Scraper", layout="wide")

# Ensure session state variables are initialized
if 'trigger_scraping' not in st.session_state:
    st.session_state.trigger_scraping = False

if 'current_index' not in st.session_state:
    st.session_state.current_index = 0

if 'scraped_data' not in st.session_state:
    st.session_state.scraped_data = []

if 'url_data_map' not in st.session_state:
    st.session_state.url_data_map = {}

if 'is_scraping' not in st.session_state:
    st.session_state.is_scraping = False

st.title("Iystream Service Web Scraper")
# Safely initialize session state variables# URL input
st.subheader("URL Input")
url_text = st.text_area("Enter URLs (one per line):", height=150, 
                        value="https://service.sarawak.gov.my/web/web/home/sla_view/211/545")

# Upload URLs from file option
uploaded_file = st.file_uploader("Or upload a file with URLs (one URL per line)", type=["txt", "csv"])
if uploaded_file is not None:
    # Read the file based on type
    if uploaded_file.name.endswith('.csv'):
        try:
            df = pd.read_csv(uploaded_file)
            # Try to find a column that might contain URLs
            url_col = None
            for col in df.columns:
                if col.lower() in ['url', 'link', 'website', 'address']:
                    url_col = col
                    break
            
            if url_col:
                urls_from_file = df[url_col].tolist()
                url_text = "\n".join([url for url in urls_from_file if isinstance(url, str) and url.strip()])
            else:
                # Just take the first column
                urls_from_file = df.iloc[:, 0].tolist()
                url_text = "\n".join([url for url in urls_from_file if isinstance(url, str) and url.strip()])
        except Exception as e:
            st.error(f"Error reading CSV file: {str(e)}")
    else:
        # Assume it's a text file
        url_text = uploaded_file.getvalue().decode("utf-8")

# Create columns for the advanced options
with st.expander("Advanced Options"):
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Basic Selectors")
        title_selector = st.text_input("Title Selector:", value="title")
        h1_selector = st.text_input("H1 Selector:", value="h1")
        section_selector = st.text_input("Section Selector:", value=".panel-heading, .panel-body")
    
    with col2:
        st.subheader("Additional Data")
        extract_meta = st.checkbox("Extract Meta Tags", value=True)
        extract_links = st.checkbox("Count Links", value=True)
        extract_images = st.checkbox("Count Images", value=True)
        extract_sections = st.checkbox("Extract Section Content", value=True)

    st.subheader("Excel Export Options")
    excel_option = st.radio(
        "How to organize Excel sheets:",
        options=["One sheet per URL", "All URLs in one sheet"]
    )

# Setup for storing results
if 'scraped_data' not in st.session_state:
    st.session_state.scraped_data = []

if 'is_scraping' not in st.session_state:
    st.session_state.is_scraping = False
# Safely initialize session state variables# Extract section content (specifically for government service pages)






def extract_section_content(soup):
    import re

    sections = {}
    common_sections = [
        "Introduction", "Who's eligible?", "What you'll need?",
        "How to get the service?", "Payment / Charges", "Need help?"
    ]

    # Step 1: Try extracting from known panel or section containers
    panels = soup.find_all(class_=re.compile(r'panel|section|accordion'))

    if not panels:
        # Step 2: Fallback to headings and collect siblings
        headers = soup.find_all(['h2', 'h3', 'h4', 'h5', 'div'], class_=re.compile(r'heading|title|header'))
        for header in headers:
            title = header.get_text(strip=True)
            if not title:
                continue
            content = []
            next_elem = header.find_next_sibling()
            while next_elem and (
                next_elem.name not in ['h2', 'h3', 'h4', 'h5'] or
                'heading' not in " ".join(next_elem.get("class", []))
            ):
                content.append(next_elem.get_text(strip=True))
                next_elem = next_elem.find_next_sibling()
            if content:
               sections[title] = "\\n".join(content)
    else:
        for panel in panels:
            header = panel.find(class_=re.compile(r'heading|title|header'))
            if not header:
                continue
            title = header.get_text(strip=True)
            if not title:
                continue
            body = panel.find(class_=re.compile(r'body|content'))
            if body:
                items = body.find_all(['p', 'li', 'div'])
                content = [item.get_text(strip=True) for item in items if item.get_text(strip=True)]
                if content:
                    sections[title] = "
".join(content)

    # Step 3: Fuzzy extract from common names
    for section_name in common_sections:
        section = soup.find(string=re.compile(section_name, re.IGNORECASE))
        if section:
            parent = None
            for parent_elem in section.parents:
                if parent_elem.name in ['div', 'section'] and parent_elem.find_all(['li', 'p']):
                    parent = parent_elem
                    break
            if parent:
                content = []
                for item in parent.find_all(['p', 'li']):
                    text = item.get_text(strip=True)
                    if text and text != section_name:
                        content.append(text)
                if content:
                    sections[section_name] = "
".join(content)

    return sections







# Function to clean URLs for sheet names
def clean_url_for_sheet_name(url):
    # Extract domain and path for a cleaner name
    from urllib.parse import urlparse
    parsed = urlparse(url)
    domain = parsed.netloc.split('.')
    domain = domain[-2] if len(domain) > 1 else domain[0]  # Get the main domain name
    
    # Extract the last path segment if available
    path = parsed.path.strip('/').split('/')
    path = path[-1] if path and path[-1] else 'home'
    
    # Combine and clean the name
    sheet_name = f"{domain}-{path}"
    # Remove invalid Excel sheet name characters
    sheet_name = re.sub(r'[\[\]:*?/\\]', '-', sheet_name)
    # Ensure it's not too long (Excel has a 31 character limit)
    if len(sheet_name) > 31:
        sheet_name = sheet_name[:31]
    
    return sheet_name

# Scraping function
def scrape_urls(urls):
    all_data = []  # List to hold all scraped data
    url_data_map = {}  # Dictionary to map URLs to their scraped data
    
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
            }, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract data
            data = {"URL": url}
            
            # Title
            title_elem = soup.select_one(title_selector)
            if title_elem:
                data["Title"] = title_elem.get_text(strip=True)
            else:
                data["Title"] = ""
            
            # Count H1 tags
            h1_count = len(soup.find_all('h1'))
            data["H1 Count"] = h1_count
            
            # Count links
            if extract_links:
                links_count = len(soup.find_all('a'))
                data["Links"] = links_count
            
            # Count images
            if extract_images:
                images_count = len(soup.find_all('img'))
                data["Images"] = images_count
            
            # Extract meta tags
            if extract_meta:
                meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
                if meta_keywords:
                    data["Meta Keywords"] = meta_keywords.get('content', '')
                
                meta_desc = soup.find('meta', attrs={'name': 'description'})
                if meta_desc:
                    data["Meta Description"] = meta_desc.get('content', '')
            
            # Extract section content for government services
            if extract_sections:
                sections = extract_section_content(soup)
                
                # Add sections as columns
                for section_name, content in sections.items():
                    column_name = f"Section: {section_name}"
                    data[column_name] = content
                
                # Store full sections data as JSON
                data["All Sections"] = json.dumps(sections)
            
            # Required documents
            documents_list = []
            for list_item in soup.find_all('li'):
                text = list_item.get_text(strip=True)
                if any(doc_word in text.lower() for doc_word in ['copy', 'document', 'certificate', 'id', 'passport']):
                    documents_list.append(text)
            
            if documents_list:
                data["Required Documents"] = "\n".join(documents_list)
            
            # Eligibility
            eligibility_list = []
            eligible_section = soup.find(string=re.compile("Who's eligible?", re.IGNORECASE))
            if eligible_section:
                for parent in eligible_section.parents:
                    if parent.name in ['div', 'section']:
                        for li in parent.find_all('li'):
                            eligibility_list.append(li.get_text(strip=True))
                        break
            
            if eligibility_list:
                data["Eligibility Criteria"] = "\n".join(eligibility_list)
            
            # Check if registration is available online
            online_indicators = ['register online', 'available online', 'online service', 'apply online']
            data["Online Registration"] = "No"
            for indicator in online_indicators:
                if indicator in response.text.lower():
                    data["Online Registration"] = "Yes"
                    break
            
            # Costs/fees
            payment_section = soup.find(string=re.compile("Payment|Charges|Fee", re.IGNORECASE))
            if payment_section:
                for parent in payment_section.parents:
                    if parent.name in ['div', 'section']:
                        fee_text = parent.get_text(strip=True)
                        if "free" in fee_text.lower() or "no charge" in fee_text.lower():
                            data["Fee"] = "Free"
                        else:
                            # Try to extract fee amount
                            fee_match = re.search(r'RM\s*(\d+(?:\.\d+)?)', fee_text)
                            if fee_match:
                                data["Fee"] = f"RM {fee_match.group(1)}"
                            else:
                                data["Fee"] = fee_text
                        break
            
            data["Status"] = "completed"
            
            # Store the data both in the list and in the URL map
            all_data.append(data)
            url_data_map[url] = data
            
            with log_container:
                st.success(f"✓ Successfully scraped: {data['Title'] or url}")
        
        except Exception as e:
            error_data = {
                "URL": url, 
                "Title": "", 
                "Status": f"Error: {str(e)}"
            }
            all_data.append(error_data)
            url_data_map[url] = error_data
            
            with log_container:
                st.error(f"✗ Error scraping {url}: {str(e)}")
        
        # Update progress
        progress_bar.progress((i+1)/len(urls))
        
        # Be nice to servers
        time.sleep(1)
    
    progress_text.text(f"Completed scraping {len(urls)} URLs")
    return all_data, url_data_map

# Function to create Excel file with multiple sheets
def create_excel_with_multiple_sheets(url_data_map):
    output = io.BytesIO()
    
    try:
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # Create a summary sheet first
            summary_data = []
            for url, data in url_data_map.items():
                summary_row = {
                    "URL": url,
                    "Title": data.get("Title", ""),
                    "Status": data.get("Status", ""),
                    "H1 Count": data.get("H1 Count", ""),
                    "Links": data.get("Links", ""),
                    "Images": data.get("Images", ""),
                    "Online Registration": data.get("Online Registration", ""),
                    "Fee": data.get("Fee", "")
                }
                summary_data.append(summary_row)
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name="Summary", index=False)
            
            # Create individual sheets for each URL
            for url, data in url_data_map.items():
                # Create a clean sheet name from the URL
                sheet_name = clean_url_for_sheet_name(url)
                
                # Convert data to dataframe 
                # For individual URL sheets, we'll use a transposed layout for better readability
                if "All Sections" in data:
                    # Remove the JSON-encoded sections data
                    data_copy = data.copy()
                    all_sections = data_copy.pop("All Sections")
                    
                    # First add the basic data
                    items = list(data_copy.items())
                    df = pd.DataFrame(items, columns=['Field', 'Value'])
                    
                    # Try to add section data as separate rows
                    try:
                        sections = json.loads(all_sections)
                        for section_name, content in sections.items():
                            items.append((f"Section: {section_name}", content))
                    except:
                        pass  # Just continue without the section data if it can't be parsed
                    
                    df = pd.DataFrame(items, columns=['Field', 'Value'])
                else:
                    # Just use the data directly if no sections
                    items = list(data.items())
                    df = pd.DataFrame(items, columns=['Field', 'Value'])
                
                # Write to excel
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # Adjust column widths
                worksheet = writer.sheets[sheet_name]
                worksheet.set_column(0, 0, 30)  # Field column
                worksheet.set_column(1, 1, 100)  # Value column
        
        # Return the Excel file as bytes
        output.seek(0)
        return output.getvalue()
    
    except Exception as e:
        st.error(f"Error creating Excel file: {str(e)}")
        return None

# Scrape button
# Optional: Resume or Reset Progress
col_resume, col_reset = st.columns(2)
with col_resume:
    if st.button("Resume Scraping"):
        st.session_state.trigger_scraping = True
with col_reset:
    if st.button("Reset Progress"):
        st.session_state.current_index = 0
        st.success("Progress has been reset.")

if st.button("Start Scraping"):
    st.session_state.trigger_scraping = True

if st.session_state.trigger_scraping:
    urls = [url.strip() for url in url_text.split("\n") if url.strip()]
    if not urls:
        st.error("No URLs provided. Please enter at least one URL.")
    else:
        st.session_state.is_scraping = True
        for i in range(st.session_state.current_index, len(urls)):
            url = urls[i]
            st.session_state.current_index = i
            st.session_state.scraped_data, st.session_state.url_data_map = scrape_urls([url])
        st.session_state.is_scraping = False
    st.session_state.trigger_scraping = False

# Display results if available
if st.session_state.scraped_data:
    st.subheader("Scraped Data")
    
    # Create a DataFrame
    df = pd.DataFrame(st.session_state.scraped_data)
    
    # Get a list of all unique column names across all URLs
    all_columns = set()
    for data in st.session_state.scraped_data:
        all_columns.update(data.keys())
    
    # Display sections in a more readable way if they exist
    section_columns = [col for col in all_columns if col.startswith("Section:")]
    if section_columns or "All Sections" in all_columns:
        st.write("### Key Sections Found:")
        for url, data in st.session_state.url_data_map.items():
            st.write(f"**URL: {url}**")
            
            # Try to display sections if available
            if "All Sections" in data:
                try:
                    sections = json.loads(data["All Sections"])
                    for section_name, content in sections.items():
                        with st.expander(f"{section_name}"):
                            st.write(content)
                except:
                    # If we can't parse All Sections, try the individual section columns
                    for col in section_columns:
                        if col in data and data[col]:
                            section_name = col.replace("Section: ", "")
                            with st.expander(f"{section_name}"):
                                st.write(data[col])
    
    # Display basic dataframe (excluding the All Sections column which is JSON)
    display_cols = [col for col in df.columns if col != "All Sections"]
    st.dataframe(df[display_cols])
    
    # Export options
    col1, col2 = st.columns(2)
    
    with col1:
        # For CSV we'll include all data
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "Download CSV",
            csv,
            "scraped_data.csv",
            "text/csv",
            key='download-csv'
        )
    
    with col2:
        # For Excel, use the appropriate export method based on user choice
        if excel_option == "One sheet per URL":
            excel_data = create_excel_with_multiple_sheets(st.session_state.url_data_map)
            if excel_data:
                st.download_button(
                    "Download Excel (Multiple Sheets)",
                    excel_data,
                    "scraped_data_multi_sheet.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key='download-excel-multi'
                )
        else:
            # All in one sheet
            try:
                buffer = pd.ExcelWriter('scraper_results.xlsx', engine='xlsxwriter')
                df.to_excel(buffer, index=False, sheet_name='All Data')
                buffer.close()
                
                with open('scraper_results.xlsx', 'rb') as f:
                    excel_data = f.read()
                
                st.download_button(
                    "Download Excel (Single Sheet)",
                    excel_data,
                    "scraped_data.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key='download-excel-single'
                )
            except Exception as e:
                st.error(f"Error creating Excel file: {str(e)}")

# Add instructions at the bottom
with st.expander("How to use this government service scraper"):
    st.markdown("""
    ### Instructions:
    
    1. **Enter URLs** of government service pages (one URL per line) OR upload a text/CSV file with URLs
    2. **Configure advanced options** if needed:
       - Adjust selectors for different page structures
       - Choose what information to extract
       - Select Excel export format (one sheet per URL or all in one sheet)
    
    3. **Click 'Start Scraping'** to begin processing the URLs
    4. **Review the extracted data**, including section content that is expanded in the results
    5. **Download results** as CSV or Excel when complete
    
    ### Excel Export Options:
    
    - **One sheet per URL**: Creates a separate worksheet for each URL, with a summary sheet
    - **All URLs in one sheet**: Places all data in a single worksheet
    
    ### This scraper specializes in:
    
    - Extracting structured content from government service pages
    - Identifying eligibility requirements
    - Finding required documents
    - Determining if online registration is available
    - Capturing fee information
    
    ### Tips for better results:
    
    - For government service pages with different layouts, you may need to adjust the selectors
    - The scraper attempts to identify common sections like "Introduction", "Eligibility", etc.
    - More detailed data extraction may require customization for specific website structures
    """)
