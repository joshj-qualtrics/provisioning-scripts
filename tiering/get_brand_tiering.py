from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait 
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
import time
# from staging.q4_2024_staging_arrays import array1, array2, array3, array4, array5, array6
from staging.feb_march_2025 import array1, array2, array3, array4, array5
from scripts.scrape_from_ticket import scrape_products_from_ticket, scrape_errors_from_ticket
import json

def scrape_odo_brand_info(driver, brand):
    """
    Scrapes brand information from the Qualtrics API endpoint using Selenium.
    
    Args:
        driver: The Selenium WebDriver instance.
        brand: The brand name to query.
    
    Returns:
        The parsed JSON data if successful, otherwise None.
    """
    # print(f"Scraping information for brand: {brand}")
    
    # Navigate to the specific brand URL
    driver.get(f'https://odo-public-api.corp.qualtrics.com/odo-api/brand/{brand}')
    
    try:
        # Wait a moment for the page to load, then find the <pre> tag
        time.sleep(2) 
        pre_element = driver.find_element(By.TAG_NAME, 'pre')
        
        # Get the text content from the <pre> tag
        json_text = pre_element.text
        
        # Parse the text as JSON and return it
        data = json.loads(json_text)
        # print(f"✅ Successfully scraped data for brand {brand}")
        print(data)
        return data

    except NoSuchElementException:
        print(f'❌ No information found for brand {brand}')
        return None
    except json.JSONDecodeError:
        print(f'❌ Failed to parse JSON for brand {brand}')
        return None


