from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
# No need to import the full webdriver or keys here, as they are passed in from the main script

def scrape_dc_from_odo_brand_page(driver, brand_id):
    """
    Scrapes the Brand ID and Data Center values from the ODO Brand Profile page.

    It uses robust XPath expressions targeting the content structure: 
    //div[@class='Box']/div[@class='BoxHeader']...
    """
    
    # Define generic XPath patterns based on the Box/BoxHeader structure
    def get_value_from_box(header_text):
        """
        Locates the container box by its header text, then finds the content div 
        inside it. 
        """
        box_xpath = f"//div[@class='Box' and .//div[@class='BoxHeader'][text()='{header_text}']]"
        

        if header_text == "Brand ID":
            return driver.find_element(By.XPATH, f"{box_xpath}//div[contains(@style, '24pt')]")
        
        if header_text == "Data Center":
            return driver.find_element(By.XPATH, f"{box_xpath}//div[last()]")

        return None

    try:
        brandid_element = get_value_from_box("Brand ID")
        datacenter_element = get_value_from_box("Data Center")
        
        if not brandid_element or not datacenter_element:
            raise NoSuchElementException("One or both required elements (Brand ID or Data Center) were not found.")

        print({
            "brandid": brandid_element.text.strip(),
            "datacenter": datacenter_element.text.strip()
        })

        return {
            "brandid": brandid_element.text.strip(),
            "datacenter": datacenter_element.text.strip()
        }
    
    except NoSuchElementException:
        print(f'No Brand ID or Data Center Information found for brand {brand_id}')
        return None
    except Exception as e:
        print(f"An unexpected error occurred while scraping brand {brand_id}: {e}")
        return None