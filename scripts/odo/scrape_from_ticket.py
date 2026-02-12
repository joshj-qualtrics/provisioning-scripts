from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
import time


def scrape_products_from_ticket_array(driver, array_of_ticket_ids):
    for ticket_id in array_of_ticket_ids:
        product_array_of_ticket = scrape_products_from_ticket(driver, ticket_id)
        print(f'{ticket_id}: ', product_array_of_ticket)


def scrape_brand_and_datacenter_from_ticket(driver, ticket_id):
    try:
        datacenter_element = driver.find_element('xpath', '//td[text() = "Data Center:"]/following-sibling::td')
        brandid_element = driver.find_element('xpath', '//td[text() = "Requested Brand ID:"]/following-sibling::td')
        return {
            "brandid": brandid_element.text,
            "datacenter": datacenter_element.text
        }
    except NoSuchElementException:
        print(f'No Datacenter Information found for ticket {ticket_id}')


def scrape_products_from_ticket(driver, ticket_id):
    products_array = []
    try:
        license_label = driver.find_element('xpath', '//*[text() = "License Information"]')
    except NoSuchElementException:
        print(f'No License Information found for ticket {ticket_id}')
        return products_array  # empty list

    try:
        license_information_div = license_label.find_element('xpath', '..')
        tbodies = license_information_div.find_elements(By.TAG_NAME, 'tbody')
        for tbody in tbodies:
            tds = tbody.find_elements(By.TAG_NAME, 'td')
            product_obj = {}
            key = None
            for i, td in enumerate(tds):  
                if i % 2 == 0:
                    key = td.text
                else:
                    product_obj[f'{key}'] = td.text
            products_array.append(product_obj)
    except Exception as e:
        print(f'Error thrown on {ticket_id}: {e}')

    return products_array


def scrape_errors_from_ticket(driver, ticket_id):
    error_array = []
    # try:
    time.sleep(2)
    audit_table = driver.find_element(By.CLASS_NAME, ("audit-log-table"))
    trs = audit_table.find_elements(By.TAG_NAME, 'tr')
    for tr in trs:
        text = tr.text
        error_array.append(text)
    
    # print(ticket_id, error_array)
    # except:
    #     print(f'Error thrown on {ticket_id}')

    return error_array