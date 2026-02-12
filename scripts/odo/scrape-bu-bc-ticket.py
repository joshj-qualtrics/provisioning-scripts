from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC
import time
# from staging.q4_2024_staging_arrays import array1, array2, array3, array4, array5, array6
from staging.feb_march_2025 import array1, array2, array3, array4, array5
from scripts.scrape_from_ticket import scrape_products_from_ticket, scrape_errors_from_ticket, scrape_brand_and_datacenter_from_ticket
from tiering.get_brand_tiering import scrape_odo_brand_info

driver = webdriver.Chrome()
driver.get(f'https://odo.corp.qualtrics.com/?TopNav=Tickets&a=Tickets&b=TicketViewer&tid=')


time.sleep(60)
# Scrape Errors
# with open("input_tickets.txt", "r") as infile, open("results.txt", "w") as outfile:
#     for line in infile:
#         ticket_id = line.strip()
#         if ticket_id:
#             ticket_errors = scrape_errors_from_ticket(driver, ticket_id)
#             outfile.write(str(ticket_errors) + "\n")

# Scrape Products
# with open("input_tickets.txt", "r") as infile, open("results.txt", "w") as outfile:
#     for line in infile:
#         ticket_id = line.strip()
#         if ticket_id:
#             array_of_products = scrape_products_from_ticket(driver, ticket_id)
#             outfile.write(str(array_of_products) + "\n")

# Scrape Brand ID and DC
with open("input_tickets.txt", "r") as infile, open("results.txt", "w") as outfile:
    for line in infile:
        ticket_id = line.strip()
        if ticket_id:
            driver.get(f'https://odo.corp.qualtrics.com/?TopNav=Tickets&a=Tickets&b=TicketViewer&tid={ticket_id}')
            return_obj = scrape_brand_and_datacenter_from_ticket(driver, ticket_id)
            if return_obj:
                row = {
                    "ticket_id": ticket_id,
                    "brandid": return_obj["brandid"],
                    "datacenter": return_obj["datacenter"]
                }
                print(f'Successfully scraped ticket {ticket_id}')
            outfile.write(str(ticket_id) + "|" + str(row["brandid"]) + "|" + str(row["datacenter"]) + "\n")




driver.close()

