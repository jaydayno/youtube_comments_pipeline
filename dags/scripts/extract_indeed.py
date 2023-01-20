# from airflow.hooks.S3_hook import S3Hook
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from itertools import chain
import time


import pathlib
from dotenv import dotenv_values

def scroll_down(selenium_driver):
    """ Scroll down until there are 100 jobs """
    job_cards = driver.find_elements(By.XPATH, "//*[@class='base-search-card__title']")

    while len(job_cards) < 100:
        # Scroll down to the bottom.
        selenium_driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        # Wait to load the page.
        time.sleep(2)
        # Count new jobs
        job_cards = driver.find_elements(By.XPATH, "//*[@class='base-search-card__title']")

script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f"{script_path}/configuration.env")

chrome_options = Options()
chrome_options.add_argument("--headless") # Ensure GUI is off
chrome_options.add_argument("--no-sandbox")

webdriver_service = Service("/usr/bin/chromedriver")
driver = webdriver.Chrome(service=webdriver_service,options=chrome_options)

## Search: data engineer, location: Canada
url = 'https://www.linkedin.com/jobs/search/?keywords=data%20engineer&location=Canada&refresh=true' + '&start=0'

driver.get(url)
WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//*[@class='base-search-card__title']")))
scroll_down(driver)
## Initializing Lists
full_jobs_titles = []

page_jobs = driver.find_elements(By.XPATH, "//*[@class='base-search-card__title']")
for i_jobs in page_jobs:
    full_jobs_titles.append(i_jobs.text)

print(full_jobs_titles)
print(len(full_jobs_titles))

driver.quit()