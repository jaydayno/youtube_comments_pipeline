# from airflow.hooks.S3_hook import S3Hook
# %%
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
    """A method for scrolling the page."""

    # Get scroll height.
    last_height = selenium_driver.execute_script("return document.body.scrollHeight")

    while True:

        # Scroll down to the bottom.
        selenium_driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        # Wait to load the page.
        time.sleep(2)

        # Calculate new scroll height and compare with last scroll height.
        new_height = selenium_driver.execute_script("return document.body.scrollHeight")

        if new_height == last_height:

            break

        last_height = new_height

# %%
script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f"{script_path}/configuration.env")

chrome_options = Options()
chrome_options.add_argument("--headless") # Ensure GUI is off
chrome_options.add_argument("--no-sandbox")

webdriver_service = Service("/usr/bin/chromedriver")
driver = webdriver.Chrome(service=webdriver_service,options=chrome_options)

url = 'https://www.linkedin.com/login?fromSignIn=true&session_redirect=https%3A%2F%2Fca.linkedin.com%2Fjobs&trk=guest_homepage-jobseeker_nav-header-signin'

driver.get(url)
## Log In to LinkedIn
WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.ID, "username")))
username_field = driver.find_element(By.ID, 'username')
username_field.send_keys(config['linkedin_login'])
pw_field = driver.find_element(By.ID, 'password')
pw_field.send_keys(config['linkedin_pw'])
sign_button = driver.find_element(By.XPATH, "//div[button/@data-litms-control-urn='login-submit']")
sign_button.click()

## Initializing Lists
full_jobs_titles = []

## Search: data engineer, location: Canada
WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.ID, "main")))
driver.get('https://www.linkedin.com/jobs/search/?keywords=data%20engineer&location=Canada&refresh=true' + '&start=0')
scroll_down(driver)
print(driver.current_url)
WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//ul[@class='scaffold-layout__list-container']/li")))
page_jobs_titles = driver.find_elements(By.XPATH, "//ul[@class='scaffold-layout__list-container']/li")
print(full_jobs_titles)
# for i_job_title in page_jobs_titles:
#     full_jobs_titles.append(i_job_title.text)
# print(full_jobs_titles)


# job_cards = []
# for i in range(4):
#     start_part = 25 * i
#     WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.ID, "main")))
#     driver.get('https://www.linkedin.com/jobs/search/?keywords=data%20engineer&location=Canada&refresh=true' + f'&start={start_part}')
#     WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//ul[@class='scaffold-layout__list-container']/li")))
#     scroll_down(driver)
#     page_jobs = driver.find_elements(By.XPATH, "//ul[@class='scaffold-layout__list-container']/li")
#     job_cards.append(page_jobs)

# whole_job_list = []
# while job_cards:
#     whole_job_list.extend(job_cards.pop(0))
# print(whole_job_list, len(whole_job_list))

driver.quit()
# %%