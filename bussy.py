#!/usr/bin/env python
# coding: utf-8

from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup as bs
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import UnicodeText, Integer, DateTime
import datetime
from selenium.common.exceptions import TimeoutException, WebDriverException

# Configure Chrome options
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--ignore-certificate-errors")
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("disable-infobars")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--no-sandbox")

# Set up Chrome WebDriver
s = Service('C:/Users/saida/OneDrive/Desktop/Archive/chromedriver-win64/chromedriver.exe')
driver = webdriver.Chrome(service=s)

# URL of the target website
url = 'https://www.hellojob.az'
driver.get(url)

# Lists to store scraped data
links_list = []
positions_list = []
companies_list = []
salary_list = []
jobdescr_list = []
deadline_list = []

# Iterate over pages
for page_num in range(1, 11):  # Assuming 10 pages, you can adjust as needed
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'vacancies__item')))
        soup = bs(driver.page_source, "html.parser")
    except TimeoutException as e:
        print(f"TimeoutException: {e}. Skipping page {page_num}.")
        continue

    positions = soup.find_all('a', attrs={'class': 'vacancies__item'})

    for position in positions:
        try:
            # Extract job name and company
            job_desc_element = position.find('div', class_='vacancies__desc')
            job_name_element = job_desc_element.find('h3') if job_desc_element else None
            company_name_element = job_desc_element.find('p') if job_desc_element else None

            # Check if job_name_element and company_name_element are not None
            if job_name_element and company_name_element:
                job_name = job_name_element.text.strip()
                company_name = company_name_element.text.strip()
                links_list.append('https://www.hellojob.az' + position['href'])

                # Visit the vacancy page
                driver.get(links_list[-1])  # Open the link to get the details
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'resume__block')))
                vacancy_soup = bs(driver.page_source, "html.parser")

                # Extract job description
                # Extract job description from the third block
                job_desc_elements = vacancy_soup.find_all('div', class_='resume__block')

# Check if there is at least one element
                if job_desc_elements:
    # Get the text from the third block (index 2)
                   third_block_text = job_desc_elements[2].text.strip()
                   jobdescr_list.append(third_block_text)
                else:
    # Handle the case where no 'resume__block' elements are found
                  jobdescr_list.append(None)  # You can replace None with any default value or handle it as needed


                # Extract wage information from the vacancy page
                salam = vacancy_soup.find('div', class_='resume__item')
                wage_element=  salam.find_all('div')[1]
                # wage_element = vacancy_soup.find('div', class_='resume__item__text')
                wage_text = wage_element.find_all('h4')[0].text.strip() if wage_element else None
                salary_list.append(wage_text)

                # Extract deadline information from the vacancy page
                deadline_element = vacancy_soup.find('div', class_='resume__item__text')
                deadline_text = deadline_element.text.strip() if deadline_element else None
                deadline_list.append(deadline_text)

                # Append job name and company to lists
                positions_list.append(job_name)
                companies_list.append(company_name)

        except (TimeoutException, WebDriverException) as e:
            print(f"Exception: {e}. Skipping vacancy page.")
        except Exception as e:
            print(f"Error: {e}. Skipping vacancy page.")

# Quit the WebDriver
driver.quit()

# Create DataFrame
dataframe_banco = pd.DataFrame({
    'job': positions_list,
    'company': companies_list,
    'job_descr': jobdescr_list,
    'wage': salary_list,
    'deadline': deadline_list,
    'links': links_list,
    'vip': [0] * len(links_list)
})

# Convert date columns to datetime format
dataframe_banco['deadline'] = pd.to_datetime(dataframe_banco['deadline'], format='%d-%m-%Y', errors='coerce')


# Database connection parameters
username = 'root'
password = ''
host = 'localhost'
port = 3306
DB_NAME = 'emp_az'

# Create SQLAlchemy engine
engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{DB_NAME}?charset=utf8mb4")

# Connect to the database
with engine.connect() as conn:
    # Drop existing table if it exists
    conn.execute(text("DROP TABLE IF EXISTS hellojob"))

    # Write DataFrame to MySQL database
    dataframe_banco.to_sql(name='hellojob', con=conn, if_exists='append', chunksize=300, index=False,
                           dtype={'job': UnicodeText(), 'company': UnicodeText(), 'job_descr': UnicodeText(),
                                  'wage': UnicodeText(), 'deadline': DateTime(), 'links': UnicodeText(),
                                  'vip': Integer()})

    # Add primary key column
    conn.execute(text("ALTER TABLE hellojob ADD id INT PRIMARY KEY AUTO_INCREMENT FIRST;"))

# Dispose of the engine
engine.dispose()
