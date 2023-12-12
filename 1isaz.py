#!/usr/bin/env python
# coding: utf-8

from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup as bs
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.types import UnicodeText, Integer, DateTime
import datetime

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
url = 'https://1is.az/allvacancy'
driver.get(url)

# Lists to store scraped data
links_list = []
positions_list = []
companies_list = []
salary_list = []
adress_list = []
jobtype_list = []
jobdescr_list = []
deadline_list = []
posted_list = []

# Iterate over pages
for page_num in range(1, 11):  # Assuming 10 pages, you can adjust as needed
    driver.get(f'https://1is.az/allvacancy?page={page_num}')
    soup = bs(driver.page_source, "html.parser")
    positions = soup.find_all('div', attrs={'class': 'vac-card'})

    for position in positions:
        # Extract job name
        job_name = position.find('a', class_='vac-name').text
        positions_list.append(job_name)

        # Extract company name
        company_name = position.find('a', class_='comp-link').text
        companies_list.append(company_name)

        # Extract vacancy link
        vacancy_link = position.a['href']
        links_list.append(vacancy_link)

        # Visit the vacancy page to get details
        driver.get(vacancy_link)
        vacancy_soup = bs(driver.page_source, "html.parser")

        # Extract job description information from the vacancy page
        job_desc_element = vacancy_soup.find('div', class_='position-instructions-container')
        job_desc_text = '\n'.join([li.text.strip() for li in job_desc_element.find_all('li')]) if job_desc_element else None
        jobdescr_list.append(job_desc_text)

        # Extract job type information from the vacancy page
        job_type_element = vacancy_soup.find('div', class_='box-info-text1')
        job_type_text = job_type_element.find_all('p')[1].text.strip() if job_type_element else None
        jobtype_list.append(job_type_text)

        # Extract wage information from the vacancy page
        wage_element = vacancy_soup.find('div', class_='box-info-text6')
        wage_text = wage_element.find_all('p')[1].text.strip() if wage_element else None
        salary_list.append(wage_text)

        # Extract deadline information from the vacancy page
        deadline_element = vacancy_soup.find('div', class_='box-info-text7')
        deadline_text = deadline_element.find_all('p')[1].text.strip() if deadline_element else None
        deadline_list.append(deadline_text)

        # Extract posted time from the vacancy page if needed
        posted_element = position.find('div', class_='vac-inn2')
        posted_text = posted_element.find('p', class_='vac-time').text.strip() if posted_element else None
        posted_list.append(posted_text)

# Quit the WebDriver
driver.quit()

# Create DataFrame
dataframe_banco = pd.DataFrame({
    'job': pd.Series(positions_list, dtype='object'),
    'job_descr': pd.Series(jobdescr_list, dtype='object'),
    'company': pd.Series(companies_list),
    'city': pd.Series(adress_list),
    'job_type': pd.Series(jobtype_list),
    'wage': pd.Series(salary_list),
    'posted': pd.Series(posted_list),
    'deadline': pd.Series(deadline_list),
    'links': pd.Series(links_list),
    'vip': pd.Series(np.zeros(shape=len(positions_list), dtype=int))
})

# Convert date columns to datetime format
dataframe_banco['deadline'] = pd.to_datetime(dataframe_banco['deadline'], format='%d-%m-%Y', errors='coerce')
dataframe_banco['posted'] = pd.to_datetime(dataframe_banco['posted'], format='%d-%m-%Y', errors='coerce')

# Filter data based on the deadline
current_date = datetime.datetime.today()
filtered_data = dataframe_banco[dataframe_banco['deadline'] > current_date]

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
    # Create or use the database
    result = conn.execute(text(f"USE {DB_NAME}"))

    # Drop existing table if it exists
    conn.execute(text("DROP TABLE IF EXISTS recruit"))

    # Write DataFrame to MySQL database
    dataframe_banco.to_sql(name='recruit', con=conn, if_exists='append', chunksize=300, index=False,
                           dtype={'job': UnicodeText(), 'job_descr': UnicodeText(), 'company': UnicodeText(),
                                  'city': UnicodeText(), 'job_type': UnicodeText(), 'wage': Integer(),
                                  'posted': DateTime(), 'deadline': DateTime(), 'links': UnicodeText(),
                                  'vip': Integer()})

    # Add primary key column
    conn.execute(text("ALTER TABLE recruit ADD id INT PRIMARY KEY AUTO_INCREMENT FIRST;"))

# Dispose of the engine
engine.dispose()
