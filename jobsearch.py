#!/usr/bin/env python
# coding: utf-8

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import numpy as np
from selenium.webdriver.common.by import By
import pandas as pd
from bs4 import BeautifulSoup as bs
import time

# Set up Chrome options
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("disable-infobars")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--ignore-certificate-errors")

# Set up Chrome driver
s = Service('C:/Users/saida/OneDrive/Desktop/Archive/chromedriver-win64/chromedriver.exe')
driver = webdriver.Chrome(service=s)

# Lists to store data
job = []
job_descr = []
company_name = []
deadline = []
posted = []
links = []
city = []  # You have city, wage, job_type lists, but they are not populated in the code
wage = []
job_type = []

# Navigate to the job search page
driver.get('https://jobsearch.az/vacancies')
driver.implicitly_wait(999)
time.sleep(5)

# Scroll down to load more job listings
target = driver.find_element(by=By.XPATH, value='//*[@id="scroller_desctop"]')
previous_height = driver.execute_script('return arguments[0].scrollTop = arguments[0].scrollHeight', target)
time.sleep(5)

count = 0
while count < 50:
    driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', target)
    time.sleep(5)
    new_height = driver.execute_script('return arguments[0].scrollTop = arguments[0].scrollHeight', target)

    if previous_height == new_height:
        break
    previous_height = new_height
    count += 1

print(count)

# Parse the page source with BeautifulSoup
soup1 = bs(driver.page_source, "html.parser")

# Extract job details
for i in soup1.find_all('h3', attrs={'class': 'list__item__title'}):
    job.append(i.text.strip())

for i in soup1.find_all('a', attrs={'class': 'list__item__text'}):
    companyName = str(i).split("</h3>")[1].split("\n")[1].strip()
    links.append(f"https://jobsearch.az{i['href']}")
    company_name.append(companyName)

for element in links:
    time.sleep(1)
    try:
        driver.get(element)
        soup = bs(driver.page_source, "html.parser")
        job_descrr = driver.find_element(by=By.XPATH, value='//*[@id="description"]').text
        job_descr.append(job_descrr)
        deadlinee = driver.find_element(by=By.XPATH,
                                        value='//*[@class="vacancy__deadline"]').text.replace('Son tarix ',
                                                                                            '').replace('okt',
                                                                                                        'October').replace(
            'noy', 'November')
        deadline.append(deadlinee)

    except:
        print('yes')

driver.quit()

# Create a DataFrame
vac = pd.DataFrame({'job': pd.Series(job), 'job_descr': pd.Series(job_descr), 'company': pd.Series(company_name),
                    'city': pd.Series(city), 'job_type': pd.Series(job_type),
                    'wage': pd.Series(wage), 'posted': pd.Series(posted), 'deadline': pd.Series(deadline),
                    'links': pd.Series(links),
                    'vip': pd.Series(np.zeros(shape=len(job), dtype=int))})

# Update date format
months = {'Yanvar': 'January',
          'Fevral': 'February',
          'Mart': 'March',
          'Aprel': 'April',
          'May': 'May',
          'İyun': 'June',
          'İyul': 'July',
          'Avqust': 'August',
          'Sentyabr': 'September',
          'Oktyabr': 'October',
          'Noyabr': 'November',
          'Dekabr': 'December'}

# Assuming vac.deadline and vac.posted are Series containing date strings
vac.deadline = vac.deadline.replace(months, regex=True)

# Check if 'posted' column has non-empty values before applying the replacement
non_empty_posted = vac.posted[vac.posted.notna()]
if not non_empty_posted.empty:
    vac.posted.loc[vac.posted.notna()] = non_empty_posted.replace(months, regex=True)

# Convert to datetime
vac.posted = pd.to_datetime(vac.posted, format='%B %d, %Y')

# Connect to MySQL and save the DataFrame to the database
from sqlalchemy import create_engine

username = 'root'
password = ''
host = 'localhost'
port = 3306
DB_NAME = 'emp_az'

engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{DB_NAME}?charset=utf8mb4")

with engine.connect() as conn:
    # Use the database
    conn.execute("USE emp_az")

    # Drop the table if it exists
    conn.execute("DROP TABLE IF EXISTS jobsearch")

    # Save the DataFrame to MySQL
    vac.to_sql(name='jobsearch', con=conn, if_exists='append', chunksize=300, index=False,
               dtype={'job': 'TEXT', 'job_descr': 'TEXT', 'company': 'TEXT', 'city': 'TEXT',
                      'job_type': 'TEXT', 'wage': 'INTEGER', 'posted': 'DATETIME', 'deadline': 'TEXT',
                      'links': 'TEXT', 'vip': 'INTEGER'})

    # Add an id column as the primary key
    conn.execute("ALTER TABLE jobsearch ADD id INT PRIMARY KEY AUTO_INCREMENT FIRST;")

# Dispose the engine
engine.dispose()
