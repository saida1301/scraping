#!/usr/bin/env python
# coding: utf-8

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine, text
import datetime
import time

def extract_company(description):
    keywords = ['təşkilatı', 'şirkəti', 'korporasiyası', 'korporasiya', 'kompaniyası', 'firması', 'kompaniya']
    words = description.split()

    for keyword in keywords:
        if keyword in words:
            index = words.index(keyword)
            return ' '.join(words[:index])

    return None

# Set up Chrome options
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("disable-infobars")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--enable-javascript")
chrome_options.add_argument("--ignore-certificate-errors")

# Initialize Chrome driver
s = Service('C:/Users/saida/OneDrive/Desktop/Archive/chromedriver-win64/chromedriver.exe')
driver = webdriver.Chrome(service=s, options=chrome_options)

# Initialize lists to store data
job = []
job_descr = []
company_name = []
city = []
job_type = []
wage = []
deadline = []
links = []
posted = []

# Web scraping logic
driver.get('https://edumap.az/kateqoriya/vakansiyalar')
driver.implicitly_wait(15)

time.sleep(2)
soup_pag = bs(driver.page_source, "html.parser")
last_p = soup_pag.find('li', attrs={'class': 'page-item'}).find_next('a')['href'].split('/')[-2]
pages = np.arange(1, 10)

for p in pages:
    url = []
    driver.get(f'https://edumap.az/kateqoriya/vakansiyalar?page={str(p)}')
    time.sleep(0.5)
    soup = bs(driver.page_source, "html.parser")
    div = soup.find_all('h2', attrs={'class': 'utf_post_title title-large'})
    for i in div:
        links.append('' + i.a['href'])
        job.append(i.text)

for link in links:
    time.sleep(0.5)
    driver.get(link)
    soup_pag = bs(driver.page_source, "html.parser")
    job_descr.append(soup_pag.find('div', attrs={'id': 'yazilar'}).text.strip())
    date = soup_pag.find('span', attrs={'class': 'utf_post_date'})
    posted.append(date.text.strip())
    deadline.append(None)

# Close the Chrome driver
driver.quit()

# Create DataFrame
vac = pd.DataFrame({
    'job': pd.Series(job),
    'job_descr': pd.Series(job_descr),
    'company': pd.Series(company_name),
    'city': pd.Series(city),
    'job_type': pd.Series(job_type),
    'wage': pd.Series(wage),
    'posted': pd.Series(posted),
    'deadline': pd.Series(deadline),
    'links': pd.Series(links),
    'vip': pd.Series(np.zeros(shape=len(job), dtype=int))
})

# Extract company names
vac['company'] = vac['job_descr'].apply(extract_company)

# Map months dictionary
months = {'Yanvar': 'January', 'Fevral': 'February', 'Mart': 'March', 'Aprel': 'April', 'May': 'May',
          'İyun': 'June', 'İyul': 'July', 'Avqust': 'August', 'Sentyabr': 'September', 'Oktyabr': 'October',
          'Noyabr': 'November', 'Dekabr': 'December'}

# Replace months in the 'posted' column
vac['posted'] = vac['posted'].replace(months, regex=True)

# Convert 'posted' column to datetime
vac['posted'] = pd.to_datetime(vac['posted'], errors='coerce')

# Filter rows based on the posted date
# Filter rows based on the posted date (excluding NaT values)
vac = vac[vac['posted'].notna() & (vac['posted'].apply(lambda x: (datetime.date.today() - x.date()).days < 31))]


# MySQL database connection parameters
username = 'your_username'
password = 'your_password'
host = 'your_host'
port = 3306
db_name = 'emp_az'

# Create the MySQL engine
engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{db_name}?charset=utf8mb4", pool_pre_ping=True)

# Use a context manager to safely handle the connection
with engine.connect() as conn:
    # Drop the table if it exists
    conn.execute(text("DROP TABLE IF EXISTS edumap"))

    # Insert data into the 'edumap' table
    vac.to_sql(name='edumap', con=conn, if_exists='replace', index=False,
               dtype={'job': pd.StringDtype(), 'job_descr': pd.StringDtype(), 'company': pd.StringDtype(),
                      'city': pd.StringDtype(), 'job_type': pd.StringDtype(), 'wage': pd.Int32Dtype(),
                      'posted': pd.DatetimeTZDtype(), 'deadline': pd.DatetimeTZDtype(), 'links': pd.StringDtype(),
                      'vip': pd.Int32Dtype()})

    # Add an auto-increment primary key 'id' column to the 'edumap' table
    conn.execute(text("ALTER TABLE edumap ADD id INT PRIMARY KEY AUTO_INCREMENT FIRST"))

# Dispose the engine to release resources
engine.dispose()
