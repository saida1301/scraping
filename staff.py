#!/usr/bin/env python
# coding: utf-8

# In[2]:


# !/usr/bin/env python
# coding: utf-8

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import numpy as np
import re
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("disable-infobars")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--enable-javascript")
# chrome_options.add_argument('--headless')
# chrome_options.add_argument("--window-size=1920x1080")
chrome_options.add_argument("--ignore-certificate-errors")

from datetime import datetime
from dateutil.relativedelta import relativedelta
from selenium.common.exceptions import NoSuchElementException


import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import time

job = []  # +
job_descr = []  # +
company_name = []  # +
city = []  # +
job_type = []  # -
wage = []  # +
deadline = []
links = []  # +
posted = []  # +
s = Service('C:/Users/saida/OneDrive/Desktop/Archive/chromedriver-win64/chromedriver.exe')
driver = webdriver.Chrome(service=s)

driver.get('https://staffy.az/jobs?page=1')
driver.implicitly_wait(5)

time.sleep(2)
soup1 = bs(driver.page_source, "html.parser")
pg = []
for i in soup1.find_all('button', attrs={
    'class': 'MuiButtonBase-root MuiPaginationItem-root MuiPaginationItem-page MuiPaginationItem-rounded MuiPaginationItem-textPrimary MuiPaginationItem-sizeLarge'}):
    pg.append(i.text)
pg = pg[-2]
n = int(pg)

pages = np.arange(1, 31)
print(pages)
for p in pages:
    url = []
    driver.get('https://staffy.az/jobs?page=' + str(p))
    time.sleep(1)
    soup = bs(driver.page_source, "html.parser")
    div = soup.find('div', attrs={'class': 'MuiGrid-root MuiGrid-container MuiGrid-spacing-xs-2 MuiGrid-item'})
    if div:
     for i in div:
        for j in [i.text for i in i.find_all('h5')]:
            job.append(j)
        for c in [i for i in i.find_next('h6')]:
            company_name.append(c)
        for l in (['https://staffy.az' + l['href'] for l in i.find_all('a', href=True)]):
            url.append(l)
    for u in url:
        links.append(u)

    for element in url:
     driver.get(element)
     time.sleep(1)
     soup1 = bs(driver.page_source, "html.parser")
     te = soup1.find('div', attrs={'class': 'MuiGrid-root MuiGrid-item MuiGrid-grid-xs-12 MuiGrid-grid-md-8'})
     if te != None:
        job_descr.append(te.text.replace('\n', ' '))
     else:
         job_descr.append(None)
         deadline.append(None)
     deadline_paragraf = soup1.find('p', string="Bitmə tarixi")
     if deadline_paragraf is not None:
         deadlin = deadline_paragraf.find_next('p')
         if deadlin is not None:
             deadline.append(deadlin.text)
         else:
             deadline.append(None)
     else:
         deadline.append(None)

driver.close()
#
driver.quit()
vac = pd.DataFrame({
    'job': pd.Series(job),
    'job_descr': pd.Series(job_descr),
    'company': pd.Series(company_name),
    'city':pd.Series(city),
    'job_type':pd.Series(job_type),
    'wage': pd.Series(wage),
    'posted': pd.Series(posted),
    'deadline': pd.Series(deadline),
    'links': pd.Series(links),
    'vip': pd.Series(np.zeros(shape=len(job), dtype=int))})

vac.deadline = pd.to_datetime(vac.deadline, format='%d.%m.%Y')
vac.posted = pd.to_datetime(vac.posted, format='%d.%m.%Y', errors='ignore')

# In[ ]:

# time.sleep(20)


from sqlalchemy import create_engine
from sqlalchemy.orm import close_all_sessions
import mysql.connector
import time
from sqlalchemy.types import UnicodeText, Integer, DateTime
from sqlalchemy import text

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
    vac.to_sql(name='recruit', con=conn, if_exists='append', chunksize=300, index=False,
                           dtype={'job': UnicodeText(), 'job_descr': UnicodeText(), 'company': UnicodeText(),
                                  'city': UnicodeText(), 'job_type': UnicodeText(), 'wage': Integer(),
                                  'posted': DateTime(), 'deadline': DateTime(), 'links': UnicodeText(),
                                  'vip': Integer()})

    # Add primary key column
    conn.execute(text("ALTER TABLE recruit ADD id INT PRIMARY KEY AUTO_INCREMENT FIRST;"))

# Dispose of the engine
engine.dispose()


# In[ ]:




