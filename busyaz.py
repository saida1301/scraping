#!/usr/bin/env python
# coding: utf-8

# In[15]:


# !/usr/bin/env python
# coding: utf-8

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
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
chrome_options.add_argument("--ignore-certificate-errors")

import pandas as pd
from bs4 import BeautifulSoup as bs
from selenium.webdriver.chrome.service import Service
import time

s = Service('C:/Users/saida/OneDrive/Desktop/Archive/chromedriver-win64/chromedriver.exe')
driver = webdriver.Chrome(service=s, options=chrome_options)

job = []
job_descr = []
company_name = []
city = []
job_type = []
wage = []
deadline = []
links = []
posted = []

driver.get('https://busy.az/vacancies')
pages = np.arange(1, 20)
for page in pages:
    time.sleep(2)
    driver.get(f'https://busy.az/vacancies?page={str(page)}')
    #     urls.append(driver.find_elements_by_class_name('pagination-arrow')[-1].find_element_by_tag_name('a').get_attribute('href'))
    #     next_p = driver.find_elements_by_class_name('pagination-arrow')[-1].find_element_by_tag_name('a').click()
    soup = bs(driver.page_source, "html.parser")
    div = soup.find_all('a', attrs={'class': 'job-listing with-apply-button'})
    for i in div:
        if i.find_next('div', attrs={'class': 'job-listing-details'}).find_all('li')[-1].text.split()[0].strip().isdigit():
            if int(i.find_next('div', attrs={'class': 'job-listing-details'}).find_all('li')[-1].text.split()[0].strip()) < 30:
                company_name.append(
                    i.find_next('div', attrs={'class': 'job-listing-details'}).find_all('li')[0].text.strip())
                links.append(i['href'])
                job.append(i.find_next('h3').text)

        else:
            company_name.append(
                i.find_next('div', attrs={'class': 'job-listing-details'}).find_all('li')[0].text.strip())
            links.append(i['href'])
            job.append(i.find_next('h3').text)

for element in links:
    time.sleep(1)
    driver.get(element)
    soup = bs(driver.page_source, "html.parser")
    desc = soup.find('div', attrs={'class': 'single-page-section'})

    job_descrr = soup.find('div', attrs={'class': 'job-overview-inner'})
    if desc!=None:
        job_descr.append(desc.text.replace('İşin təsviri','').strip())
        wage.append(desc.find_next('i', attrs={'class': 'icon-line-awesome-euro'}).parent.find_next('h5').text)
        posted.append(desc.find_next('i', attrs={'class': 'icon-material-outline-date-range'}).parent.find_next('h5').text)
        deadline.append(desc.find_next('i', attrs={'class': 'icon-material-outline-date-range'}).find_next('i', attrs={
            'class': 'icon-material-outline-date-range'}).parent.find_next('h5').text)
    else:
      wage.append(None)
      posted.append(None)
      job_descr.append(None)
      deadline.append(None)


driver.close()
driver.quit()

# In[16]:


vac = pd.DataFrame({'job': pd.Series(job), 'job_descr': pd.Series(job_descr), 'company': pd.Series(company_name),
                    'city': pd.Series(city), 'job_type': pd.Series(job_type),
                    'wage': pd.Series(wage), 'posted': pd.Series(posted), 'deadline': pd.Series(deadline),
                    'links': pd.Series(links),
                    'vip': pd.Series(np.zeros(shape=len(job), dtype=int))})

# In[10]:


vac.deadline = pd.to_datetime(vac.deadline, format='%d.%m.%Y')
vac.posted = pd.to_datetime(vac.posted, format='%d.%m.%Y')

# In[10]:


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

engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{DB_NAME}?charset=utf8mb4")

with engine.connect() as conn:
    result = conn.execute(text("USE emp_az"))
    conn.execute(text("DROP TABLE IF EXISTS busy"))
    vac.to_sql(name='busy', con=conn, if_exists='append', chunksize=300, index=False,
               dtype={'job': UnicodeText(), 'job_descr': UnicodeText(), 'company': UnicodeText(),
                      'city': UnicodeText(), 'job_type': UnicodeText(), 'wage': Integer(),
                      'posted': DateTime(), 'deadline': DateTime(), 'links': UnicodeText(), 'vip': Integer()})
    conn.execute(text("ALTER TABLE busy ADD id INT PRIMARY KEY AUTO_INCREMENT first ;"))
    conn.close()
#         time.sleep(120)
engine.dispose()

# In[ ]:
