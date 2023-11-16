#!/usr/bin/env python
# coding: utf-8

# In[1]:


# !/usr/bin/env python
# coding: utf-8

import datetime
from selenium import webdriver
import numpy as np
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
import re
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
chrome_options.add_argument("--ignore-certificate-errors")

s = Service('C:/Users/turqa/chromedriver.exe')
driver = webdriver.Chrome(service=s)
import pandas as pd
from bs4 import BeautifulSoup as bs

import time
from sqlalchemy import create_engine
from sqlalchemy.orm import close_all_sessions
import mysql.connector

job = []
job_descr = []
job_descr2 = []
company_name = []
city = []
job_type = []
wage = []
deadline = []
posted = []
links = []
driver.get('https://www.hellojob.az/vakansiyalar?page=1')
time.sleep(0.5)
pages = np.arange(1, 9)

for p in pages:
    time.sleep(1)
    driver.get('https://www.hellojob.az/vakansiyalar?page='+str(p))
    driver.refresh()
    soup = bs(driver.page_source, "html.parser")
    vacancies = soup.find_all('a',attrs={'class':'vacancies__item'})


    for vac in vacancies:
         soup1 = bs(driver.page_source, "html.parser")
         linksa = soup1.find('a', attrs={'class': 'vacancies__item'})
         job.append(vac.h3.text)
         company_name.append(vac.p.text)
         links.append('https://www.hellojob.az'+vac['href'])

for link in links:
    time.sleep(1)
    driver.get(link)
    pag_vac = bs(driver.page_source,"html.parser")
    if job_descr!=None and pag_vac !=None:
       job_descr.append(pag_vac.find('div',attrs={'class':'col-md-12 col-lg-12 elan_inner_desc ei_vo'}).text.strip())
    else:
       job_descr.append(None)
    city.append(pag_vac.find(attrs={'class':'company_details'}).find_next('ul').find_next('li').find_next('a').text)
    wage.append(pag_vac.find(attrs={'class':'salary'}).text)
    deadl = re.compile(r"Bitmə tarixi:\s[0-9]+\s\w+")
    info = driver.find_element(by=By.XPATH,value='/html/body/main/div[2]/div[1]/div[2]/div/div/div/ul').text.replace('\n', ' ')
    if re.search(deadl, info)==None:
        deadline.append('22 Dekabr')
    else:
       deadline.append(re.search(deadl, info).group(0).split('Bitmə tarixi:')[1])
    print(deadline)



time.sleep(3)
driver.close()
driver.quit()


# In[24]:


vac = pd.DataFrame({'job': pd.Series(job), 'job_descr': pd.Series(job_descr), 'company': pd.Series(company_name),
                    'city': pd.Series(city), 'job_type': pd.Series(job_type),
                    'wage': pd.Series(wage), 'posted': pd.Series(posted), 'deadline': pd.Series(deadline),
                    'links': pd.Series(links),
                    'vip': pd.Series(np.zeros(shape=len(job), dtype=int))})

monhts = {'Yanvar': 'January',
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

vac.deadline = vac.deadline.str.strip()
vac.deadline = vac.deadline.replace(monhts,regex = True)
print(vac.deadline)
vac.deadline = vac.deadline.apply(lambda x:x+' 2022' if 'January' in x else x+' 2023')
vac.deadline = pd.to_datetime(vac.deadline,format = '%d %B %Y')

pd.to_datetime(vac.posted, format = '%d %B')
vac.job_descr = vac.job_descr.apply(lambda x:x.replace('İş barədə məlumat',''))
vac.job_descr = vac.job_descr.apply(lambda x:x.strip())
vac.job = vac.job.apply(lambda x:x.replace('vakansiyası','') if type(x) == str else None)
pat = re.compile(r'\s\w+@\w+.\w+\s')
vac.job_descr = vac.job_descr.apply(lambda x:re.sub(pat,'',x))

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

engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{port}")

with engine.connect() as conn:
    result = conn.execute(text("USE emp_az"))
    conn.execute(text("drop table IF EXISTS hellojob"))
    vac.to_sql(name='hellojob', con=conn, if_exists='append', chunksize=300, index=False,
               dtype={'job': UnicodeText(), 'job_descr': UnicodeText(), 'company': UnicodeText(),
                      'city': UnicodeText(), 'job_type': UnicodeText(), 'wage': UnicodeText(),
                      'posted': DateTime(), 'deadline': DateTime(), 'links': UnicodeText(), 'vip': Integer()})
    conn.execute(text("ALTER TABLE hellojob ADD id INT PRIMARY KEY AUTO_INCREMENT first ;"))
    conn.close()
engine.dispose()


