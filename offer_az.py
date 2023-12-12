#!/usr/bin/env python
# coding: utf-8

# In[21]:


# !/usr/bin/env python
# coding: utf-8

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import numpy as np
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
s = Service('C:/Users/saida/OneDrive/Desktop/Archive/chromedriver-win64/chromedriver.exe')
driver = webdriver.Chrome(service=s)
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import time

job = []
job_descr = []
company_name = []
city = []
job_type = []
wage = []
post_time = []
deadline = []
links = []
posted = []
driver.get('https://www.offer.az/is-elanlari/')
time.sleep(1)
pg = driver.find_element(by=By.XPATH, value='/html/body/main/section[3]/div/nav[1]/div/a[2]')
n = int(pg.text)
pages = np.arange(1, 16)


for p in pages:
    time.sleep(1)
    driver.get('https://www.offer.az/is-elanlari/page/'+str(p))


    # url = []
    soup = bs(driver.page_source, "html.parser")
    lpg = soup.find_all('a', attrs={'class': 'job-card__title'})

    for i in lpg:
        links.append(i['href'])

        # url.append(i['href'])
        cities = driver.find_elements(by=By.XPATH, value='/html/body/main/section[3]/div/div[1]/div[9]/div[2]/p[2]')
        for ci in cities:
            city.append(ci.text.split('-')[1])
        wag = driver.find_elements(by=By.XPATH, value='/html/body/main/section[3]/div/div[1]/div[9]/div[1]')
        for w in wag:
            wage.append(w.text)

for element in links:
    time.sleep(1)
    driver.get(element)
    soup1 = bs(driver.page_source, "html.parser")
    descr = soup1.find('div', attrs={'class': 'post__content'})
    if descr != None:
       job_descr.append(descr.text)
    else:
       job_descr.append(None)
    job_name = soup1.find('h1', attrs={'class': 'top-banner__title'}).text.replace('vakansiya — 2023','')
    job.append(job_name)
    company = soup1.find('span', attrs={'class': 'post__meta-value'}).find_next('a').text
    company_name.append(company)
    # city_name = soup1.find('ul', attrs={'class': 'post__meta'}).find_next('li').find_next('li').find_next('li').find_next('li').find('span', attrs={'class': 'post__meta-value'}).find_next('a').text
    # city.append(city_name)
    posted.append(''.join(soup1.find('ul', attrs={'class': 'post__meta'}).find_next('li').text.split('Elan tarixi:')[0:]))
    deadline_time = driver.find_element(by=By.XPATH,value='/html/body/main/article/section[1]/div/div[1]/div/div/ul/li[2]/span[2]').text[0:]
    deadline.append(deadline_time)
# driver.switch_to.window(driver.window_handles[0])

time.sleep(3)
driver.close()
driver.quit()

# In[25]:


vac = pd.DataFrame({'job': pd.Series(job, dtype='object'), 'job_descr': pd.Series(job_descr, dtype='object'),
                    'company': pd.Series(company_name),
                    'city': pd.Series(city), 'job_type': pd.Series(job_type),
                    'wage': pd.Series(wage), 'posted': pd.Series(posted), 'deadline': pd.Series(deadline),
                    'links': pd.Series(links),
                    'vip': pd.Series(np.zeros(shape=len(job), dtype=int))})
pat = re.compile(r'\s\w+@\w+.\w+\s')
vac.job_descr = vac.job_descr.apply(lambda x: re.sub(pat, '', x))
vac.job_descr = vac.job_descr.apply(lambda x: x.replace('\n', ' '))

vac.deadline = pd.to_datetime(vac.deadline, format='%d.%m.%Y')
vac.posted = pd.to_datetime(vac.posted, format=' %d.%m.%Y')

# In[ ]:


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

engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/emp_az?charset=utf8mb4")

with engine.connect() as conn:
    result = conn.execute(text("USE emp_az"))

    conn.execute(text("drop table IF EXISTS offeraz"))
    vac.to_sql(name='offeraz', con=conn, if_exists='append', chunksize=300, index=False,
               dtype={'job': UnicodeText(), 'job_descr': UnicodeText(), 'company': UnicodeText(),
                      'city': UnicodeText(), 'job_type': UnicodeText(), 'wage': Integer(),
                      'posted': DateTime(), 'deadline': DateTime(), 'links': UnicodeText(), 'vip': Integer()})
    conn.execute(text("ALTER TABLE offeraz ADD id INT PRIMARY KEY AUTO_INCREMENT first ;"))
    conn.close()
engine.dispose()


