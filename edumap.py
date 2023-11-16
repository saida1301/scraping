#!/usr/bin/env python
# coding: utf-8

# In[17]:


#!/usr/bin/env python
# coding: utf-8

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import numpy as np
import re
import datetime
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

s = Service('C:/Users/turga/chromedriver.exe')
driver = webdriver.Chrome(service=s,options = chrome_options)
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
deadline = []
links = []
posted = []

driver.get('https://edumap.az/kateqoriya/vakansiyalar')
driver.implicitly_wait(15)

time.sleep(2)
soup_pag = bs(driver.page_source,"html.parser")
last_p = soup_pag.find('li',attrs={'class':'page-item'}).find_next('a')['href'].split('/')[-2]
pages = np.arange(1,10)

for p in pages:
    url = []
    driver.get(f'https://edumap.az/kateqoriya/vakansiyalar?page={str(p)}')
    time.sleep(0.5)
    soup = bs(driver.page_source,"html.parser")
    div = soup.find_all('h2',attrs={'class':'utf_post_title title-large'})
    for i in div:
        links.append(''+i.a['href'])
        job.append(i.text)

for link in links:
    time.sleep(0.5)
    driver.get(link)
    soup_pag = bs(driver.page_source,"html.parser")
    job_descr.append(soup_pag.find('div',attrs={'id':'yazilar'}).text.strip())
    date = soup_pag.find('span', attrs={'class': 'utf_post_date'})
    posted.append(date.text.strip())
    deadline.append(None)

driver.quit()

vac = pd.DataFrame({'job':pd.Series(job),'job_descr':pd.Series(job_descr),'company':pd.Series(company_name),
                     'city':pd.Series(city),'job_type':pd.Series(job_type),
                   'wage':pd.Series(wage),'posted':pd.Series(posted),'deadline':pd.Series(deadline),
                    'links':pd.Series(links),'vip':pd.Series(np.zeros(shape = len(job),dtype = int))})


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


# In[20]:


# vac.posted.replace(monhts,regex = True,inplace = True)


# In[22]:


# vac.posted = vac.posted.apply(lambda x:datetime.date.today() if 'saat' in x else (datetime.date.today() - datetime.timedelta(days = int(x)) if
#                                                                  'gün' in x else (datetime.date.today() - datetime.timedelta(weeks = int(x))
#                                                                  if 'həftə' in x else (datetime.date.today() - datetime.timedelta(minutes = int(x))
#                                                                  if 'dəqiqə' in x else (datetime.date.today() - datetime.timedelta(seconds = int(x))
#                                                                  )))))


# In[23]:


# vac = vac[vac['posted'].apply(lambda x: vac['posted'][0] - x).dt.days < 31]


# In[150]:


from sqlalchemy import create_engine
from sqlalchemy.orm import close_all_sessions
import mysql.connector
import time
import MySQLdb
from sqlalchemy.types import UnicodeText,Integer,DateTime
from sqlalchemy import text

username = 'root'
password = ''
host = 'localhost'
port = 3306
DB_NAME = 'emp_az'

engine = create_engine(f"mysql+mysqldb://{username}:{password}@{host}:{port}/emp_az?charset=utf8mb4")

with engine.connect() as conn:
    result = conn.execute(text("USE emp_az"))
    conn.execute(text("drop table IF exists edumap"))
    vac.to_sql(name = 'edumap', con = conn,if_exists='append',chunksize = 300,index=False,
                        dtype = {'job':UnicodeText(),'job_descr':UnicodeText(),'company':UnicodeText(),
                                'city':UnicodeText(),'job_type':UnicodeText(),'wage':Integer(),
                                'posted':UnicodeText(),'deadline':DateTime(),'links':UnicodeText(),'vip':Integer()} )
    conn.execute(text("ALTER TABLE edumap ADD id INT PRIMARY KEY AUTO_INCREMENT first ;"))
    conn.close()
#         time.sleep(120)
engine.dispose()

