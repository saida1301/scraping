#!/usr/bin/env python
# coding: utf-8

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service

import datetime
from selenium import webdriver
import numpy as np
from selenium.webdriver.common.action_chains import ActionChains
import re
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--ignore-certificate-errors")

chrome_options.add_argument("start-maximized")
chrome_options.add_argument("disable-infobars")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--no-sandbox")

s = Service('C:/Users/turga/chromedriver.exe')
driver = webdriver.Chrome(service=s)
import pandas as pd

from bs4 import BeautifulSoup as bs

import time

url = 'https://banco.az/az/jobs'
driver.get(url)

links_list = []
positions_list = []
companies_list = []
salary_list = []
adress_list = []
jobtype_list = []
jobdescr_list = []
deadline = []
posted = []

pg = driver.find_element(by=By.XPATH, value='/html/body/main/div/div/div[1]/div[4]/div/div[31]/div/div/a[2]')
n = int(pg.text)
pages = np.arange(0, 4)

# links = driver.find_elements(by=By.XPATH, value='//div[@class="item panel m-b-10"]/a')
# positions = driver.find_elements(by=By.XPATH, value='//div[@class="item panel m-b-10"]/a/p')
# companies = driver.find_elements(by=By.CLASS_NAME, value='title')
# deadlines = driver.find_elements(by=By.XPATH, value='//span[@class="time"]/b')
# posteds = driver.find_elements(by=By.XPATH, value='//div[@class="item panel m-b-10"]/a/time')

for p in pages:
    # time.sleep(1)
    driver.get('https://banco.az/az/jobs?page='+str(p))
    soup = bs(driver.page_source, "html.parser")
    positions=soup.find_all('div', attrs={'class': 'item panel m-b-10'})
    for position in positions:
        positions_list.append(position.p.text.replace('\n', ' '))
        companies_list.append(position.span.text)
        posted.append(position.time.text)
        deadline.append(soup.find('span', attrs={'class': 'time'}).find_next('span').text)
        links_list.append('https://banco.az'+position.a['href'])

for url in links_list:
    try:
        driver.get(url)
        time.sleep(0.5)
        soup_pag = bs(driver.page_source, "html.parser")
        descr = soup_pag.find('div', attrs={'class': 'field-item even'})
        if descr is not None:
            jobdescr_list.append(descr.text.replace('\n', ' '))
        else:
            jobdescr_list.append('')
    except TimeoutException:
        driver.refresh()


driver.quit()

# In[70]:


dataframe_banco = pd.DataFrame({'job': pd.Series(positions_list, dtype='object'),
                                'job_descr': pd.Series(jobdescr_list, dtype='object'),
                                'company': pd.Series(companies_list),
                                'city': pd.Series(adress_list), 'job_type': pd.Series(jobtype_list),
                                'wage': pd.Series(salary_list), 'posted': pd.Series(posted),
                                'deadline': pd.Series(deadline), 'links': pd.Series(links_list),
                                'vip': pd.Series(np.zeros(shape=len(positions_list), dtype=int))})

# In[71]:


dataframe_banco.job_descr = dataframe_banco.job_descr.apply(
    lambda x: x.replace('Bizi sosial şəbəkələrdən izləyin!!FacebookInstagramTelegram', '').
    replace('İş barədə məlumat', '').replace('Ümumi məlumat', '').
    replace('Namizədə tələblər', ''))
dataframe_banco.deadline = pd.to_datetime(dataframe_banco.deadline, format='%d.%m.%Y')
dataframe_banco.posted = pd.to_datetime(dataframe_banco.posted, format=' %d.%m.%Y')

# In[72


# dataframe_banco = dataframe_banco[(dataframe_banco['deadline'] - datetime.timedelta(days=30)) >
#                                   (datetime.datetime.today() - datetime.timedelta(days=30))]

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
    conn.execute(text("DROP TABLE IF EXISTS bancoaz"))
    dataframe_banco.to_sql(name='bancoaz', con=conn, if_exists='append', chunksize=300, index=False,
                           dtype={'job': UnicodeText(), 'job_descr': UnicodeText(), 'company': UnicodeText(),
                                  'city': UnicodeText(), 'job_type': UnicodeText(), 'wage': Integer(),
                                  'posted': DateTime(), 'deadline': DateTime(), 'links': UnicodeText(),
                                  'vip': Integer()})
    conn.execute(text("ALTER TABLE bancoaz ADD id INT PRIMARY KEY AUTO_INCREMENT first ;"))
    conn.close()
engine.dispose()
# test
# Test_emp1234.