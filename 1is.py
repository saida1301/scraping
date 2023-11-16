#!/usr/bin/env python
# coding: utf-8

# In[103]:


#!/usr/bin/env python
# coding: utf-8

import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("disable-infobars")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--ignore-certificate-errors");
s = Service('C:/Users/turga/chromedriver.exe')
driver = webdriver.Chrome(service=s, options=chrome_options)
import pandas as pd
from bs4 import BeautifulSoup as bs
import time
import numpy as np


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

driver.get('https://1is.az/vacancy.php')
time.sleep(0.5)
pages = np.arange(1, 15)
for i in pages:
    time.sleep(1)
    driver.get('https://1is.az/vacancy?type=1&pageno='+str(i)+'&view=on')
    soup = bs(driver.page_source, "html.parser")
    vacancies = soup.find_all('div', attrs={'class': 'card job-box rounded shadow border-0 overflow-hidden'})

    for vac in vacancies:
         soup1 = bs(driver.page_source, "html.parser")
         job.append(vac.h5.text)
         company_name.append(vac.h6.text)
         wage.append(vac.p.text)
         links.append('https://1is.az/' + vac.a['href'])
         posted.append(soup.find('i', attrs={'class': 'far fa-calendar-alt text-color'}).find_next('span', attrs={
             'class': 'text-color'}).text)
for link in links:
    time.sleep(1)
    driver.get(link)
    pag_vac = bs(driver.page_source,"html.parser")
    desc=pag_vac.find_all('div',attrs={'class':'ml-lg-4'})
    if desc==None:
      job_descr.append(None)
      deadline.append(None)
    else:
      for p in desc:
       desc = pag_vac.find_all('div', attrs={'class': 'ml-lg-4'})
       job_descr.append(pag_vac.find('div',attrs={'class':'ml-lg-4'}).text.strip())
       info = pag_vac.find_all('div',attrs={'class':'card-body'})[1]
       deadline.append(info.find('p',attrs={'class':'text-primary mb-0 mb-0 deadline-scrap'}).text.strip())

time.sleep(3)
driver.close()
driver.quit()


# In[104]:



vac = pd.DataFrame({
    'job':pd.Series(job),
    'job_descr':pd.Series(job_descr),
    'company':pd.Series(company_name),
    'city':pd.Series(city),
    #'job_type':pd.Series(job_type),
    'wage':pd.Series(wage),
    'posted':pd.Series(posted),
    'deadline':pd.Series(deadline),
    'links':pd.Series(links),
    'vip':pd.Series(np.ones(shape = len(job),dtype = int))})

vac.deadline = pd.to_datetime(vac.deadline,format = '%d-%m-%Y')
vac.posted = pd.to_datetime(vac.posted, format='%d.%m.%Y')
#
# vac = vac[(vac['deadline'] - datetime.timedelta(days = 30)) >
#         (datetime.datetime.today() - datetime.timedelta(days = 30))]


from sqlalchemy import create_engine
from sqlalchemy.orm import close_all_sessions
import mysql.connector
import time
from sqlalchemy.types import UnicodeText,Integer,DateTime
from sqlalchemy import text


username = 'root'
password = ''
host = 'localhost'
port = 3306
DB_NAME = 'emp_az'

engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/emp_az?charset=utf8mb4")


with engine.connect() as conn:
    result = conn.execute(text("USE emp_az"))
    conn.execute(text("drop table IF Exists recruit"))
    vac.to_sql(name = 'recruit', con = conn,if_exists='append',chunksize = 300,index=False,
                        dtype = {'job':UnicodeText(),'job_descr':UnicodeText(),'company':UnicodeText(),
                                'city':UnicodeText(),'job_type':UnicodeText(),'wage':UnicodeText(),
                                'posted':DateTime(),'deadline':DateTime(),'links':UnicodeText(),'vip':Integer()} )
    conn.execute(text("ALTER TABLE recruit ADD id INT PRIMARY KEY AUTO_INCREMENT first ;"))
    conn.close()
#         time.sleep(120)
engine.dispose()


# In[ ]:




