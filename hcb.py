#!/usr/bin/env python
# coding: utf-8

# In[18]:


#!/usr/bin/env python
# coding: utf-8

# In[3]:

import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
import pandas as pd
import numpy as np
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import requests
from bs4 import BeautifulSoup

s = Service('C:/Users/turga/chromedriver.exe')
driver = webdriver.Chrome(service=s)
url = 'https://hcb.az/'
driver.get(url)

links_list = []
positions_list = []
companies_list = []
salary_list=[]
adress_list=[]
jobtype_list=[]
jobdescr_list=[]
deadline=[]
posted = []


r = requests.get(url)

html_table = BeautifulSoup(r.text,'html.parser').find('table')
r.close()

df = pd.read_html(str(html_table))
df = df[0]

all_position = driver.find_elements(by=By.XPATH, value='//table[@class="table"]/tbody/tr/td[3]/a')
all_company = driver.find_elements(by=By.XPATH, value='//table[@class="table"]/tbody/tr/td[4]/a')
all_deadline = driver.find_elements(by=By.XPATH, value='//table[@class="table"]/tbody/tr/td[2]/a')
all_salary = driver.find_elements(by=By.XPATH, value='//table[@class="table"]/tbody/tr/td[5]/a')

for link in all_position:
    links_list.append(link.get_attribute('href'))
    positions_list.append(" ".join(link.text.split(" ")))
for link in all_deadline:
    deadline.append(" ".join(link.text.split(" ")))
for link in all_company:
    companies_list.append(" ".join(link.text.split(" ")))

for link in all_salary:
    salary_list.append(" ".join(link.text.split(" ")))

for url in links_list:
    driver.get(url)
    descr = driver.find_element(by=By.CLASS_NAME, value='page_details')
    descr = descr.text.replace("\n"," ").replace(")","").replace("(","").split(" ")
    a=[]
    for i in descr:
        if len(i) >= 2:
            a.append(i)
    descr = " ".join(a)
    jobdescr_list.append(descr)

driver.quit()


wage=[]

print(deadline)
vac = pd.DataFrame({'job':pd.Series(positions_list, dtype=pd.StringDtype()),'job_descr':pd.Series(jobdescr_list, dtype=pd.StringDtype()),'company':pd.Series(companies_list,dtype=pd.StringDtype()),
                     'city':pd.Series(adress_list,dtype='float64'),'job_type':pd.Series(jobtype_list,dtype='float64'),
                   'wage':pd.Series(salary_list,dtype=pd.StringDtype()),'posted':pd.Series(posted, dtype='float64'),'deadline':pd.Series(deadline),'links':pd.Series(links_list),
                                'vip':pd.Series(np.zeros(shape = len(positions_list),dtype = int))})

vac.deadline = pd.to_datetime(vac.deadline,format = '%d.%m.%Y')
vac.posted = pd.to_datetime(vac.posted,format = '%d.%m.%Y')


# In[ ]:


from sqlalchemy import create_engine
from sqlalchemy.orm import close_all_sessions
import mysql.connector
import time
from sqlalchemy.types import UnicodeText,Integer,DateTime


username = 'root'
password = ''
host = 'localhost'
port = 3306
DB_NAME = 'abdulkadirbudak_com_emp_az'

engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{port}")

with engine.connect() as conn:
    result = conn.execute("USE abdulkadirbudak_com_emp_az")
    conn.execute("DROP TABLE IF EXISTS hcbaz")
    vac.to_sql(name = 'hcbaz', con = conn,if_exists='append',chunksize = 300,index=False,
                        dtype = {'job':UnicodeText(),'job_descr':UnicodeText(),'company':UnicodeText(),
                                'city':UnicodeText(),'job_type':UnicodeText(),'wage':Integer(),
                                'posted':DateTime(),'deadline':DateTime(),'links':UnicodeText(),'vip':Integer()} )
    conn.execute("ALTER TABLE hcbaz ADD id INT PRIMARY KEY AUTO_INCREMENT first ;")
    conn.close()
engine.dispose()

