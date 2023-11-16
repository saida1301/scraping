#!/usr/bin/env python
# coding: utf-8

# In[2]:


#!/usr/bin/env python
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
chrome_options.add_argument("--ignore-certificate-errors")

s = Service('C:/Users/saida/OneDrive/Desktop/Archive/chromedriver-win64/chromedriver.exe')
driver = webdriver.Chrome(service=s)
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs

import time


job = []
job_descr = []
company_name = []
deadline = []
posted = []
links = []
city = []
wage = []
job_type = []

driver.get('https://jobsearch.az/vacancies')
driver.implicitly_wait(999)
time.sleep(5)




target = driver.find_element(by=By.XPATH,value='//*[@id="scroller_desctop"]')
previus_height = driver.execute_script('return arguments[0].scrollTop = arguments[0].scrollHeight', target)
time.sleep(5)

driver.implicitly_wait(5)
time.sleep(5)





count = 0
while count<50:
    driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', target)
    time.sleep(5)
    new_height = driver.execute_script('return arguments[0].scrollTop = arguments[0].scrollHeight', target)

    if previus_height == new_height:
        break
    previus_height = new_height
    count += 1


print(count)


soup1 = bs(driver.page_source,"html.parser")


for i in soup1.find_all('h3',attrs={'class':'list__item__title'}):
    job.append(i.text.strip())


for i in soup1.find_all('a',attrs={'class':'list__item__text'}):
    companyName = str(i).split("</h3>")[1].split("\n")[1].strip()
    links.append(f"https://jobsearch.az{i['href']}")
    company_name.append(companyName)

for i in soup1.find_all('div',attrs={'class':'list__item__body'}):
    posted.append(soup1.find('div',attrs={'class':'list__item__end'}).find_next('ul').find_next('li').find_next('li').find_next('span').text)


for element in links:
        time.sleep(1)
        try:
          source =driver.get(element)
          soup = bs(driver.page_source,"html.parser")
          job_descrr = driver.find_element(by=By.XPATH,value='//*[@id="description"]').text
          job_descr.append(job_descrr)
          deadlinee = driver.find_element(by=By.XPATH, value='//*[@class="vacancy__deadline"]').text.replace('Son tarix ','').replace('okt','October').replace('noy','November')
          deadline.append(deadlinee)


        except:
            print('yes')





driver.quit()










vac = pd.DataFrame({'job':pd.Series(job),'job_descr':pd.Series(job_descr),'company':pd.Series(company_name),
                     'city':pd.Series(city),'job_type':pd.Series(job_type),
                   'wage':pd.Series(wage),'posted':pd.Series(posted),'deadline':pd.Series(deadline),'links':pd.Series(links),
                                'vip':pd.Series(np.zeros(shape = len(job),dtype = int))})



#vac.posted = pd.to_datetime(vac.posted,format = '%d.%m.%Y')


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

vac.deadline = pd.to_datetime(vac.deadline, errors='coerce', infer_datetime_format=True)


# In[ ]:


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
    conn.execute(text("drop table IF exists jobsearch"))
    vac.to_sql(name = 'jobsearch', con = conn,if_exists='append',chunksize = 300,index=False,
                        dtype = {'job':UnicodeText(),'job_descr':UnicodeText(),'company':UnicodeText(),
                                'city':UnicodeText(),'job_type':UnicodeText(),'wage':Integer(),
                                'posted':DateTime(),'deadline':UnicodeText(),'links':UnicodeText(),'vip':Integer()} )
    conn.execute(text("ALTER TABLE jobsearch ADD id INT PRIMARY KEY AUTO_INCREMENT first ;"))
    conn.close()
#         time.sleep(120)
engine.dispose()


# In[ ]:




