#!/usr/bin/env python
# coding: utf-8

# In[33]:
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
import pandas as pd
import numpy as np
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
s = Service('C:/Users/turga/chromedriver.exe')
driver = webdriver.Chrome(service=s)
url = 'https://position.az/'
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

links = driver.find_elements(by=By.XPATH, value='//tbody[@class="grid"]/tr/td[1]/a')
positions = driver.find_elements(by=By.XPATH, value='//tbody[@class="grid"]/tr/td[1]/a')
companies = driver.find_elements(by=By.XPATH, value='//tbody[@class="grid"]/tr/td[2]/a')
# x = slice(5)
# links = links[x]
# print(len(links))
for link in links:
    links_list.append(link.get_attribute('href'))

for position in positions:
    positions_list.append(position.text)

for company in companies:
    companies_list.append(company.text)

for i in links_list:
    driver.execute_script("window.open('');")
    driver.switch_to.window(driver.window_handles[1])
    driver.get(i)
    driver.implicitly_wait(5)
    time.sleep(5)

    p_d = (driver.find_element(by=By.XPATH, value="//span[@class='vacancy-date']")).text
    posted.append(p_d[8:18])
    deadline.append(p_d[21:])

    # adress = driver.find_element_by_xpath("//*[@id='main']/div[2]/div/div/div/div[1]/div[3]/p/span")
    # adress_list.append(adress.text)

    descr = driver.find_element(by=By.XPATH, value="//*[@id='main']/div[2]/div/div/div/div[1]/div[4]")
    jobdescr_list.append(descr.text)

    driver.close()
    driver.switch_to.window(driver.window_handles[0])

driver.quit()

dataframe_position = pd.DataFrame(
    {'job': pd.Series(positions_list, dtype='object'), 'job_descr': pd.Series(jobdescr_list, dtype='object'),
     'company': pd.Series(companies_list),
     'city': pd.Series(adress_list), 'job_type': pd.Series(jobtype_list),
     'wage': pd.Series(salary_list), 'posted': pd.Series(posted), 'deadline': pd.Series(deadline),
     'links': pd.Series(links_list),
     'vip': pd.Series(np.zeros(shape=len(positions_list), dtype=int))})

dataframe_position.deadline = pd.to_datetime(dataframe_position.deadline, format='%Y-%m-%d')
dataframe_position.posted = pd.to_datetime(dataframe_position.posted, format='%Y-%m-%d')

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
    conn.execute(text("DROP TABLE IF EXISTS positionaz"))
    dataframe_position.to_sql(name='positionaz', con=conn, if_exists='append', chunksize=300, index=False,
                              dtype={'job': UnicodeText(), 'job_descr': UnicodeText(), 'company': UnicodeText(),
                                     'city': UnicodeText(), 'job_type': UnicodeText(), 'wage': Integer(),
                                     'posted': DateTime(), 'deadline': DateTime(), 'links': UnicodeText(),
                                     'vip': Integer()})
    conn.execute(text("ALTER TABLE positionaz ADD id INT PRIMARY KEY AUTO_INCREMENT first ;"))
    conn.close()
engine.dispose()

# In[ ]:




