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
chrome_options.add_argument("--ignore-certificate-errors")

chrome_options.add_argument("start-maximized")
chrome_options.add_argument("disable-infobars")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--no-sandbox")
s = Service('C:/Users/saida/OneDrive/Desktop/Archive/chromedriver-win64/chromedriver.exe')
driver = webdriver.Chrome(service=s)
import pandas as pd
from bs4 import BeautifulSoup as bs

import time

job = []
job_descr = []
job_descr2 = []
company_name = []
city = []
job_type = []
wage = []
deadline = []
links = []
posted = []

purl = ['https://boss.az/vacancies?action=index&controller=vacancies&only_path=true&type=vacancies']

for p in purl:
    driver.get(p)
    url = driver.find_elements(by=By.CLASS_NAME, value='page')

    for i in url:
        for i in (i.find_elements(by=By.TAG_NAME, value='a')):
            if i.get_attribute('href') not in purl:
                purl.append(i.get_attribute('href'))
    url = []

    link = driver.find_elements(by=By.CLASS_NAME, value='results-i-link')
    for lnk in link:
        url.append(lnk.get_attribute("href"))
    for u in url:
        links.append(u)

    jbnm = driver.find_elements(by=By.CLASS_NAME, value='results-i-title')
    for t in jbnm:
        job.append(t.text)

    cmpnm = driver.find_elements(by=By.CLASS_NAME, value='results-i-company')
    for c in cmpnm:
        company_name.append(c.text)
    print(f"""
    job -->   {job}
    job_descr -->  {job_descr}
    company_name  -->{company_name}
    city  -->  {city}
    type --->   {job_type}
    wage -- >>>  {wage}
    dedline -->  {deadline}
    linkss    --- >   {links}
    post    --->   {posted}
    """)

    for element in url:
        driver.execute_script("window.open('');")
        driver.switch_to.window(driver.window_handles[-1])
        driver.get(element)
        txt = driver.find_element(by=By.CSS_SELECTOR, value=
            'body > div.container > div.main > div.post-cols.post-info > div:nth-child(1) > dd')
        job_descr.append(txt.text)
        cit = driver.find_element(by=By.CSS_SELECTOR, value=
            'body > div.container > div.main > div.main-highlight > div > div:nth-child(1) > ul > li:nth-child(1) > div.region.params-i-val')
        city.append(cit.text)
        wag = driver.find_elements(by=By.XPATH, value='/html/body/div[3]/div[1]/div[2]/div[1]/span')
        for w in wag:
            wage.append(w.text)
        posted.append(driver.find_element(by=By.CSS_SELECTOR, value='div.bumped_on.params-i-val').text)
        deadline.append(driver.find_element(by=By.CSS_SELECTOR, value='div.expires_on.params-i-val').text)

        driver.close()
        driver.switch_to.window(driver.window_handles[0])

time.sleep(3)
driver.quit()

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
vac.deadline.replace(monhts, regex=True, inplace=True)
vac.posted.replace(monhts, regex=True, inplace=True)
vac.job_descr = vac.job_descr.replace('İş barədə məlumat\n', '', regex=True)
vac.deadline = vac.deadline.str.strip()
vac.posted = vac.posted.str.strip()
vac.deadline = pd.to_datetime(vac.deadline, format='%B %d, %Y')
vac.posted = pd.to_datetime(vac.posted, format='%B %d, %Y')

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

    conn.execute(text("drop table IF EXISTS boss"))
    vac.to_sql(name='boss', con=conn, if_exists='append', chunksize=300, index=False,
               dtype={'job': UnicodeText(), 'job_descr': UnicodeText(), 'company': UnicodeText(),
                      'city': UnicodeText(), 'job_type': UnicodeText(), 'wage': Integer(),
                      'posted': DateTime(), 'deadline': DateTime(), 'links': UnicodeText(), 'vip': Integer()})
    conn.execute(text("ALTER TABLE boss ADD id INT PRIMARY KEY AUTO_INCREMENT first ;"))
    conn.close()
#         time.sleep(120)
engine.dispose()

