# -*- coding: utf-8 -*-
"""
Created on Wed Nov 25 16:41:55 2020

@author: u12257
"""

from selenium import webdriver
import requests
import urllib3
import time
import pandas as pd


url = 'https://kaspi.kz/red/?ref=startHeader'

urllib3.disable_warnings()

session = requests.Session()
session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.92 Safari/537.36'
    })


request = session.get(url, verify=False)

print(request)

driver = webdriver.Chrome(r"C:\Users\Админ\Downloads\chromedriver.exe")

driver.get(url)

time.sleep(5)
driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
submit_button = driver.find_element_by_css_selector('body > div.wrapper.wrapper--full.landing-wrapper > div.content.centered.clearfix.full > div > div.faq > div.faq__item.faq__item--terms > div > div.faq__question')
submit_button.click()

time.sleep(10)


df = pd.DataFrame(columns = ['Name', 'Data'])

proc = driver.find_element_by_css_selector('body > div.wrapper.wrapper--full.landing-wrapper > div.content.centered.clearfix.full > div > div.faq > div.faq__item.faq__item--terms.faq__item--active > div > div.faq__answer.faq__answer--active > div.kr_terms > div.kr_term.kr_term--purchases > div:nth-child(1) > span.kr_term__left')
proc_value = driver.find_element_by_css_selector('body > div.wrapper.wrapper--full.landing-wrapper > div.content.centered.clearfix.full > div > div.faq > div.faq__item.faq__item--terms.faq__item--active > div > div.faq__answer.faq__answer--active > div.kr_terms > div.kr_term.kr_term--purchases > div:nth-child(1) > span.kr_term__right')

df = df.append({'Name':proc.text,'Data':proc_value.text}, ignore_index=True)

limit = driver.find_element_by_css_selector('body > div.wrapper.wrapper--full.landing-wrapper > div.content.centered.clearfix.full > div > div.faq > div.faq__item.faq__item--terms.faq__item--active > div > div.faq__answer.faq__answer--active > div.kr_terms > div.kr_term.kr_term--limits > div.kr_term__text.kr_term__left')
limit_value = driver.find_element_by_css_selector('body > div.wrapper.wrapper--full.landing-wrapper > div.content.centered.clearfix.full > div > div.faq > div.faq__item.faq__item--terms.faq__item--active > div > div.faq__answer.faq__answer--active > div.kr_terms > div.kr_term.kr_term--limits > div.kr_term__text.kr_term__right')

df = df.append({'Name':limit.text,'Data':limit_value.text}, ignore_index=True)

bonus = driver.find_element_by_css_selector('body > div.wrapper.wrapper--full.landing-wrapper > div.content.centered.clearfix.full > div > div.faq > div.faq__item.faq__item--terms.faq__item--active > div > div.faq__answer.faq__answer--active > div.kr_terms > div.kr_term.kr_term--bonuses > div.kr_term__text.kr_term__left')
bonus_value = driver.find_element_by_css_selector('body > div.wrapper.wrapper--full.landing-wrapper > div.content.centered.clearfix.full > div > div.faq > div.faq__item.faq__item--terms.faq__item--active > div > div.faq__answer.faq__answer--active > div.kr_terms > div.kr_term.kr_term--bonuses > div.kr_term__row.kr_term__right > div > div')

df = df.append({'Name':bonus.text,'Data':bonus_value.text}, ignore_index=True)


df.to_csv("kaspi_red.csv")



time.sleep(3)
driver.close()
