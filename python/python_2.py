from selenium import webdriver
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
import time,random,names,os,requests,shutil
from seleniumwire import webdriver
from random_username.generate import generate_username
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.select import Select
from twocaptcha import TwoCaptcha
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
import time
from selenium.webdriver.common.proxy import *
global ALL_PROXIES
import uuid
global driver
global email
global DELAYS,CREDS
from fake_useragent import UserAgent

def solve_captcha(path):

    API_KEY = '...'
    solver = TwoCaptcha(API_KEY)
    captcha_code = solver.normal(path)['code']
    return captcha_code

def getProxy():
    a=re.get('xxx.com/api2').json()
    while a['protocol'] not in ['https','socks4','socks5']:
        a=re.get('xxxx.com/api1').json()
    return a

def register():
    global driver
    
    f=open("useragents.txt",'r')
    for i in range(random.randint(1, 1000)):
        agent=f.readline()
    profile = webdriver.FirefoxProfile()
    profile.set_preference("general.useragent.override", UserAgent().random)
    pxy=getProxy()
    options = {
        'proxy': { 
             pxy['protocol']:pxy['url'],
            'no_proxy': 'localhost,127.0.0.1,test_server:8080'
            }
        }
    print ("Using proxy : "+ pxy['url'])
    binary = FirefoxBinary('/.../firefox/firefox')
    driver = webdriver.Firefox(seleniumwire_options=options,firefox_profile=profile,firefox_binary=binary,executable_path=r"/.../geckodriver")
    print ("Loading Instagram")
    driver.get('https://www.instagram.com/accounts/emailsignup/?hl=en')
    gender = random.choice(['male', 'female'])
    time.sleep(random.randint(DELAYS["min"], DELAYS["max"])/100.0)
    

def fillName():
    fname=CREDS['fname']
    lname=CREDS['lname']
    print ("Filling Full name")
    for i in fname+" "+lname:
        driver.find_element_by_name('fullName').send_keys(i)
        time.sleep(random.randint(DELAYS["keys_min"], DELAYS["keys_max"])/1000.0)
    time.sleep(random.randint(DELAYS["min"], DELAYS["max"])/100.0)

def fillUsername():
    user_name=CREDS['username']
    print ("Filling username")
    for i in user_name:
        driver.find_element_by_name('username').send_keys(i)
        time.sleep(random.randint(DELAYS["keys_min"], DELAYS["keys_max"])/1000.0)
    time.sleep(random.randint(DELAYS["min"], DELAYS["max"])/100.0)

def fillPassword():
    password=CREDS['password']
    print ("Filling password")
    for i in password:
        driver.find_element_by_name('password').send_keys(i)
        time.sleep(random.randint(DELAYS["keys_min"], DELAYS["keys_max"])/1000.0)
    time.sleep(random.randint(DELAYS["min"], DELAYS["max"])/100.0)

def fillEmail():
    global email
    print ("Filling email")
    for i in email:
        driver.find_element_by_name('emailOrPhone').send_keys(i)
        time.sleep(random.randint(DELAYS["keys_min"], DELAYS["keys_max"])/1000.0)
    time.sleep(random.randint(DELAYS["min"], DELAYS["max"])/100.0)

def submit():
    driver.find_element_by_name('password')
    driver.find_element_by_xpath('//button[@type="submit"]').click()
    time.sleep(random.randint(DELAYS["min"], DELAYS["max"])/100.0)

def shuffleFunc():
    f = [fillName, fillUsername, fillPassword, fillEmail]
    random.shuffle(f)
    for i in f:
        i()
    time.sleep(random.randint(DELAYS["min"], DELAYS["max"])/100.0)

def birthday():
    print ("Filling birthday")
    time.sleep(random.randint(DELAYS["min"], DELAYS["max"])/100.0)
    driver.find_element_by_xpath("//select[@title='Month:']/option[@value='%d']" % random.randint(1,12)).click()
    time.sleep(random.randint(DELAYS["min"], DELAYS["max"])/100.0)
    driver.find_element_by_xpath("//select[@title='Day:']/option[text()='%d']" % random.randint(1,30)).click()
    time.sleep(random.randint(DELAYS["min"], DELAYS["max"])/100.0)
    driver.find_element_by_xpath("//select[@title='Year:']/option[text()='%s']" % CREDS['year']).click()
    

def nextStep():
    print ("Click on Next Button")
    driver.find_element_by_xpath('''//button[contains(text(),"Next")]''').click()
    return True

def begin(em,delays,creds):
    global email,DELAYS,CREDS
    CREDS=creds
    email=em
    DELAYS=delays
    register()
    shuffleFunc()
    submit()
    time.sleep(10)
    birthday()
    nextStep()
