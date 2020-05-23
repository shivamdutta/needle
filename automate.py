from selenium import webdriver
import json
import time
from selenium.webdriver.chrome.options import Options

from LoggerWrapper import Logger
from Mailer import Mailer

class Automate:
    
    def __init__(self):
        with open('config.json') as f:
            self.config = json.load(f)
        self.logger = Logger(self.config['log_filename'], self.config['log_level']).logging
        self.mailer = Mailer()
        
    def generate_auth(self):
        
        try:
            options = Options()
            options.headless = True
            driver = webdriver.Chrome(options=options)
            
            # Headless feels
            # Open this in browser *Machine IP*:8080
            # or curl "http://127.0.0.1:8080/"
            
            driver.get('http://127.0.0.1:8080/')
            time.sleep(5)
            login_link = driver.find_element_by_id("login_url_div")
            login_link.click()
            time.sleep(5)

            user_name=self.config['user_name']
            password=self.config['password']
            driver.implicitly_wait(20)
            #self.logger.info('{}'.format(driver.page_source))
            x_path_id = '''/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[1]/input'''
            x_path_pass = '''/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[2]/input'''
            x_path_submit = '''/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[4]/button'''
            driver.find_element_by_xpath(x_path_id).send_keys(user_name)
            driver.find_element_by_xpath(x_path_pass).send_keys(password)
            driver.find_element_by_xpath(x_path_submit).click()
            time.sleep(5)
            pin=self.config['pin']
            x_path_pin = '''//*[@id="container"]/div/div/div/form/div[2]/div/input'''
            x_path_login = '''//*[@id="container"]/div/div/div/form/div[3]/button'''
            driver.find_element_by_xpath(x_path_pin).send_keys(pin)
            driver.find_element_by_xpath(x_path_login).click()
            time.sleep(5)
            auth_str = driver.find_element_by_xpath('//body').text
            auth_json = json.loads(auth_str)
            
            driver.quit()
            
            with open('auth.json', 'w') as outfile:
                json.dump(auth_json, outfile)
                
            self.logger.info('Generated auth file successfully : {}'.format(auth_json))    
            self.mailer.send_mail('Needle : Auth Generation Successful', 'Generated auth file successfully : {}'.format(auth_json))
        except Exception as ex:
            try:
                driver.quit()
            except:
                self.logger.error('Error while generating auth file : No driver found')
            self.logger.error('Error while generating auth file : {}'.format(ex))
            self.mailer.send_mail('Needle : Auth Generation Failure', 'Error while generating auth file : {}'.format(ex))
            
        return auth_json
    
if __name__ == "__main__":
    
    Automate().generate_auth()
            