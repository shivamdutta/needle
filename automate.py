from selenium import webdriver
import json
import time
from selenium.webdriver.chrome.options import Options

from LoggerWrapper import Logger

class Automate:
    
    def __init__(self):
        with open('config.json') as f:
            self.config = json.load(f)
        self.logger = Logger(self.config['log_filename'], self.config['log_level']).logging
        
    def generate_auth(self):
        
        try:
            
            options = Options()
            options.headless = True
            driver = webdriver.Chrome(options=options)

            driver.get('http://127.0.0.1:8080/')
            time.sleep(5)
            login_link = driver.find_element_by_id("login_url_div")
            login_link.click()
            time.sleep(5)

            user_name=self.config['user_name']
            password=self.config['password']
            driver.implicitly_wait(20)
            x_path_id = "//*[@id=\"container\"]/div/div/div[2]/form/div[1]/input" # "//*[@id=\"container\"]/div/div/div/form/div[2]/input"
            x_path_pass = "//*[@id=\"container\"]/div/div/div[2]/form/div[2]/input" # "//*[@id=\"container\"]/div/div/div/form/div[3]/input"
            x_path_submit = "//*[@id=\"container\"]/div/div/div/form/div[4]/button"
            driver.find_element_by_xpath(x_path_id).send_keys(user_name)
            driver.find_element_by_xpath(x_path_pass).send_keys(password)
            driver.find_element_by_xpath(x_path_submit).click()
            time.sleep(5)

            pin=self.config['pin']
            x_path_pin = "//*[@id=\"container\"]/div/div/div/form/div[2]/div/input"
            x_path_login = "//*[@id=\"container\"]/div/div/div/form/div[3]/button"
            driver.find_element_by_xpath(x_path_pin).send_keys(pin)
            driver.find_element_by_xpath(x_path_login).click()
            time.sleep(5)
            
            auth_str = driver.find_element_by_xpath('//body').text
            auth_json = json.loads(auth_str)

            driver.quit()
            
            with open('auth.json', 'w') as outfile:
                json.dump(auth_json, outfile)
                
            self.logger.info('Generated auth file successfully : {}'.format(auth_json))    
            
        except Exception as ex:
            self.logger.error('Could not generate auth file : {}'.format(ex))
            
        return auth_json
    
if __name__ == "__main__":
    
    Automate().generate_auth()
            