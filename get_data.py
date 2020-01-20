from selenium import webdriver
# 1st import: Allows you to launch/initiate a browser
from selenium.webdriver.common.by import By
# 2nd import: Allows you to search for things using specific parameters.
from selenium.webdriver.support.ui import WebDriverWait
# 3rd import: Allows you to wait for a page to load.
from selenium.webdriver.support import expected_conditions as EC
# 4th import: Specify what you are looking for on a specific page in order to determine that the webpage has loaded.
from selenium.common.exceptions import TimeoutException
# 5th import: Handling a timeout situation
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.action_chains import ActionChains

import re, pymongo, time, pdb, itertools, requests, json, locale, csv
from pymongo import MongoClient
from datetime import datetime

def init_browser():
    driver = webdriver.Chrome('/Users/hh/Documents/chromedriver')
    driver.set_window_size(500, 1280)
    return driver

# def sign_in(driver, login_url):
#     driver.get(login_url)
#     user = 'your ID'
#     pass_w = 'your PW'

#     username = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="login-username"]')))
#     username.send_keys(user)
#     signin_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="login-signin"]')))
#     driver.execute_script("arguments[0].();", signin_button)
#     password = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="login-passwd"]')))
#     password.send_keys(pass_w)
#     submit_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="login-signin"]')))
#     driver.execute_script("arguments[0].();", submit_button)
#     return  

def scrape_revenue_from_yahoo_finance_and_csvFile(driver, each_company, collection):
    # DATASET 1: scrape from yahoo finance
    # driver.get(each_company)   
    
    # time.sleep(2)
    # quarterly_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#Col1-1-Financials-Proxy > section > div.Mt\(18px\).Mb\(15px\) > div.Fl\(end\).smartphone_Fl\(n\).IbBox.smartphone_My\(10px\).smartphone_D\(b\) > button')))  
    # quarterly_button.()

    # time.sleep(2)
    # pre_date = get_revenue_from_yahoo_finance(driver, '#Col1-1-Financials-Proxy > section > div.Pos\(r\) > div.W\(100\%\).Whs\(nw\).Ovx\(a\).BdT.Bdtc\(\$seperatorColor\) > div > div.D\(tbhg\) > div')
    # year = [datetime.strptime(d, '%m/%d/%Y').date().year for d in pre_date]
    # peoriode = ['Q1' if datetime.strptime(d, '%m/%d/%Y').date().month in [1,2,3] else 'Q2' if datetime.strptime(d, '%m/%d/%Y').date().month in [4,5,6] else 'Q3' if datetime.strptime(d, '%m/%d/%Y').date().month in [7,8,9] else 'Q4' for d in pre_date]
    # date = [datetime.strptime(d, '%m/%d/%Y').strftime('%Y-%m-%d') for d in pre_date]
    # total_revenue = get_revenue_from_yahoo_finance(driver, '#Col1-1-Financials-Proxy > section > div.Pos\(r\) > div.W\(100\%\).Whs\(nw\).Ovx\(a\).BdT.Bdtc\(\$seperatorColor\) > div > div.D\(tbrg\) > div:nth-child(1) > div.D\(tbr\).fi-row.Bgc\(\$hoverBgColor\)\:h')
    # total_revenue[0] = ''
    # DATASET 2: scrape from each csv file
    
    with open(f'Resources/{collection}.csv') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        next(readCSV)
        year_peoriode, total_revenue, smi_us_ad_revenue, capture_rate, ori, lag, mau, YoY_Y, Y_test, Y_t_YoY = [], [], [], [], [], [], [], [], [], []
        for row in readCSV:
            y = row[0]
            t = row[2]
            s = row[3]
            c = row[4]
            o = row[5]
            l = row[6]
            m = row[7]
            y_y = row[8]
            y_t = row[9]
            y_t_y = row[10]
            year_peoriode.append(y)
            total_revenue.append(t)
            smi_us_ad_revenue.append(s)
            capture_rate.append(c)
            ori.append(o)
            lag.append(l)
            mau.append(m)
            YoY_Y.append(y_y)
            Y_test.append(y_t)
            Y_t_YoY.append(y_t_y)

    # put two dataset into one dictionary
    # print('Data points differences in two resources', collection, len(year), len(year_peoriode))

    # length = len(year) if len(year) < len(year_peoriode) else len(year_peoriode)
    revenue_history = []
    year_peoriode, total_revenue, smi_us_ad_revenue, capture_rate, ori, lag, mau, YoY_Y, Y_test, Y_t_YoY = list(reversed(year_peoriode)), list(reversed(total_revenue)), list(reversed(smi_us_ad_revenue)), list(reversed(capture_rate)), list(reversed(ori)), list(reversed(lag)), list(reversed(mau)), list(reversed(YoY_Y)), list(reversed( Y_test)), list(reversed(Y_t_YoY))

    for index in range(len(year_peoriode)):
        container_dicts = {}
        container_dicts['Year_Peoriode'] = year_peoriode[index]
        container_dicts['Total_Revenue'] = total_revenue[index]
        container_dicts['SMI_US_Ad_Revenue'] = smi_us_ad_revenue[index]
        container_dicts['ORI'] = ori[index]
        container_dicts['LAG'] = lag[index]
        container_dicts['MAU'] = mau[index]
        container_dicts['YoY_Y'] = YoY_Y[index]
        container_dicts['Y_test'] = Y_test[index]
        container_dicts['Y_t_YoY'] = Y_t_YoY[index]
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8') 
        container_dicts['Capture_Rate'] = capture_rate[index]
        revenue_history.append(container_dicts)

    return revenue_history

def get_revenue_from_yahoo_finance(driver, css_selecter):
    # Call from (def scrape_revenue_from_yahoo_finance_and_csvFile)
    row = driver.find_element_by_css_selector(css_selecter)
    divs = row.find_elements_by_tag_name('div')[3:]
    span_text = [span.text for div in divs for span in div.find_elements_by_tag_name('span')]
    return span_text

def scrape_stock_from_yahoo_finance_and_stock_api(driver, each_company, collection):
    # DATASET 3: scrape historical stock data from stock api
    url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v2/get-historical-data"
    headers = {'x-rapidapi-host': "apidojo-yahoo-finance-v1.p.rapidapi.com", 'x-rapidapi-key': "af07fb24f3msh70060f8b18a57bcp14655fjsnfecc298458e1"}
    queries = {"frequency":"1mo","filter":"history","period1":"1041379200","period2":"1546300740","symbol":collection}
    response = get_res_from_stock_api(url, headers, queries, collection)

    index_arr = []
    for dicts in response['prices']:
        if 'splitRatio' in list(dicts.keys()):
            index_arr.append(response['prices'].index(dicts))
    if len(index_arr) > 0:
        for index in index_arr:
            del response['prices'][index]

    pre_stock_history = {}
    for i in response['prices']:
        pre_stock_history[i['date']] = i['close'] 

    # DATASET 4: scrape stock data for a recent year from yahoo finance
    base_url_monthly_data_for_a_year = 'https://finance.yahoo.com/quote/' + collection + '/history?period1=1547225224&period2=1578761224&interval=1mo&filter=history&frequency=1mo'
    driver.get(base_url_monthly_data_for_a_year)
    find_table_tbody = driver.find_element_by_css_selector('#Col1-1-HistoricalDataTable-Proxy > section > div.Pb\(10px\).Ovx\(a\).W\(100\%\) > table > tbody')
    stock_history = {}
    for row in find_table_tbody.find_elements_by_tag_name('tr'):
        tds = row.find_elements_by_tag_name('td')
        pre_date = tds[0].find_element_by_tag_name('span').text
        date = datetime.strptime(pre_date, '%b %d, %Y').strftime('%Y-%m-%d')
        close = tds[4].find_element_by_tag_name('span').text
        stock_history[date] = float(close.replace(',',''))

    # put two dataset into one dictionary
    stock_infos = []
    for date, price in zip(list(pre_stock_history.keys()), list(pre_stock_history.values())):
        stock_history[time.strftime('%Y-%m-%d', time.localtime(int(date)))] = price
    stock_infos.append(stock_history)
    return stock_infos

def scrape_summary_from_stock_api(collection):
    # DATASET 5: scrape company's summary from stock api
    url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/get-detail"
    headers = {'x-rapidapi-host': "apidojo-yahoo-finance-v1.p.rapidapi.com",'x-rapidapi-key': "af07fb24f3msh70060f8b18a57bcp14655fjsnfecc298458e1"}
    queries = {"region":"US","lang":"en","symbol":collection}
    response = get_res_from_stock_api(url, headers, queries, collection)
    # del response['summaryProfile']['maxAge']
    del response['summaryProfile']['companyOfficers']
    summary = response['summaryProfile']
    summary['address'] = summary['address1'] + ' ' + summary['address2'] + ' ' + summary['city'] + ' ' + summary['state'] + ' ' + summary['country'] if 'address2' in summary.keys() else summary['address1'] + ' ' + summary['city'] + ' ' + summary['state'] + ' ' + summary['country'] 
    return summary

def get_res_from_stock_api(url, headers, queries, collection):
    # Call from (def scrape_stock_from_stock_api) & (def scrape_summary_from_stock_api)
    querystring = queries
    request_string = requests.request("GET", url, headers=headers, params=querystring)
    response = json.loads(request_string.text)
    return response

def scrape(driver, each_company, collection):
    # Call from (def insert_database)
    summary = scrape_summary_from_stock_api(collection)
    revenue_history = scrape_revenue_from_yahoo_finance_and_csvFile(driver, each_company, collection)
    stock_infos = scrape_stock_from_yahoo_finance_and_stock_api(driver, each_company, collection)
    return summary, revenue_history, stock_infos

def insert_database(driver, collections, signin_url, base_url):
    # Call from (def main)
    # sign_in(driver, signin_url)
    search_companies = [base_url + company[:4] + '/financials?p=' + company if len(company) > 2 else base_url + company + '/financials?p=' + company for company in collections ]
    
    for each_company in search_companies:
        collection = each_company[48:] if len(each_company) == 50 else each_company[50:]
        summary, revenue_history, stock_infos = scrape(driver, each_company, collection)

        collections[collection].insert_many([
                { 'name': collection,
                  'summary': summary,
                  'revenue_infos': revenue_history,
                  'stock_infos': stock_infos
            }])
    return

def access_db(dbname, collectionnames):
    # Call from (def main)
    cluster = MongoClient('mongodb+srv://rimho:0000@cluster0-yehww.mongodb.net/test?retryWrites=true&w=majority')
    db = cluster[dbname]
    collections = {c:db[c] for c in collectionnames}
    return db, collections

def main():
    db, collections = access_db('yahoo_finance_db', ['GOOGL', 'FB', 'TWTR', 'SNAP'])
    driver = init_browser()
    signin_url = 'https://login.yahoo.com/config/login?.intl=us&.lang=en-US&.src=finance&.done=https%3A%2F%2Ffinance.yahoo.com%2F'
    base_url = 'https://finance.yahoo.com/quote/'
    insert_database(driver, collections, signin_url, base_url)
    print('Go To MongoDB')
    return 

main()

