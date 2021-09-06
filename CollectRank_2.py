import csv
import requests
from random import randint
from bs4 import BeautifulSoup as bs
from time import time, sleep
import time
import os
import threading

thread_limit = 20
lock_file = threading.Lock()

def scrap_data(brand):
    global brands_num
    collected_data = []
    collected_data.append(brand)
    error = 0
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'
    }
    while True:
        try:
            s = requests.session()
            url = 'https://www.macroaxis.com/invest/advice/'+brand
            #res = s.get(url)
            res = s.get(url, headers=headers)
            while res.status_code in [ 403, 408, 500, 502, 503, 505]:
                with lock_file:
                    print('Server Overflow occur sleep for seconds...')
                sleep(randint(5, 15))
                res = s.get(url)
            parser = bs(res.content, 'html.parser')
            website1 = parser.find('div', {"class": "adviseText"}).text
            collected_data.append(website1)
            break
        except ( requests.exceptions.SSLError, requests.exceptions.RequestException, requests.exceptions.ConnectionError, requests.exceptions.HTTPError, requests.exceptions.Timeout) as e:
            error += 1
            if error == 3 :
                with lock_file:
                    print("Not woriking URL:",url)
                    with open('invalid_url.csv', 'a',newline='') as writeFile:
                        writer = csv.writer(writeFile)
                        writer.writerow([url])
                break
            else:
                continue

        except (IndexError, AttributeError) as e:
            collected_data.append(None)
            with lock_file:
                print("Not woriking URL:",url)
                with open('invalid_url.csv', 'a',newline='') as writeFile:
                    writer = csv.writer(writeFile)
                    writer.writerow([url])
            break
    
    error = 0
    while True:
        try:
            url = 'https://www.barchart.com/stocks/quotes/%s/overview'%brand
            res = s.get(url, headers=headers)
            #res = s.get(url)
            while res.status_code in [ 403, 408, 500, 502, 503, 505]:
                with lock_file:
                    print('Server Overflow occur sleep for seconds...')
                sleep(randint(5, 15))
                res = s.get(url)
            parser = bs(res.content, 'html.parser')
            website2 = parser.find('div', {"class": "technical-opinion-widget clearfix"}).a.text.strip()
            collected_data.append(website2)
            break
        except ( requests.exceptions.SSLError, requests.exceptions.RequestException, requests.exceptions.ConnectionError, requests.exceptions.HTTPError, requests.exceptions.Timeout) as e:
            error += 1
            if error == 3 :
                with lock_file:
                    print("Not woriking URL:",url)
                    with open('invalid_url.csv', 'a',newline='') as writeFile:
                        writer = csv.writer(writeFile)
                        writer.writerow([url])
                break
            else:
                continue

        except (IndexError, AttributeError) as e:
            collected_data.append(None)
            with lock_file:
                print("Not woriking URL:",url)
                with open('invalid_url.csv', 'a',newline='') as writeFile:
                    writer = csv.writer(writeFile)
                    writer.writerow([url])
            break
        
    
    
    error = 0    
    while True:
        try:
            url = 'https://stockinvest.us/technical-analysis/'+brand
            res = s.get(url, headers=headers)
            while res.status_code in [ 403, 408, 500, 502, 503, 505]:
                with lock_file:
                    print('Server Overflow occur sleep for seconds...')
                sleep(randint(5, 15))
                res = s.get(url)
            parser = bs(res.content, 'html.parser')
            website3 = parser.find('span', {"class": "hidden-sm-down"}).text.split('as')[1].strip()
            collected_data.append(website3)
            break
        except ( requests.exceptions.SSLError, requests.exceptions.RequestException, requests.exceptions.ConnectionError, requests.exceptions.HTTPError, requests.exceptions.Timeout) as e:
            error += 1
            if error == 3 :
                with lock_file:
                    print("Not woriking URL:",url)
                    with open('invalid_url.csv', 'a',newline='') as writeFile:
                        writer = csv.writer(writeFile)
                        writer.writerow([url])
                break
            else:
                continue

        except (IndexError, AttributeError) as e:
            collected_data.append(None)
            with lock_file:
                print("Not woriking URL:",url)
                with open('invalid_url.csv', 'a',newline='') as writeFile:
                    writer = csv.writer(writeFile)
                    writer.writerow([url])
            break
    
    error = 0
    while True:
        try:
            url = 'https://www.zacks.com/stock/quote/'+brand
            res = s.get(url, headers=headers)
            while res.status_code in [ 403, 408, 500, 502, 503, 505]:
                with lock_file:
                    print('Server Overflow occur sleep for seconds...')
                sleep(randint(5, 15))
                res = s.get(url)
            parser = bs(res.content, 'html.parser')
            website4 = parser.find('p', {"class": "rank_view"}).text.strip().split()[0]
            collected_data.append(website4)
            break
        except ( requests.exceptions.SSLError, requests.exceptions.RequestException, requests.exceptions.ConnectionError, requests.exceptions.HTTPError, requests.exceptions.Timeout) as e:
            error += 1
            if error == 3 :
                with lock_file:
                    print("Not woriking URL:",url)
                    with open('invalid_url.csv', 'a',newline='') as writeFile:
                        writer = csv.writer(writeFile)
                        writer.writerow([url])
                break
            else:
                continue

        except (IndexError, AttributeError) as e:
            collected_data.append(None)
            with lock_file:
                print("Not woriking URL:",url)
                with open('invalid_url.csv', 'a',newline='') as writeFile:
                    writer = csv.writer(writeFile)
                    writer.writerow([url])
            break
    if len(collected_data) > 1:
        print(collected_data)       
        with open(file_name+'.csv', 'a', newline='') as writeFile:
            writer = csv.writer(writeFile)
            writer.writerow(collected_data)
                
    brands_num -= 1
    with lock_file:
        print('Remaining Brands: ', brands_num)
            

row = ['Ticker', 'https://www.macroaxis.com/', 'https://www.barchart.com/', 'https://stockinvest.us/', 'https://www.zacks.com/']
file_name = 'InvestData_'+time.strftime("%Y%m%d%H%M%S")+'_'
start_time = time.time()
with open(file_name+'.csv', 'w',newline='') as writeFile:
        writer = csv.writer(writeFile)
        writer.writerow(row)
        
# clear content of not working URLS File during start
with open('invalid_url.csv', 'w',newline='') as writeFile:
        writer = csv.writer(writeFile)
        writer.writerow([])

        
# s = requests.session()
with open('CollectDataList.txt', 'r') as file:
    reader = csv.reader(file)
    for line in reader:
        brands_num = len(line)
        print('Totale Brands: ',brands_num)
        for brand in line:
            t = threading.Thread(target=scrap_data, args=(brand,))
            t.setDaemon = True
            t.start()
            while threading.activeCount() >= (thread_limit+4):
                sleep(1)
            
taken_time = str(round(time.time()-start_time))
full_file_name = file_name + taken_time
os.rename(file_name+'.csv', full_file_name+'.csv')
