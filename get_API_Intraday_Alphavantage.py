import csv
import requests
import threading
from time import time, sleep


def get_api(tick):
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=%s&interval=1min&outputsize=full&apikey=UHY6J7LANSK6GJX1&datatype=csv'%tick
    
    try:
        ses = requests.session()
        res = ses.get(url)
        isi = res.text
        #print (isi)
        if len(isi) > 100:
            f = open('D:\EDI2\Invest\GetStock_API\Result\intraday_1min_FULL_'+tick+'.csv', 'w')
            isi = isi.replace("\n", "")
            f.write(isi)
            f.close()
            print(tick + " downloaded")
            
                        
    except ( requests.exceptions.SSLError, requests.exceptions.RequestException, requests.exceptions.ConnectionError, requests.exceptions.HTTPError, requests.exceptions.Timeout) as e:
        error += 1
        if error == 3:
            print("Not woriking URL1:",url)
            with open('invalid_url.csv', 'a',newline='') as writeFile:
                writer = csv.writer(writeFile)
                writer.writerow([url])
            error = 0
          
    except (IndexError, AttributeError) as e:
        with open('invalid_url.csv', 'a',newline='') as writeFile:
            writer = csv.writer(writeFile)
            writer.writerow([url])
        

# clear content of not working URLS File during start
with open('invalid_url.csv', 'w',newline='') as writeFile:
        writer = csv.writer(writeFile)
        writer.writerow([])

cc = 0
maxtick = 5
maxtime = 75
cf = 0


with open('CollectDataList.txt', 'r') as file:
    reader = csv.reader(file)
    for line in reader:
        brands_num = len(line)
        print('Totale Brands: ',brands_num)
        for brand in line:
            if cc == 0:
                threads = []
            brand = brand.strip()
            t = threading.Thread(target=get_api, args=(brand,))
            threads.append(t)
            cc += 1
            cf += 1
            if cc == maxtick or cf == brands_num:
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()
                print ("get ", maxtick , " ticks is done wait for %s seconds..."%maxtime)
                sleep(maxtime) # Delay for maxtime seconds
                cc = 0
            #if cf == 10:
            #        break
print ("total downloaded", cf)
