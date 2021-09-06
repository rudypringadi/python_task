import os
import csv, math
import asyncio
import time
import aiohttp
import configparser, operator
from pathlib import Path
from datetime import datetime

key = 'I1C1R481B394CN6K'    #real key
#key = 'YCN9XC476F4IYT8Q'    # rudy key demo tibak e
#key = 'UHY6J7LANSK6GJX1'    #demo key

dstfldrCOMPACT = "Result_Compact"
dstfldrFULL = "Result_Full"
period = 1
TopWin = 5
TopLose = 5
TopWinPrice = 5
TopLossPrice = 5
TopVolumeChange = 5
time_end = datetime.strptime('11:00:00', '%H:%M:%S')
time_start = datetime.strptime('09:30:00', '%H:%M:%S')
getDay = 0
mtype = "COMPACT"  # value is COMPACT or FULL
debug = 0       # for debug used
returnswin = []
returnslose = []
returns = []
hist = []
Tolarance = 5


async def download_site(session, url, tick):
    async with session.get(url) as response:
        isi = await response.text()
        #print(debug)
        if debug == 1:
            if mtype == "COMPACT":
                fl = "{}\intraday_1min_HistCompact_{}.csv".format("Hist", tick)
            else:
                fl = "{}\intraday_1min_HistFULL_{}.csv".format("Hist", tick)
            f = open(fl, 'w')
            isi = isi.replace("\n", "")
            f.write(isi)
            f.close()
            #print(isi)
        else:
            if isi != '':
                get_RSI(tick, isi)
            #if mtype == "COMPACT":
            #    fl = "{}\intraday_1min_COMPACT_get_{}.csv".format(dstfldrCOMPACT, tick)
            #else:
            #    fl = "{}\intraday_1min_FULL_get_{}.csv".format(dstfldrFULL, tick)
            #print(hist)
            #print(len(hist))
            #with open(fl, 'w',newline='') as writeFile:
            #    writer = csv.writer(writeFile)
            #    for m in hist:
            #        writer.writerow(m)
            
        #print("Read {0} from {1}".format(response.content_length, url))


async def download_all_sites(sites):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for tick in sites:
            url = sites[tick]
            task = asyncio.ensure_future(download_site(session, url, tick))
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)

def get_links(urldownload):
    links = {}
    with open('CollectDataList.txt', 'r') as file:
        reader = csv.reader(file)
        cont = 0
        for line in reader:
                brands_num = len(line)
                print('Totale Brands: ',brands_num)
                for brand in line:
                    links[brand] = urldownload.replace("{TICK}", brand)
                    cont += 1
                    #if cont == 1:
                    #  break;

    return links

def setup_download_dir(mfolder):
    download_dir = Path(mfolder)
    if not download_dir.exists():
        download_dir.mkdir()
    return download_dir / os.path.basename("")


def get_RSI(tick, isi):
        misi = isi.splitlines()
        csv_reader = csv.reader(isi.splitlines())
        cols = []
        cols.append(tick)
        for x in range(10):
            cols.append("")
        closeprice = []
        brs = 0
        xCnt = 0
        for row in csv_reader:
            if brs > 0:
                
                row_date = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
                
                if row_date.timestamp() <= time_end.timestamp() and row_date.timestamp() >= time_start.timestamp():
                        
                    mprice = float(row[4])
                    mtime = row_date.time()
                    
                    cols.append( "{}_{}".format(mtime, mprice))
                    closeprice.append(mprice)
                    xCnt += 1
                
                if xCnt > period :
                    break
            else:
                if row[0] != 'timestamp':
                    break
            brs += 1
        
        #for mp in cols:
        #   print(mp)
        Gain = 0
        Lose = 0
        AVGGain = 0
        AVGLose = 0
        xCnt = 0
        oldPrice = 0
        RSIBefore = 0
        brs = 0
        for mp in closeprice:
            #print(mp)
            if brs == 0:
                oldPrice = mp
            else:
                Chg = mp - oldPrice
                oldPrice = mp
                
                if Chg >= 0:
                    Gain += Chg
                else:
                    Lose += abs(Chg)
                xCnt += 1
            
            if xCnt == 14:
                AVGGain = Gain/14
                AVGLose = Lose/14
                
            else:
                if xCnt > 14:
                    if Chg >= 0:
                        Gain = Chg
                        Lose = 0
                    else:
                        Lose = abs(Chg)
                        Gain = 0
                    AVGGain = ((AVGGain * 13) + Gain)/14
                    AVGLose = ((AVGLose * 13) + Lose)/14
            if brs > 0:
                if cols[1] != '':
                    cols[1] = ";" + cols[1]
                if cols[2] != '':
                        cols[2] = ";" + cols[2]
                if cols[3] != '':
                        cols[3] = ";" + cols[3]
                cols[1] = str(round(Chg, 2)) + cols[1]
                if Chg >= 0:
                    cols[2] = str(round(Chg, 2)) + cols[2]
                    cols[3] = '0' + cols[3]
                else:
                    cols[2] = '0' + cols[2]
                    cols[3] = str(abs(round(Chg, 2))) + cols[3]
                    
                if xCnt > 13:
                    RS = AVGGain/AVGLose
                    if AVGLose == 0:
                        RSI = 100
                    else:
                        RSI = 100 - (100/(1 + RS))
                        
                    if cols[4] != '':
                        cols[4] = ";" + cols[4]
                    if cols[5] != '':
                        cols[5] = ";" + cols[5]
                    if cols[6] != '':
                        cols[6] = ";" + cols[6]
                    if cols[7] != '':
                        cols[7] = ";" + cols[7]
                    cols[4] = str(round(AVGGain ,2 )) + cols[4]
                    cols[5] = str(round(AVGLose, 2)) + cols[5]
                    cols[6] = str(round(RS, 2)) + cols[6]
                    cols[7] = str(round(RSI, 2)) + cols[7]
                    #print("RSI->" + str(RSI))
                    cols[8] = RSI
                    cols[9] = RSI - RSIBefore
                    RSIBefore = RSI
                    
            brs += 1
        if cols[9] == '':
            cols[9] = 0
        if cols[20] != '':
            hist.append(cols)
            #print('OK for ' + tick)
        



if os.path.exists('config.cfg'):
        #if file config.cfg exists will get from this file , else default
        config = configparser.ConfigParser()
        config.read('config.cfg')
        section = config['Config']
        collectRow = int(section['collectRow'])
        mtype = section['Type'].strip()        #for get realtime did not need FULL
        period = int(section['Period'])
        debug = int(section['Debug'])
        TopWin = int(section['TopWin'])
        TopLose = int(section['TopLose'])
        TopVolumeChange = int(section['TopVolumeChange'])
        TopWinPrice = int(section['TopWinPrice'])
        TopLossPrice = int(section['TopLossPrice'])
        getDay = int(section['getDay'])
        time_end = datetime.strptime(section['EndTime'], '%H:%M')
        time_start = datetime.strptime(section['StartTime'], '%H:%M')
        Tolarance = int(section['Tolarance'])
        


if mtype == "COMPACT":
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={TICK}&interval=1min&apikey=%s&datatype=csv'%key
else:
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={TICK}&interval=1min&outputsize=full&apikey=%s&datatype=csv'%key
        
if __name__ == "__main__":
    #print(url)
    dstfldrCOMPACT = setup_download_dir(dstfldrCOMPACT)
    dstfldrFULL = setup_download_dir(dstfldrFULL)
    
            
    if mtype == "COMPACT":
        fnew = "{}\\result_RSI_COMPACT_Data_Period_{}_{}.csv".format(dstfldrCOMPACT, str(period), time.strftime("%Y%m%d%H%M%S"))
        dstfldr = dstfldrCOMPACT
    else:
        fnew = "{}\\result_RSI_FULLDAY_Data_Period_{}_{}.csv".format(dstfldrFULL, str(period), time.strftime("%Y%m%d%H%M%S"))
        dstfldr = dstfldrFULL
    
    start_time = time.time()
    sites = get_links(url)
    asyncio.get_event_loop().run_until_complete(download_all_sites(sites))
    if len(hist) > 0:
        sorten = sorted(hist, key=operator.itemgetter(9), reverse = True)
        # print(sorten)
        with open(fnew, 'w',newline='') as writeFile:
            writer = csv.writer(writeFile)
            writer.writerow(['Ticker', 'Change', 'Gain', 'Loss', 'Av Gain', 'Av Loss', 'RS', 'RSI', 'RSI1', 'RSI1-RSI2', 'RSM1;RSM2;RSM3…..RS', 'Current Minute', 'Minute 1', 'Minute 2', 'Minute 3', 'Minute 4', 'Minute 5', 'Minute 6', 'Minute 7', 'Minute 8', 'Minute 9', 'Minute 10', 'Minute 11', 'Minute 12', 'Minute 13', 'Minute 14', 'Minute 15', 'Minute 16', 'Minute 17', 'Minute 18', 'Minute 19', 'Minute 20', 'Minute 21', 'Minute 22', 'Minute 23', 'Minute 24', 'Minute 25', 'Minute 26', 'Minute 27', 'Minute 28', 'Minute 29', 'Minute 30'])
            for m in sorten:
                writer.writerow(m)

    
    duration = time.time() - start_time
    
    if debug:
        print(f"Downloaded {len(sites)} ticks in {duration} seconds")
    else:
        print(f"Process {len(sites)} ticks in {duration} seconds")
