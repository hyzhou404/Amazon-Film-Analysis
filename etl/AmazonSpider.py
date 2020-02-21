import csv
import ssl
import threading
import time
from functools import wraps
from time import sleep
from urllib import request

import pyautogui as pyautogui
import threadpool as threadpool
from bs4 import BeautifulSoup
import requests
import random
import urllib3


ip_list = []

def get_ip_list(url, headers):
    web_data = requests.get(url, headers=headers)
    soup = BeautifulSoup(web_data.text, 'lxml')
    ips = soup.find_all('tr')
    ip_list = []
    for i in range(1, len(ips)):
        ip_info = ips[i]
        tds = ip_info.find_all('td')
        ip_list.append(tds[1].text + ':' + tds[2].text)
    return ip_list


def get_random_ip(ip_list):
    ran_ip = random.choice(ip_list)

    proxy_ip = 'http://' + ran_ip
    # proxy_ip2 = 'https://' + ran_ip

    proxy = {'http': proxy_ip}
    # print(proxy)
    return proxy


def randHeader():
    head_connection = ['Keep-Alive', 'close']
    head_accept = ['text/html, application/xhtml+xml, */*']
    head_accept_language = ['zh-CN,fr-FR;q=0.5', 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3']
    head_user_agent = [
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:38.0) Gecko/20100101 Firefox/38.0",
            "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR 3.0.30729; .NET CLR 3.5.30729; InfoPath.3; rv:11.0) like Gecko",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)",
            "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
            "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
            "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
            "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
            "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; The World)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Avant Browser)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)"]


    header = {
        'Connection': head_connection[0],
        'Accept': head_accept[0],
        'Accept-Language': head_accept_language[1],
        'User-Agent': head_user_agent[random.randrange(0, len(head_user_agent))]
    }
    return header


def analysisHtml(soup, ASIN):
    data = {}

    # print(soup)
    # 初始化
    data["ASIN"] = ASIN
    data['title'] = ""
    data['dir'] = ""
    data['actor'] = ""
    data['date'] = ""
    data['genre'] = ""
    data['commit'] = ""
    data['format'] = ""
    data['language'] = ""
    data['studio'] = ""
    data['score'] = ""

    # =============================================================================
    #     print (soup)
    # =============================================================================
    title = soup.find('span', class_="a-size-large")
    title1 = soup.find('section', class_="av-detail-section")
    #黑底
    if title is None and title1 is not None:
        data['title'] = title1.h1.get_text()
        details = soup.find('div', class_="aiv-container-limited")
        trList = details.find_all('tr')
        for tr in trList:
            allTd = ""
            th = tr.get_text().strip()
            td = tr.find_all('a', class_="a-link-normal")
            for i in td:
                allTd += i.get_text() + ','
            allTd = allTd[:-1]
            if 'Director' in th:
                data['dir'] = allTd
            if 'Starring' in th:
                data['actor'] = allTd
            if 'Genres' in th:
                data['genre'] = allTd
            if 'Studio' in th:
                data['studio'] = allTd
            if 'Format' in th:
                data['format'] = allTd
            if 'Audio' in th:
                data['language'] = allTd
            if 'Genres' in th:
                data['genre'] = allTd
        findDate = soup.find_all('span', class_="DigitalVideoUI_Badge__text")
        data['date'] = findDate[2].get_text()
        data['commit'] = soup.find('a', id="dp-summary-see-all-reviews").find('h2').get_text()
        data['commit']=data['commit'][:-17]
        data['score'] = soup.find('span',class_="arp-rating-out-of-text a-color-base").get_text()
    # 白底
    elif title1 is None and title is not None:
        data['title'] = title.get_text().strip()
        detail = soup.find('div', id="detail-bullets")
        liList = detail.find_all('li')
        # dir1 = soup.find('b', text="Directors:")
        for li in liList:
            temb = li.find('b')
            tema = li.find_all('a')
            allTema = ""
            for i in tema:
                allTema += i.get_text() + ','
            allTema = allTema[:-1]
            if 'Directors:' in temb.get_text():
                data['dir'] = allTema
            if 'Actors:' in temb.get_text():
                data['actor'] = allTema
            if 'DVD Release Date:' in temb.get_text():
                data['date'] = li.get_text()
                data['date'] = data['date'][18:]
            if 'Format:' in temb.get_text():
                data['format'] = li.get_text()
            if 'Language:' in temb.get_text():
                data['language'] = li.get_text()
            if 'Studio:' in temb.get_text():
                data['studio'] = li.get_text()
        data['genre']=""
        commit = soup.find('a', id="dp-summary-see-all-reviews").find('h2')
        data['score'] = soup.find('span',class_="arp-rating-out-of-text a-color-base").get_text()
        data['commit']=commit.get_text()
        data['commit']=data['commit'][:-17]
    # 机器人检测
    else:
        print("robot")
        return "r"
    wdata = [data["ASIN"], data['title'], data['dir'], data['actor'], data['date'], data['genre'], data['commit'], data['format'], data['language'], data['studio'], data['score']]

    if data['dir'] is "":
        return ""
    return wdata


def mains(filename):
    fr = open(filename, "r")
    # k = filename[61:62]
    # #print(k)
    # fw = open(r"D:\1_Study\DataWarehouse_Patrick_eBay\homework\rescsv\res"+k+".csv", "a+")
    #print(fw)
    fw = open(r"D:\1_Study\DataWarehouse_Patrick_eBay\homework\rescsv\res.csv", "a+")
    urlList = fr.readlines()
    count = 0
    proxy = get_random_ip(ip_list)

    fwriter = csv.writer(fw, dialect='excel', lineterminator='\n')
    fwriter.writerow(["ASIN", "name", "director", "actors", "date", "genre", "commits", "format", "language", "studio", "score"])
    for url in urlList:
        header = randHeader()

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        try:
            requests.adapters.DEFAULT_RETRIES = 5
            s = requests.session()
            s.keep_alive = False
            # print(proxy)
            resp = s.get(url, headers=header, proxies=proxy, timeout=40, verify=False)
            # print(proxy)
            # print(resp.text)
            soup = BeautifulSoup(resp.text, "lxml")

            # fw.write(analysisHtml(soup) + '\n')
            fwriter.writerow(analysisHtml(soup))
            time.sleep(random.randint(2, 5))
        except Exception as e:
            print(e)
            continue
        count += 1
        print(count)

    fr.close()
    fw.close()



def visitWeb(url):
    fw = open(r"D:\1_Study\DataWarehouse_Patrick_eBay\homework\rescsv\res-t.csv", "a+")
    fwriter = csv.writer(fw, dialect='excel', lineterminator='\n')
    header = randHeader()
    # proxy = get_random_ip(ip_list)

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    try:
        requests.adapters.DEFAULT_RETRIES = 5
        s = requests.session()
        s.keep_alive = False
        s.cookies.clear()
        s.headers=header
        resp = s.get(url, timeout=35)

        soup = BeautifulSoup(resp.text, "lxml")

        # fw.write(analysisHtml(soup) + '\n')
        text = analysisHtml(soup,url[26:])
        if text is "r":
            ferror = open("error.txt", "a+")
            ferror.write(url)
            ferror.close()
        else:
            fwriter.writerow(text)
        # time.sleep(random.randint(2, 5))
    except Exception as e:
        print(e)
        ferror = open("error.txt", "a+")
        ferror.write(url)
        ferror.close()

    fw.close()

def multi(filename):
    counter = 0
    threadNum = 100
    # fileNum = 400
    websites = []
    threads = []
    with open(filename) as rf:
        for line in rf:
            if counter is not threadNum:
                websites.append(line)
                counter += 1
            if counter is threadNum:
                print(counter)
                for website in websites:
                    th = threading.Thread(target=visitWeb, args=(website, ), daemon=True)
                    threads.append(th)
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()  # wait for all thread in threads are finished
                websites.clear()
                threads.clear()
                counter = 0


    pyautogui.hotkey('ctrl','q')
    time.sleep(10)  # wait for ip changed

if __name__ == '__main__':
    for i in range(0, 2):
        multi(r"D:\1_Study\DataWarehouse_Patrick_eBay\homework\m5\e" + str(i) + ".txt")
        print(i)

