import threading
import os
import requests
from time import sleep, time
from collections import deque
import pandas as pd
df = pd.read_csv('wit_v1.train.all-1percent_sample.tsv', sep='\t')
data = df[['page_title', 'image_url']]
q = deque([[page_title, image_url] for page_title, image_url in data.values])

def prep_title(title):
    title = title.replace('/', ' #slash# ')
    title = title.replace('*', ' #star# ')
    return title

def download_image(url, path):
    r = requests.get(url, stream=True, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'})
    if r.status_code == 200:
        with open(path, 'wb') as f:
            for chunk in r:
                f.write(chunk)
    else:
        print(r)

class MyThread(threading.Thread):
 
    def __init__(self, number):
        threading.Thread.__init__(self)
        self.number = number
 
 
    def run(self):
        global q
        global t
        while len(q) > 0:
            if len(q) % 1000 == 0:
                print(len(q), time() - t, len(os.listdir()))
            page_title, image_url = q.pop()
            page_title = prep_title(page_title)
            download_image(image_url, f'{page_title}.jpg')
t = time()
while len(q) > 0:
    threads = []
    for i in range(10):
        threads.append(MyThread(i))
        threads[-1].start()
    for thread in threads:
        thread.join()
