import requests
from bs4 import BeautifulSoup
import time
import json

# When I originally coded cdc_scrape.py, I forgot to scrape the primary chemical names from each header, so 
#   I made this file to re-scrape and merge these names with the rest of the synonyms.

def is_valid(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    if "Oops!" in soup.text:
        return False
    else:
        return True

def init_soup(url):
    req = requests.get(url)
    soup = BeautifulSoup(req.content, "html.parser")
    return soup

def get_url_1_all():
    url_1_all = []
    max_code = 686
    for code in range(0, max_code): 
        num_str = str(code).zfill(4)
        url = f"https://www.cdc.gov/niosh/npg/npgd{num_str}.html"
        if is_valid(url):
            url_1_all.append(url)
        time.sleep(1)
    return url_1_all

def get_url_2_all(url_1_all):
    url_2_all = []
    for url in url_1_all:
        soup = init_soup(url)
        card_texts = soup.find_all("div", class_ = "card-text")
        for text in card_texts:
            a_tag = text.find('a')
            if a_tag:
                url_2 = 'https://www.cdc.gov' + a_tag['href']
                url_2_all.append(url_2)
                break
    return url_2_all

def get_cas_no(url):
    soup = init_soup(url)
    tagged = soup.find_all(class_ = "card-text")
    if len(tagged) > 1:  
        cas = tagged[1].text.split('\n')[1]
        cas = cas.replace('\r', '')
        cas = cas.strip()
        return cas
    else:
        return "NOT FOUND"

def get_primary_name(url):
    soup = init_soup(url)
    h1 = soup.find_all("h1")
    if h1:
        return h1[0].text.strip().upper()  
    else:
        return "NOT FOUND"
    
def build_name_dict():
    url_1_all = get_url_1_all()
    url_2_all = get_url_2_all(url_1_all)
    prim_name_dict = {}  
    for url in url_2_all:
        cas = get_cas_no(url)
        name = get_primary_name(url) 
        if name is not "NOT FOUND":
            prim_name_dict[cas] = name 
        time.sleep(1)
    return prim_name_dict

prim_name_dict = build_name_dict()
with open('prim_name_dict.json', 'w') as f:
    json.dump(prim_name_dict, f)
