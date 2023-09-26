import pandas as pd
import requests
from bs4 import BeautifulSoup
import time

max_code = 686 # SPECIFIES THE TOTAL NUMBER OF URLS TO VISIT

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

def get_rtecs_no_2(url_2):
    soup = init_soup(url_2)
    tagged = soup.find_all(class_ = "card-text")
    if tagged:
        rtecs_no = tagged[0].text.strip()
        return rtecs_no
    else:
        return "NOT FOUND"

def get_cas_no(url):
    soup = init_soup(url)
    tagged = soup.find_all(class_= "card-text")
    if len(tagged) > 1: 
        cas = tagged[1].text.split('\n')[1]
        cas = cas.replace('\r', '')
        cas = cas.strip()
        return cas
    else:
        return "NOT FOUND"

def get_mol_weight(url):
    soup = init_soup(url)
    tagged = soup.find_all("div", class_ = "card-text")
    if len(tagged) > 3:
        return tagged[3].string.strip()
    else:
        return "NOT FOUND"

def get_synonyms(url):
    soup = init_soup(url)
    tagged = soup.find_all("div", class_ = "card-text")
    if len(tagged) >= 6: 
        synonyms_html = str(tagged[5])
        synonyms = synonyms_html.split('<br/>')
        cleaned_synonyms = [BeautifulSoup(s, "html.parser").get_text().strip() for s in synonyms]
        return cleaned_synonyms
    else:
        return []

def get_tox(url):
    soup = init_soup(url)
    h2_tag = soup.find("h2", text = "Acute Toxicity Data and References")
    if h2_tag: 
        table = h2_tag.find_next("table", class_ = "table table-striped")
        if table: 
            headers = [header.get_text().strip() for header in table.find_all("th")]
            rows = table.find_all("tr")
            table_data = []
            for row in rows[1:]:
                cols = row.find_all("td")
                if len(cols) >= 3:
                    data_dict = {
                        "ROUTE/ORGANISM": cols[0].get_text().strip(), 
                        "DOSE": cols[1].get_text().strip(), 
                        "EFFECT": cols[2].get_text().strip() if cols[2].get_text().strip() else 'NO EFFECT REPORTED' 
                    }
                    for key in data_dict:
                        data_dict[key] = ' '.join(data_dict[key].split())
                    table_data.append(data_dict)
            df = pd.DataFrame(table_data)
            return df
    return pd.DataFrame()

def build_df():
    url_1_all = get_url_1_all()
    url_2_all = get_url_2_all(url_1_all)
    rtecs_all = []
    cas_all = []
    mol_weight_all = []
    syn_all = []
    route_org_all = []
    dose_all = []
    effect_all = []
    for url in url_2_all:
        rtecs_no = get_rtecs_no_2(url)
        cas = get_cas_no(url)
        mol_weight = get_mol_weight(url)
        syn = get_synonyms(url)
        tox = get_tox(url)
        for i in range(0, len(tox)):
            tox_row = tox.iloc[i]
            route_org_all.append(tox_row["ROUTE/ORGANISM"])
            dose_all.append(tox_row["DOSE"])
            effect_all.append(tox_row["EFFECT"])
            rtecs_all.append(rtecs_no)
            cas_all.append(cas)
            mol_weight_all.append(mol_weight)
            syn_all.append(syn)
        time.sleep(1)
    data = {
        'RTECS': rtecs_all, 
        'CAS': cas_all,
        'MOLECULAR WEIGHT': mol_weight_all,
        "SYNONYMS": syn_all,
        "ROUTE/ORGANISM": route_org_all,
        "DOSE": dose_all,
        "EFFECT": effect_all
    }
    df = pd.DataFrame(data)
    print(df)
    df.to_csv("output.csv", index = False)

build_df()
