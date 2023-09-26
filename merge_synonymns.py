import sqlite3
import json

def get_unique_synonyms():
    db_path = "/Users/jackbecker/xforce/CDC_Webscraping/tox_database.db"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    synonyms_dict = {}
    with open('primary_name_dict.json', 'r') as f:
        primary_name_dict = json.load(f)
    cur.execute("SELECT * FROM synonyms;")
    rows = cur.fetchall()
    for row in rows:
        cas_numbers = str(row[1]).strip().split(";")
        for cas_number in cas_numbers:
            if cas_number != "None":
                synonym1 = row[2] if row[2] else ''
                synonym2 = row[3] if row[3] else ''
                synonyms = synonym1 + ';' + synonym2
                synonyms = synonyms.upper()
                if synonyms:
                    synonyms_list = synonyms.split(';')
                    for synonym in synonyms_list:
                        if synonym:
                            synonyms_dict.setdefault(cas_number, set()).add(synonym)

    cur.execute("SELECT * FROM clean_output;")
    rows = cur.fetchall()
    for row in rows:
        cas_numbers = str(row[2]).strip().split(";")
        for cas_number in cas_numbers:  
            if cas_number != "None":
                synonyms = row[4].upper()
                if synonyms:
                    synonyms = [syn.strip() for syn in synonyms.strip("[]").split(',')]
                    synonyms_dict.setdefault(cas_number, set()).update(synonyms)
    for cas_number, chem_name in primary_name_dict.items():
        cas_numbers = cas_number.split(";")
        for cas_number in cas_numbers:
            if cas_number != "None":
                synonyms_dict.setdefault(cas_number, set()).add(chem_name)

    for cas_number in synonyms_dict:
        synonyms_dict[cas_number] = list(synonyms_dict[cas_number])
    conn.close()
    return synonyms_dict


def export_dict_to_json(dictionary, file_path):
    with open(file_path, 'w') as jsonfile:
        json.dump(dictionary, jsonfile)

export_dict_to_json(get_unique_synonyms(), '/Users/jackbecker/xforce/CDC_Webscraping/final_synonyms_dict.json')
