import pandas as pd
import sqlite3
import json
import os
from fuzzywuzzy import process

def cdc_query(cas = None, chemical = None):
    if not cas and not chemical:
        raise ValueError("Must specify either CAS number and/or chemical name")
    chemical = chemical.upper() if chemical else None
    db_path = "/Users/jackbecker/xforce/CDC_Webscraping/tox_database.db"
    conn = sqlite3.connect(db_path)
    tables = ['clean_output']
    results = []
    with open("/Users/jackbecker/xforce/CDC_Webscraping/final_synonyms_dict.json", "r") as file:
        final_synonyms_dict = json.load(file)
    for table in tables:
        if cas:
            query = f"SELECT * FROM {table} WHERE CAS = ?"
            params = (cas,)
        elif chemical:
            chemical_cas_numbers = [cas for cas, names in final_synonyms_dict.items() if chemical in names]
            if len(chemical_cas_numbers) > 1:
                print(f"The chemical {chemical} corresponds to multiple CAS numbers: {chemical_cas_numbers}.")
                query = f"SELECT * FROM {table} WHERE CAS IN ({','.join(['?' for _ in chemical_cas_numbers])})"
                params = tuple(chemical_cas_numbers)  # Convert list to tuple
            elif not chemical_cas_numbers:
                matches = process.extract(chemical, [name for names in final_synonyms_dict.values() for name in names], limit=5)
                if matches:
                    suggested_chemicals = [match[0] for match in matches]
                    suggested_cas_numbers = [cas for cas, names in final_synonyms_dict.items() if any(name in suggested_chemicals for name in names)]
                    print(f"Did you mean one of these chemicals? {', '.join([f'{chemical} (CAS: {cas})' for chemical, cas in zip(suggested_chemicals, suggested_cas_numbers)])}")
                else:
                    print("Chemical not found. Please enter a valid CAS number and/or name")
                return
            else:
                query = f"SELECT * FROM {table} WHERE CAS = ?"
                params = (chemical_cas_numbers[0],)
        else:
            raise ValueError("Must specify either CAS number and/or chemical name (preferably CAS)")
        df = pd.read_sql_query(query, conn, params=params)
        results.append(df)
    df_final = pd.concat(results, ignore_index = True).drop_duplicates()
    conn.close()
    return df_final  

def trv_query(cas = None, chemical = None):
    if not cas and not chemical:
        raise ValueError("Must specify either CAS number and/or chemical name")
    chemical = chemical.upper() if chemical else None
    db_path = "/Users/jackbecker/xforce/CDC_Webscraping/tox_database.db"
    conn = sqlite3.connect(db_path)
    tables = ['TRV_ACGIH_TLV_dataframe', 'TRV_AIHA_WEEL_dataframe', 'TRV_NIOSH_REL_dataframe', 'TRV_EPA_LOC_dataframe',
             'TRV_DoE_PAC_dataframe', 'TRV_OSHA_PEL_dataframe', 'TRV_Cal_OSHA_PEL_dataframe', 'TRV_NRC_EEGL_dataframe',
             'TRV_EPA_AEGL_dataframe', 'TRV_OARS_WEEL_dataframe', 'TRV_AIHA_ERPG_dataframe'] 
    results = []
    with open("/Users/jackbecker/xforce/CDC_Webscraping/final_synonyms_dict.json", "r") as file:
        final_synonyms_dict = json.load(file)
    for table in tables:
        if cas:
            query = f"SELECT * FROM {table} WHERE CAS = ?"
            params = (cas,)
        elif chemical:
            chemical_cas_numbers = [cas for cas, names in final_synonyms_dict.items() if chemical in names]
            if len(chemical_cas_numbers) > 1:
                print(f"The chemical {chemical} corresponds to multiple CAS numbers: {chemical_cas_numbers}.")
                query = f"SELECT * FROM {table} WHERE CAS IN ({','.join(['?' for _ in chemical_cas_numbers])})"
                params = tuple(chemical_cas_numbers)
            elif not chemical_cas_numbers:
                matches = process.extract(chemical, [name for names in final_synonyms_dict.values() for name in names], limit=5)
                if matches:
                    suggested_chemicals = [match[0] for match in matches]
                    suggested_cas_numbers = [cas for cas, names in final_synonyms_dict.items() if any(name in suggested_chemicals for name in names)]
                    print(f"Did you mean one of these chemicals? {', '.join([f'{chemical} (CAS: {cas})' for chemical, cas in zip(suggested_chemicals, suggested_cas_numbers)])}")
                else:
                    print("Chemical not found. Please enter a valid CAS number and/or name")
                return
            else:
                query = f"SELECT * FROM {table} WHERE CAS = ?"
                params = (chemical_cas_numbers[0],)
        else:
            raise ValueError("Must specify either CAS number and/or chemical name (preferably CAS)")
        df = pd.read_sql_query(query, conn, params=params)
        results.append(df)
    df_final = pd.concat(results, ignore_index = True).drop_duplicates()
    conn.close()
    return df_final

def aux_query(cas = None, chemical = None):
    if not cas and not chemical:
        raise ValueError("Must specify either CAS number and/or chemical name")
    chemical = chemical.upper() if chemical else None
    db_path = "/Users/jackbecker/xforce/CDC_Webscraping/tox_database.db"
    conn = sqlite3.connect(db_path)
    tables = ['chemical_properties', 'ghs_classifications', 'markush_children', 'markush_parent', 'niosh_guidance', 'odor_threshold']
    results = {}
    with open("/Users/jackbecker/xforce/CDC_Webscraping/final_synonyms_dict.json", "r") as file:
        final_synonyms_dict = json.load(file)
    for table in tables:
        if cas:
            query = f"SELECT * FROM {table} WHERE CAS = ?"
            params = (cas,)
        elif chemical:
            chemical_cas_numbers = [cas for cas, names in final_synonyms_dict.items() if chemical in names]
            if len(chemical_cas_numbers) > 1:
                print(f"The chemical {chemical} corresponds to multiple CAS numbers: {chemical_cas_numbers}.")
                query = f"SELECT * FROM {table} WHERE CAS IN ({','.join(['?' for _ in chemical_cas_numbers])})"
                params = tuple(chemical_cas_numbers) 
            elif not chemical_cas_numbers:
                matches = process.extract(chemical, [name for names in final_synonyms_dict.values() for name in names], limit=5)
                if matches:
                    suggested_chemicals = [match[0] for match in matches]
                    suggested_cas_numbers = [cas for cas, names in final_synonyms_dict.items() if any(name in suggested_chemicals for name in names)]
                    print(f"Did you mean one of these chemicals? {', '.join([f'{chemical} (CAS: {cas})' for chemical, cas in zip(suggested_chemicals, suggested_cas_numbers)])}")
                else:
                    print("Chemical not found. Please enter a valid CAS number and/or name")
                return
            else:
                query = f"SELECT * FROM {table} WHERE CAS = ?"
                params = (chemical_cas_numbers[0],)
        else:
            raise ValueError("Must specify either CAS number and/or chemical name (preferably CAS)")
        df = pd.read_sql_query(query, conn, params = params)
        results[table] = df
    conn.close()
    return results

def query_all(cas_list = None, chemical_list = None):
    # Checking for input validity
    if not cas_list and not chemical_list:
        raise ValueError("Must specify either CAS numbers list and/or chemical names list")
    
    # Ensure both cas_list and chemical_list are lists (even if they contain only one item)
    if cas_list and not isinstance(cas_list, list):
        cas_list = [cas_list]
    if chemical_list and not isinstance(chemical_list, list):
        chemical_list = [chemical_list.upper() for chemical in chemical_list]

    # Looping over the provided CAS numbers and chemical names
    for idx, (cas, chemical) in enumerate(zip(cas_list or [None] * len(chemical_list), chemical_list or [None] * len(cas_list))):
        df_cdc = cdc_query(cas, chemical)
        df_trv = trv_query(cas, chemical)
        aux_data = aux_query(cas, chemical)
        
        # Creating a unique file name for each chemical/CAS 
        filename = f"/Users/jackbecker/xforce/CDC_Webscraping/query_all_output_{idx}.xlsx"
        
        # Ensure no overwrite happens
        counter = 1
        while os.path.exists(filename):
            filename = f"/Users/jackbecker/xforce/CDC_Webscraping/query_all_output_{idx}_{counter}.xlsx"
            counter += 1
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            if not df_cdc.empty:
                df_cdc.to_excel(writer, sheet_name='CDC Data', index=False)
            if not df_trv.empty:
                df_trv.to_excel(writer, sheet_name='TRV Data', index=False)
            for table, df in aux_data.items():
                if not df.empty:
                    df.to_excel(writer, sheet_name=table, index=False)
        
        print(f"Data for {chemical or cas} saved to {filename}")
