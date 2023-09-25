import pandas as pd
import re
import warnings

# Suppress an intel warning that doesn't matter
warnings.filterwarnings("ignore")

# Notes:
# Around a dozen entries were manually processed after running cdc_clean.py. A few of these were removed
#    for not meeting the criteria of an acute toxicity study.
# Clean up dictionary/elminate entries that are not needed.

type_dict = {
    "lethal concentration (50 percent kill)": "LC50",
    "lethal concentration equivalent": "LCEQ",
    "lowest reported lethal concentration": "LCLO",
    "lethal dose (50 percent kill)": "LD50",
    "lowest reported lethal dose": "LDLO",
    "lower explosive limit": "LEL",
    "level of concern": "LOC",
    "permissible exposure limit": "PEL",
    "toxic concentration lowest": "TCLO",
    "toxic dose lowest": "TDLO",
    "temporary emergency exposure limit": "TEEL",
    "toxicology excellence for risk assessment": "TERA",
    "threshold limit value": "TLV",
    "workplace environmental exposure level": "WEEL",
    "emergency exposure guidance level": "EEGL",
    "acute exposure guideline level": "AEGL",
    "ceiling limit": "C",
    "inhibitor concentration low": "ICL",
    "lowest published toxic dose": "LPTD",
    "toxic concentration": "TC",
    "lowest published lethal concentration": "LCLO",
    "lowest published lethal dose": "LDLO",
    "lethal concentration": "LC", # is this a correct abbr?
    "lethal dose": "LD", # abbr?
    "inhibitor concentration (5 percent kill)": "IC5",
    "inhibitor concentration (10 percent kill)": "IC10",
    "inhibitor concentration (15 percent kill)": "IC15",
    "inhibitor concentration (20 percent kill)": "IC20",
    "inhibitor concentration (25 percent kill)": "IC25",
    "inhibitor concentration (30 percent kill)": "IC30",
    "inhibitor concentration (35 percent kill)": "IC35",
    "inhibitor concentration (40 percent kill)": "IC40",
    "inhibitor concentration (45 percent kill)": "IC45",
    "inhibitor concentration (50 percent kill)": "IC50",
    "inhibitor concentration (55 percent kill)": "IC55",
    "inhibitor concentration (60 percent kill)": "IC60",
    "inhibitor concentration (65 percent kill)": "IC65",
    "inhibitor concentration (70 percent kill)": "IC70",
    "inhibitor concentration (75 percent kill)": "IC75",
    "inhibitor concentration (80 percent kill)": "IC80",
    "inhibitor concentration (85 percent kill)": "IC85",
    "inhibitor concentration (90 percent kill)": "IC90",
    "inhibitor concentration (95 percent kill)": "IC95",
    "inhibitor concentration": "IC"
}

route_dict = {
    "INTRAPERITONEAL": "IP",
    "INTRAVENOUS": "IV",
    "SKIN": "DERMAL"
}

def parse_dose(dose):
    dose = dose.lower()
    match = re.match(r'([^:]+): (>?)([\d.]+) (.*?)(/\d+H)?$', dose)
    if not match:
        return pd.Series({'TYPE': 'NA', 'VALUE': 'NA', 'UNITS': 'NA', 'TIME (HR)': 'NA'})
    type_, gt, value, unit, time = match.groups()
    type_abbr = 'NA'
    for key, dict_value in type_dict.items():
        if key in type_:
            type_abbr = dict_value
            break
    unit_split = unit.split('/')
    parts_per = any(unit.startswith(pp) for pp in ['ppm', 'ppb', 'pph', 'ppt'])
    if parts_per and len(unit_split) == 1:
        unit = unit_split[0]
        time = 'NA'
    elif parts_per and len(unit_split) == 2:
        unit = unit_split[0]
        time = unit_split[1]
    elif parts_per and len(unit_split) == 3:
        return pd.Series({'TYPE': 'DROP_ROW', 'VALUE': 'DROP_ROW', 'UNITS': 'DROP_ROW', 'TIME (HR)': 'DROP_ROW'})
    elif(len(unit_split)) == 1:
        unit = unit_split[0]
        time = 'NA'
    elif len(unit_split) == 2:
        unit = unit_split[0] + '/' + unit_split[1]
        time = "NA"
    elif len(unit_split) == 3:
        unit = unit_split[0] + '/' + unit_split[1]
        time = unit_split[2]
    elif len(unit_split) == 4:
        return pd.Series({'TYPE': 'DROP_ROW', 'VALUE': 'DROP_ROW', 'UNITS': 'DROP_ROW', 'TIME (HR)': 'DROP_ROW'})
    else:
        unit = 'NA' 
        time = 'NA'
    return pd.Series({
        'TYPE': type_abbr,
        'VALUE': f"{gt}{value}",
        'UNITS': unit.strip(),
        'TIME (HR)': time if time != 'NA' else 'NA'
    })

def convert_time_to_hours(time):
    if pd.isnull(time) or time == "NA":
        return time
    time_regex = re.compile(r'(\d+\.?\d*)([A-Za-z]+)')
    match = time_regex.match(time)
    if match:
        number, unit = match.groups()
        number = float(number)
        unit = unit.upper()
        if unit in ['H', 'HR', 'HRS', 'HOUR', 'HOURS']:
            return number
        elif unit in ['D', 'DS', 'DAY', 'DAYS']:
            return number * 24
        elif unit in ['M', 'MS', 'MIN', 'MINS', 'MINUTE', 'MINUTES']:
            return number / 60
        elif unit in ['S', 'SEC', 'SECS', 'SECOND', 'SECONDS']:
            return number / 3600
        elif unit in ['W', 'WK', 'WKS', 'WEEK', 'WEEKS']:
            return number * 24 * 7
        elif unit in ['MO', 'MON', 'MONS', 'MONTH', 'MONTHS']:
            return number * 24 * 30
        elif unit in ['Y', 'YR', 'YRS', 'YEAR', 'YEARS']:
            return number * 24 * 365
    return "NA"

def duplicate_cas(df):
    def split_cas(row):
        if isinstance(row['CAS'], str):
            cas_numbers = row['CAS'].split(';')
            new_rows = pd.DataFrame([row]*len(cas_numbers))
            new_rows['CAS'] = cas_numbers
            return new_rows
        else:
            return pd.DataFrame([row])
    df_series = df.apply(split_cas, axis = 1)
    new_df = pd.concat(df_series.values).reset_index(drop = True)
    return new_df

def clean():
    df = pd.read_csv("/Users/jackbecker/xforce/CDC_Webscraping/scrape_output.csv")
    df['ROUTE/ORGANISM'] = df['ROUTE/ORGANISM'].str.upper()
    df[['ROUTE', 'SPECIES']] = df['ROUTE/ORGANISM'].str.split('/', expand = True)
    del df['ROUTE/ORGANISM']
    df['ROUTE'] = df['ROUTE'].replace(route_dict)
    df = df.join(df['DOSE'].apply(parse_dose))
    df = df[df['TYPE'] != 'DROP_ROW']
    del df['DOSE']
    df = df.apply(lambda s: s.str.upper() if s.name != 'UNITS' else s)
    df['TIME (HR)'] = df['TIME (HR)'].apply(convert_time_to_hours)
    df = duplicate_cas(df)
    df['UNITS'] = df['UNITS'].map(lambda x: x.replace('gm', 'g') if isinstance(x, str) else x) 
    # df.to_csv("/Users/jackbecker/xforce/CDC_Webscraping/clean_output.csv", index = False)
    print(df.sample(10))

clean()
