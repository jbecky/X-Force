import pandas as pd

def excel_to_csv(excel_path, output_folder):
    xlsx = pd.ExcelFile(excel_path)
    for sheet_name in xlsx.sheet_names:
        df = pd.read_excel(excel_path, sheet_name= sheet_name)
        csv_path = f"{output_folder}/{sheet_name}.csv"
        df.to_csv(csv_path, index = False)

def csv_to_excel(csv_path, output_folder):
    pass

excel_path = '/Users/jackbecker/xforce/TRV_Lists_initial/Auxiliary_dataframes/niosh_guidance_dictionary.xlsx'
output_folder = '/Users/jackbecker/xforce/TRV_Lists_initial/Auxiliary_dataframes/'
excel_to_csv(excel_path, output_folder)
