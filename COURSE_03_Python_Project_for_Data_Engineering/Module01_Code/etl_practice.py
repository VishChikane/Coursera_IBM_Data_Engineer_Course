import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 

'''
Commands :
source_zip_data_link = wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip 
unzip_source_data = unzip datasource.zip
'''

log_file = "log_file.txt" 
target_file = "transformed_data.csv" 

# Extracting Data from a .csv files using pandas
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

# Extracting Data from a .json files using pandas
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)  #lines=True to enable the function to read the file as a JSON object on line to line basis
    return dataframe

# Extracting Data from a .xml files
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["car_model","year_of_manufacture","price","fuel"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for car_detail in root:
        car_model = car_detail.find("car_model").text
        year_of_manufacture = car_detail.find("year_of_manufacture").text
        price = float(car_detail.find("price").text)
        fuel = car_detail.find("fuel").text
        dataframe = pd.concat([dataframe,pd.DataFrame([{"car_model":car_model,"year_of_manufacture":year_of_manufacture,"price":price,"fuel":fuel}])],ignore_index=True)
    return dataframe

# Main Extract method to handle all files type
def extract():
    # Create empty dataFrame to store extracted data
    extracted_data = pd.DataFrame(columns=["car_model","year_of_manufacture","price","fuel"])

    # process all csv files
    for csvfile in glob.glob("*.csv"):
        extracted_data = pd.concat([extracted_data,pd.DataFrame(extract_from_csv(csvfile))],ignore_index=True)

    # process all json files
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data,pd.DataFrame(extract_from_json(jsonfile))],ignore_index=True)
    
    # process all xml files 
    for xmlfile in glob.glob("*.xml"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True) 
    
    return extracted_data

# Transform the extracted data 
def transform(data):

    '''Convert price up to two decimals '''
    data["price"] = round(data.price, 2)

    return data

# Load data method
def load_data(target_file,transformed_data):
    transformed_data.to_csv(target_file)

# logger
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H-%M-%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file,"a") as f:
        f.write(timestamp+','+message+'\n')


# Now Test All ETL opration 

# Log the initialization of the ETL process 
log_progress("ETL Job Started") 

# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended")     

# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data : \n") 
print(transformed_data) 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 

# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(target_file,transformed_data) 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 

# Log the completion of the ETL process 
log_progress("ETL Job Ended") 