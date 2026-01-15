# to find csv, jason, xml file to extract
import glob
# python3 -m pip install pandas
# Installing collected packages: pytz, tzdata, python-dateutil, numpy, pandas
import pandas as pd
# only import ElementTree to parse xml file
import xml.etree.ElementTree as ET
# for log
from datetime import datetime

# Need path to these files
log_file = "log_file.txt"
target_file = "transformed_data.csv"


def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe


def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe


# need to concat when adding xml data : car_model is the root element
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["year_of_manufacture", "price", "fuel"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for car_model in root:
        year_of_manufacture = car_model.find("year_of_manufacture").text
        price = float(car_model.find("price").text)
        fuel = car_model.find("fuel").text
        dataframe = pd.concat(
            [dataframe, pd.DataFrame([{"year_of_manufacture": year_of_manufacture, "price": price, "fuel": fuel}])],
            ignore_index=True)
    return dataframe


def extract():
    extracted_data = pd.DataFrame(
        columns=['year_of_manufacture', 'price', 'fuel'])  # create an empty data frame to hold extracted data

    # process all csv files, except the target file
    for csvfile in glob.glob("*.csv"):
        if csvfile != target_file:  # check if the file is not the target file
            extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True)

            # process all json files
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)

        # process all xml files
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True)

    return extracted_data


def transform(data):
    '''round off to two decimals'''
    data['price'] = round(data.price, 2)
    return data


# load data
def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)


################# LOG #######################
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')

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
print("Transformed Data")
print(transformed_data)

# Log the completion of the Transformation process
log_progress("Transform phase Ended")

# Log the beginning of the Loading process
log_progress("Load phase Started")
load_data(target_file, transformed_data)

# Log the completion of the Loading process
log_progress("Load phase Ended")

# Log the completion of the ETL process
log_progress("ETL Job Ended")


#############OUT PUT#############

year_of_manufacture     price    fuel car_model
0                 2017   4253.73  Petrol  alto 800
1                 2015  10223.88  Diesel      ciaz
2                 2015  11194.03  Petrol      ciaz
3                 2015   9104.48  Petrol    ertiga
4                 2009   3358.21  Petrol     dzire
..                 ...       ...     ...       ...
85                2015   5895.52  Petrol       NaN
86                2013   8208.96  Petrol       NaN
87                2004   2238.81  Petrol       NaN
88                2010   7835.82  Petrol       NaN
89                2012  21641.79  Diesel       NaN

