# import python libraries
import csv
import json
import logging
import xml.etree.ElementTree as ET
from pony.orm import *

# setting logging basic configuration
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ETLPipeline():
    def __init__(self, csv_file, json_file, xml_file) -> None:
        self.csv_file = csv_file
        self.json_file = json_file
        self.xml_file = xml_file


    def read_csv(self):

        # read csv file
        file = open(self.csv_file,'r')
        df = csv.DictReader(file)
        return df

    def rename_csv(self):

        df = self.read_csv()
        new_dataframe = []

        # iterate on rows
        for row in df:
            idx_row = {'firstName': row['First Name'],
            'lastName': row['Second Name'],
            'age': row['Age (Years)'],
            'sex': row['Sex'],
            'Vehicle Make': row['Vehicle Make'],
            'Vehicle Model': row['Vehicle Model'],
            'Vehicle Year': row['Vehicle Year'],
            'Vehicle Type': row['Vehicle Type']}

            # Apend to the new dataframe
            new_dataframe.append(idx_row)

        # close file afterwards
        # df.close()

        # open new file to write the changes
        file = open('updated1', "w", newline='')
        headers = ['firstName', 'lastName', 'age', 'sex',
        'Vehicle Make', 'Vehicle Model', 'Vehicle Year', 'Vehicle Type']

        data = csv.DictWriter(file, delimiter=',', fieldnames=headers)

        data.writerow(dict((heads, heads) for heads in headers))

        data.writerows(new_dataframe)

        # close the edited file
        file.close()


    # read and convert json data to csv

    def read_json(self):

        # empty data array
        data = []

        with open(self.json_file) as f:
            df = json.load(f)

            # append data
            data = df

            # Create new csv file
            file = open('updated2', 'w', newline='')
            file_writer = csv.writer(file)

            count = 0

            for row in df:
                # Check if the debt column exists
                if 'debt' in row:
                    pass
                else:
                    row['debt'] = "0"

                if count == 0:
                    header = row.keys()
                    file_writer.writerow(header)
                    count += 1
                file_writer.writerow(row.values())
            
            file.close()

    
    # ================== Read XML data ========================= #

    def read_xml(self):
        tree = ET.parse(self.xml_file)

        root = tree.getroot()

        data = []
        row = {}

        for child in root:
            data.append(child.attrib)

            # create a new csv file
            file = open('updated3','w', newline='')
            file_writer = csv.writer(file)

            count = 0
            for elem in data:
                # print(elem)
                if count == 0:
                    header = elem.keys()
                    file_writer.writerow(header)
                    count += 1
                file_writer.writerow(elem.values())
            file.close()

    # ======================== Generate data Map =========================

    def create_map(self,file):
        map = {}
        headers = []

        # files
        with open(file,'r') as f:
            reader = csv.DictReader(f)
            headers += reader.fieldnames
            for row in reader:
                first_name, last_name = row['firstName'], row['lastName']

                key_str = first_name + " " + last_name
                map[key_str] = row
        headers = set(headers)
        return map


    #  ================================= Join Maps ============================


    def wrangler(self, file1, file2):
        for (k1, v1), (k2, v2) in zip(file1.items(), file2.items()):
            file1[k1].update(v1)
            if k1==k2:
                file1[k1].update(v2)
            file1[k2].update(v2)
        return file1


# Database Class
db = Database()

# the different configurations
db_params = dict(provider="mysql", host="europa.ashley.work",
                 user="student_bi29aq", password="iE93F2@8EhM@1zhD&u9M@K", db="student_bi29aq")





if __name__ == "__main__":
    pipe = ETLPipeline('user_data.csv', 'user_data.json', 'user_data.xml')

    
    logger.info('Reading JSON data')
    pipe.read_json()
    logger.info('Completed reading JSON')


    # Read XML data
    logger.info('Reading XML data')
    pipe.read_xml()

    # Pre-process CSV data
    logger.info('Pre-processing CSV data')
    pipe.rename_csv()



    # generate map
    logger.info('Generating Maps')
    csvf= pipe.create_map('updated1')
    mapf= pipe.create_map('updated2')
    mapf2= pipe.create_map('updated3')

    logger.info('Wrangling Data')
    wrng1 = pipe.wrangler(csvf,mapf)
    wrng1 = pipe.wrangler(wrng1,mapf2)
    # print(wrng1)




    # ============================ Answering Questions ========================================

    # 1. 
    wrng1['Shane Chambers'].update({'credit_card_security_code':'935'})

    # 2. 
    wrng1['Joshua Lane'].update({'salary': '2000'})

    # 3. 
    wrng1['Suzanne Wright'].update({'age': '37'})

    wrng1['Kyle Dunn'].update({'pension':'25711.7'})


    # the table instance
    class User(db.Entity):
        _table = "Users"
        firstName = Optional(str)
        secondName = Optional(str)
        age = Optional(int)
        iban = Optional(str)
        debt = Optional(str)
        cc_no = Optional(str)
        cc_security_code = Optional(str)
        cc_start_date = Optional(str)
        cc_end_date = Optional(str)
        address_name = Optional(str)
        address_city = Optional(str)
        dependants = Optional(str)
        marital_status = Optional(str)
        salary = Optional(str)
        pension = Optional(str)
        company = Optional(str)
        commute_distance = Optional(str)
        vhcl_make = Optional(str)
        vhcl_model = Optional(str)
        vhcl_year = Optional(str)
        vhcl_type = Optional(str)


    sql_debug(True)

    # # bind the different attributes
    db.bind(**db_params)
    db.generate_mapping(create_tables=True)  # Create tables


    # ================================= Saving to the DB =========================
    
    data =[]
    for _, v in wrng1.items():
        data.append(v)

    @db_session
    def save():
        for row in range(0, len(data)):
            User(
                firstName=data[row]["firstName"],
                secondName=data[row]["lastName"],
                age=data[row]["age"],
                iban=data[row]["iban"],
                debt = data[row]["debt"],
                cc_no=str(data[row]["credit_card_number"]),
                cc_security_code=data[row]["credit_card_security_code"],
                cc_start_date=data[row]["credit_card_start_date"],
                cc_end_date=data[row]["credit_card_end_date"],
                address_name=data[row]["address_main"],
                address_city=data[row]["address_city"],
                dependants=data[row]["dependants"],
                marital_status=data[row]["marital_status"],
                salary=data[row]["salary"],
                pension=data[row]["pension"],
                company=str(data[row]["company"]),
                commute_distance=data[row]["commute_distance"],
                vhcl_make=data[row]["Vehicle Make"],
                vhcl_model=data[row]["Vehicle Model"],
                vhcl_year=str(data[row]["Vehicle Year"]),
                vhcl_type=data[row]["Vehicle Type"]
            )

    logger.info('Saving to Database...')

    save()








    



            
