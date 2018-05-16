import csv
import json
from urllib.request import urlopen
from io import StringIO
from database.mdb import *


def data_download():
    # NSW government school locations and student enrolment numbers
    url_1 = 'https://data.cese.nsw.gov.au/data/dataset/027493b2-33ad-3f5b-8ed9-37cdca2b8650/resource/2ac19870-44f6-443d-a0c3-4c867f04c305/download/masterdatasetnightlybatchcollections.csv'
    # NSW non-government school locations
    url_2 = 'https://data.cese.nsw.gov.au/data/dataset/1d019767-d953-426c-8151-1a6503d5a08a/resource/a5871783-7dd8-4b25-be9e-7d8b9b85422f/download/datahub_nongov_locations-2017.csv'
    # Student attendance rate by individual government schools
    url_3 = 'https://data.cese.nsw.gov.au/data/dataset/68b47d34-a014-4345-b41c-c97b8b58aca3/resource/d7decef3-e026-4268-a5f3-8caad0322f90/download/2011-2017-attendance-rates-by-nsw-government-schools.csv'
    # Specialist support classes by school and support needs type
    url_4 = 'https://data.cese.nsw.gov.au/data/dataset/85699cfe-366e-4caa-910f-bb7bedfdc311/resource/a963949f-99b5-49a9-a208-d15ab9a4d320/download/masterdatasetnightlybatchsupport.csv'
    data_1 = urlopen(url_1).read().decode('ascii', 'ignore')
    data_2 = urlopen(url_2).read().decode('ascii', 'ignore')
    data_3 = urlopen(url_3).read().decode('ascii', 'ignore')
    data_4 = urlopen(url_4).read().decode('ascii', 'ignore')
    datafile_1 = StringIO(data_1)
    datafile_2 = StringIO(data_2)
    datafile_3 = StringIO(data_3)
    datafile_4 = StringIO(data_4)
    return datafile_1, datafile_2, datafile_3, datafile_4


# load data set
def load_file():
    data_1, data_2, data_3, data_4 = data_download()
    # # print(type(data_1))
    reader_1 = csv.reader(data_1)
    reader_2 = csv.reader(data_2)
    reader_3 = csv.reader(data_3)
    reader_4 = csv.reader(data_4)

    return reader_1, reader_2, reader_3, reader_4


# return data in the format of dictionary
def data_to_dictionary(reader):
    # print('type(reader)', type(reader))
    # reader = list(reader)
    title = next(reader)
    # print('title', title)
    dic = {}
    data_in_dic = []
    index = 0

    # load data from reader to dictionary
    for row in reader:
        # print(row)
        for index, data in enumerate(row):
            title_name = title[index]
            dic[title_name] = data
            # print(dic)
        data_in_dic.append(dic)
        dic = {}

    # print(data_in_dic)
    return data_in_dic


# load data dictionaries
def load_dictionary():
    reader1, reader2, reader3, reader4 = load_file()

    data_in_dic_1 = data_to_dictionary(reader1)
    # print(data_in_dic_1)
    data_in_dic_2 = data_to_dictionary(reader2)
    data_in_dic_3 = data_to_dictionary(reader3)
    data_in_dic_4 = data_to_dictionary(reader4)

    return data_in_dic_1, data_in_dic_2, data_in_dic_3, data_in_dic_4

# data_in_dic_1, data_in_dic_2, data_in_dic_3, data_in_dic_4 = load_file()

# insert data into database
def insert_data(table_name, data_in_dic):
    for dic in data_in_dic:
        # print('d2', dic)
        insert(table_name, dic)


def db_insert():
    dic_list = list(load_dictionary())
    table_name = ['govschool', 'non-govschool', 'attendancerate_nongovschool', 'Specialist support']
    for index, dic in enumerate(dic_list):
        insert_data(table_name[index], dic)


# data to local
def dtl():
    dic_list = list(load_dictionary())
    table_name = ['govschool', 'non-govschool', 'attendancerate_nongovschool', 'Specialist support']
    for index, dic in enumerate(dic_list):
        path = 'local_json/{}'.format(table_name[index])
        s = json.dumps(dic, indent=2, ensure_ascii=False)
        with open(path, 'w+', encoding='utf-8') as f:
            f.write(s)


if __name__ == '__main__':
    db_insert()
    dtl()
