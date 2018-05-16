import pymongo

# connet mongo database, port 27017
client = pymongo.MongoClient("mongodb://localhost:27017")
print('database connected', client)

# set database
mongodb_name = 'auto3'
db = client[mongodb_name]


# insert data
# ===
# document name: table(user defined)
# insert data in the format of dictionary
# _id automatically generated
def insert(table, dic):
    # may insert a format like this:
    # school = {
    #     'name': 'a',
    #     'rank': 1,
    #     'note': 'aaa',
    # }
    db[table].insert(dic)


# a = {
#     'a': '1'
# }
# insert('b', a)


# data search
# get all the data in one table
# ===
# returns an iterable object, use the list function to convert to an array
def get_data(table):
    object_list = list(db[table].find())
    print('all data', object_list)


# get_data('b')


# find(): take a dictionary to do conditional search
def find(table, query):
    # query = {
    #     'a': 1,
    # }
    # return as a list
    us = list(db[table].find(query))
    query = {
        'a': {
            '$gt': 1
        },
    }
    print('a > 1', list(db[table].find(query)))
    #
    # $or search
    query = {
        '$or': [
            {
                'a': 1,
            },
            {
                'b': 2
            }
        ]
    }
    us = list(db.user.find(query))
    print('or search', us)


# partial query search, select xx, yy from table
def find_cond(table, **kwargs):
    query = {}
    # field defines what data will be showed
    field = kwargs
    id = {
        '_id': 0,
    }
    field.update(id)
    print('only search', list(db['table'].find(query, field)))


# update data
# ===
def update(table, query_dic, dic):
    query = query_dic
    form = {
        '$set': dic
    }
    options = {
        'multi': True,
    }
    # set multi=True to get all the data match the query
    db[table].update(query, form, **options)


# delete table
def remove(table):
    db[table].remove()
