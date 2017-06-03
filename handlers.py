import random
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient, errors

############################################### handlers ########################################
def reset_db():
    '''
    Drop the collections created. Always returns True unless db connection fail.
    '''
    client = get_db_client()
    if not client:
        return False
    db = client.fin
    rst1 = db.users.drop()
    rst2 = db.stocks.drop()
    rst2 = db.job_results.drop()
    return True

def get_db_client():
    '''
    get the db client or None if error
    '''
    client = MongoClient('db', 27017)
    try: # this is because of change in pymongo 3.0, we have to do this
        client.admin.command('ismaster') # The ismaster command is cheap and does not require auth.
        return client
    except errors.ConnectionFailure:
        print("Server not available")
        return None

def insert_from_file(FILE_PATH):
    '''
    insert data into database from a json file
    returns the number of documents inserted
    '''
    # read test data from file
    f = open(FILE_PATH, 'r')
    test_data = json.load(f)
    # connect database
    client = get_db_client()
    if not client:
        return False
    # insert stock data into database
    db = client.fin
    result = db.stocks.insert_many(test_data, ordered=False)
    return len(result.inserted_ids)

def gen_test_data():
    ''' 
    create test collections, because the seed is same, the result is same every you run it
    '''
    # generate test stock data
    random.seed(0)
    test_data = []
    utc_date = datetime(2016, 1, 1, tzinfo=timezone.utc)
    for code_id in range(0,10): # 10 test stock code
        for day in range(0,366): # use 2016 as test year
            doc = {'code':'test_code_{0}'.format(code_id), 'date': utc_date.timestamp(), 'close_price': random.uniform(80,120)}
            test_data.append(doc)
            utc_date += timedelta(days=1)
    # generate test user data
    test_user = { 'id': 'test', 'passwd': '098f6bcd4621d373cade4e832627b4f6' } # the md5 of 'test'
    # create indexes in database
    client = get_db_client()
    if not client:
        return False
    # insert into database
    db = client.fin
    rst1 = db.stocks.insert_many(test_data)
    if rst1:
        print('[ok] inserted {0} stock docs'.format(len(rst1.inserted_ids)))
    rst2 = db.users.insert_one(test_user)
    if rst2:
        print('[ok] inserted test user, name: test, password: test')

def create_user(name, passwd):
    # connect database
    client = get_db_client()
    if not client:
        return False
    # insert user into users collection
    user = {'id': name, 'passwd': passwd}
    db = client.fin
    result = db.users.insert_one(user)
    return result
