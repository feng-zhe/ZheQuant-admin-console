import random
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient, errors, ASCENDING, DESCENDING

############################################### handlers ########################################
BACKUP_TAG = 'mwsrjtns' # used to find the backup collection name

def restore():
    # get db
    client = get_db_client()
    if not client:
        return False
    db = client.fin
    # remove test data 
    names = db.collection_names()
    # restore data from backup 
    if ('stocks' + BACKUP_TAG) in names:
        db['stocks' + BACKUP_TAG].rename('stocks', dropTarget=True)
        print('restored backup data into stocks');
    if ('users' + BACKUP_TAG) in names:
        db['users' + BACKUP_TAG].rename('users', dropTarget=True)
        print('restored backup data into users');
    if ('job_results' + BACKUP_TAG) in names:
        db['job_results' + BACKUP_TAG].rename('job_results', dropTarget=True)
        print('restored backup data into jobs');

def reset_db():
    '''
    Reset the whole database. Always returns True unless db connection fail.
    '''
    client = get_db_client()
    if not client:
        return False
    client.drop_database('fin')
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
    # get db
    client = get_db_client()
    if not client:
        return False
    db = client.fin
    # backup current data
    names = db.collection_names()
    if ('stocks'+BACKUP_TAG) in names: # already using test data?
        print('Already using test data. Aborting')
        return
    if ('stocks') in names:
        db.stocks.rename('stocks' + BACKUP_TAG)
        print('Backup stocks data')
    if ('users') in names:
        db.users.rename('users' + BACKUP_TAG)
        print('Backup users data')
    if ('job_results') in names:
        db.job_results.rename('job_results' + BACKUP_TAG)
        print('Backup jobs data')
    # generate test stock data
    random.seed(0)
    test_data = []
    utc_date = datetime(2016, 1, 1, tzinfo=timezone.utc)
    for code_id in range(0,10): # 10 test stock code
        for day in range(0,366): # use 2016 as test year
            doc = {'code':'test_code_{0}'.format(code_id), 'date': utc_date, 'close_price': random.uniform(80,120)}
            test_data.append(doc)
            utc_date += timedelta(days=1)
    # generate test user data
    test_user = { 'id': 'test', 'passwd': '098f6bcd4621d373cade4e832627b4f6' } # the md5 of 'test'
    # create indexes
    db.stocks.create_index([('code', ASCENDING), 
                            ('date', ASCENDING)], 
                            unique=True)
    db.users.create_index([('id', ASCENDING)], 
                            unique=True)
    db.job_results.create_index([('name', ASCENDING), 
                                 ('creator', ASCENDING), 
                                 ('create_date',DESCENDING)], 
                            unique=True)
    # insert into database
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
