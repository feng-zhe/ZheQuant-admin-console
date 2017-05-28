#!./venv/bin/python
import argparse
import json
import random
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient, errors

def insert_from_file(FILE_PATH):
    '''
    insert data into database from a json file
    returns the number of documents inserted
    '''
    # read test data from file
    f = open(FILE_PATH, 'r')
    test_data = json.load(f)
    # connect database
    client = MongoClient('db', 27017)
    try: # this is because of change in pymongo 3.0, we have to do this
        client.admin.command('ismaster') # The ismaster command is cheap and does not require auth.
    except errors.ConnectionFailure:
        print("Server not available")
        return False
    # insert data to database
    db = client.fin
    result = db.fin.insert_many(test_data, ordered=False)
    return len(result.inserted_ids)

def gen_test_data(FILE_PATH):
    ''' 
    create test data, because the seed is same, the result is same every you run it
    '''
    # generate test data
    random.seed(0)
    test_data = []
    utc_date = datetime(2016, 1, 1, tzinfo=timezone.utc)
    for code_id in range(0,10): # 10 test stock code
        for day in range(0,366): # use 2016 as test year
            doc = {'code':'test_code_{0}'.format(code_id), 'date': utc_date.timestamp(), 'close_price': random.uniform(80,120)}
            test_data.append(doc)
            utc_date += timedelta(days=1)
    # write to file
    f = open(FILE_PATH, 'w')
    json.dump(test_data, f)
    return True

def main():
    # define arguments
    parser = argparse.ArgumentParser(description='administration tools for ZheQuant')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-r', '--reset', action='store_true', help='reset database')
    group.add_argument('-i', '--insert', metavar='JSON_FILE_PATH', help='insert data into database')
    group.add_argument('-g', '--gen_test', metavar='JSON_FILE_PATH', help='generate test data into database')
    args = parser.parse_args()
    # behave according to the arguments
    if args.reset:
        # TODO
        pass
    if args.insert:
        num = insert_from_file(args.insert)
        print('successfully inserted {0} documents'.format(num))
    if args.gen_test:
        if gen_test_data(args.gen_test):
            print('successfully generated test data')
        else:
            print('failed to generate test data')

if __name__ == '__main__':
    main()
