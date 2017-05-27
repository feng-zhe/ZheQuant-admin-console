#!./venv/bin/python
import argparse
import json
from pymongo import MongoClient, errors

def insert_from_file(FILE_PATH):
    # read test data from file
    f = open(FILE_PATH, 'r')
    test_data = json.load(f)
    # connect database
    client = MongoClient('db', 27017)
    try: # this is because of change in 3.0, refer to http://api.mongodb.com/python/current/api/pymongo/mongo_client.html#pymongo.mongo_client.MongoClient
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
    except errors.ConnectionFailure:
        print("Server not available")
        return False
    # insert data to database
    db = client.fin
    db.fin.insert_many(test_data)

def main():
    # define arguments
    parser = argparse.ArgumentParser(description='administration tools for ZheQuant')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-r', '--reset', action='store_true', help='reset database')
    group.add_argument('-i', '--insert', metavar='JSON_FILE_PATH', help='insert data into database')
    args = parser.parse_args()
    # behave according to the arguments
    if args.reset:
        # TODO
        pass
    if args.insert:
        insert_from_file(args.insert)

if __name__ == '__main__':
    main()
