import argparse
# mine modules
import handlers

def main():
    # define arguments
    parser = argparse.ArgumentParser(description='administration tools for ZheQuant')
    ex_group = parser.add_mutually_exclusive_group()
    ex_group.add_argument('-r', '--reset', action='store_true', help='reset database')
    ex_group.add_argument('-i', '--insert', metavar='JSON_FILE_PATH', help='insert data into database')
    ex_group.add_argument('-g', '--gen_test', action='store_true', help='generate test data into database')
    ex_group.add_argument('-c', '--create_user', metavar=('USERID', 'PASSWORD'), nargs=2, help='create user and password')
    args = parser.parse_args()
    # behave according to the arguments
    if args.reset:
        handlers.reset_db()

    if args.insert:
        num = handlers.insert_from_file(args.insert)
        print('successfully inserted {0} documents'.format(num))

    if args.gen_test:
        handlers.gen_test_data()

    if args.create_user:
        name = args.create_user[0]
        passwd = args.create_user[1]
        if handlers.create_user(name, passwd):
            print('successfully created user')
        else:
            print('failed to create user')


if __name__ == '__main__':
    main()
