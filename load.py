from pymongo import MongoClient
import pandas as pd


class MongoDB:
    def __init__(self, user, password, host, db_name, port='27017'):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.db_name = db_name
        self.uri = 'mongodb://' + self.user + ':' + self.password + '@' + self.host + ':' + self.port + '/' + self.db_name
        try:
            self.client = MongoClient(self.uri)
            self.db = self.client[self.db_name]
            print("MongoDB Connected Successful!!!.")
        except Exception as e:
            print(' Error! Connection Unsuccessful!!')
            print(e)

    def insert_into_db(self, data, collection):
        if isinstance(data, pd.DataFrame):
            try:
                self.db[collection].insert_many(data.to_dict('records'))
                print('Data Inserted Successfully!!')
            except Exception as e:
                print("Opps!! Data Insertion Failed!!")
                print(e)
        else:
            try:
                self.db[collection].insert_many(data)
                print('Data Inserted Successfully!!')
            except Exception as e:
                print("Opps!! Data Insertion Failed!!")
                print(e)

    def read_from_db(self, collection):
        try:
            data = pd.DataFrame(list(self.db[collection].find()))
            print('Data Detched Successfully!!')
            return data
        except Exception as e:
            print('Opps!! Data Fetch Failed!!')
            print(e)
