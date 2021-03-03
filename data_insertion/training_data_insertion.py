import pandas as pd
import json
import pymongo
from aws_operations.aws_storage import aws_storage_operations
from application_logging.aws_logging import aws_logger


class insert_training_data:
    """
    This class shall be used for inserting good training data into MongoDB Atlas database.

    Written By: Prafful Agrawal
    Version: 1.0
    Revisions: None
    """
    def __init__(self, good_data_path, aws_operations=None, logger=None):
        self.good_data_path = good_data_path
        self.username = 'waferfaultdetection-temp-user'
        self.password = 'RNp04vHPIiYOZX1h'
        self.db_name = 'waferfaultdetectiondb'
        self.csv_file_path = 'training_file_from_db/input_file_for_training.csv'
        self.log_file = 'training_logs/training_data_insertion_log.txt'
        if aws_operations is not None:
            self.aws_operations = aws_operations
        else:
            self.aws_operations = aws_storage_operations()
        if logger is not None:
            self.logger = logger
        else:
            self.logger = aws_logger(self.aws_operations)

    def initialize_connection(self):
        """
        Method to initialize a connection to the MongoDB Atlas database.

        Output:
            Database connection
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Initializing a connection to MongoDB Atlas database!')
        try:
            connection_uri = f'mongodb+srv://{self.username}:{self.password}' \
                             f'@waferfaultdetectionclus.zwf2l.mongodb.net/'\
                             f'{self.db_name}?retryWrites=true&w=majority'
            client = pymongo.MongoClient(connection_uri)
            conn = client[self.db_name]
            self.logger.log(self.log_file, 'Connected to the database successfully!')
            return conn
        except Exception as e:
            message = 'Error occurred while connecting to the database: %s' % e
            self.logger.log(self.log_file, message)
            raise e

    def create_collection(self, collection_name, drop=True):
        """
        Method to create and connect to a collection in the MongoDB Atlas database.
        A collection is roughly equivalent to a table in a relational database.
        If 'drop' is set to 'True' then this method will drop any existing collection;
        otherwise if 'drop' is set to 'False' it will connect to the existing collection.

        Input Parameters:
            collection_name - name of the collection; str
            drop - whether to drop existing collection; boolean (default = True)
        Output:
            Collection connection
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Creating a collection for the training data!')
        try:
            conn = self.initialize_connection()
            collection_names = conn.list_collection_names()
            if drop and (collection_name in collection_names):
                conn.drop_collection(collection_name)
            collection = conn[collection_name]
            self.logger.log(self.log_file, 'Collection created successfully!')
            return collection
        except Exception as e:
            message = 'Error occurred while creating the collection: %s' % e
            self.logger.log(self.log_file, message)
            raise e

    def insert_good_data_into_collection(self, collection_name, index_column=None):
        """
        Method to insert the good training data into a collection.
        It also sets up a column as a unique index if specified.
        The unique index ensures each entry is unique and will raise
        an error if it encounters an index value which already exists.

        Input Parameters:
            collection_name - name of the collection; str
            index_column - index column name; str (default = None)
        Output:
            None
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Inserting good training data into a collection!')
        try:
            collection = self.create_collection(collection_name)
            if index_column:
                collection.create_index([(index_column, pymongo.ASCENDING)],
                                        unique=True)
            file_paths = self.aws_operations.list_files(self.good_data_path)
            if file_paths is None:
                raise Exception('Directory not found or otherwise empty!')
            for file_path in file_paths:
                csv_file = self.aws_operations.read_csv(file_path)
                if csv_file is None:
                    raise Exception('File not found or otherwise empty!')
                data = csv_file.to_json(orient='records')
                json_data = json.loads(data)
                collection.insert_many(json_data)
            self.logger.log(self.log_file, 'Data inserted into a collection successfully!')
            collection.database.client.close()
            return None
        except Exception as e:
            message = 'Error occurred while inserting the data into a collection: %s' % e
            self.logger.log(self.log_file, message)
            raise e

    def copy_data_from_collection_to_csv_file(self, collection_name):
        """
        Method to copy the training data from the collection to a .CSV file.

        Input Parameters:
            collection_name - name of the collection; str
        Output:
            None
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Copying training data from the collection to a .CSV file!')
        try:
            conn = self.initialize_connection()
            collection_names = conn.list_collection_names()
            if collection_name not in collection_names:
                raise Exception('Collection not found or otherwise empty!')
            collection = conn[collection_name]
            data = collection.find()
            csv_file = pd.DataFrame(list(data))
            csv_file = csv_file.drop(columns=['_id'])
            self.aws_operations.write_csv(self.csv_file_path, csv_file)
            message = 'Data copied from the collection to a .CSV file successfully!'
            self.logger.log(self.log_file, message)
            collection.database.client.close()
            return None
        except Exception as e:
            message = 'Error occurred while copying data from the collection to a .CSV file: %s' % e
            self.logger.log(self.log_file, message)
            raise e
