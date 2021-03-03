import boto3
from botocore.exceptions import ClientError
import pandas as pd
import json
from io import BytesIO
from application_logging.local_logging import local_logger

import os
os.putenv('AWS_SHARED_CREDENTIALS_FILE', 'C:/Users/Lenovo/Documents/.aws/credentials')


class aws_storage_operations:
    """
    This class shall be used for handling all the AWS storage operations.

    Written By: Prafful Agrawal
    Version: 1.0
    Revisions: None
    """
    def __init__(self, initialize_connection=True):
        self.region_name = 'us-east-2'
        self.service_name = 's3'
        self.profile_name = 'waferfaultdetectionuser'
        self.bucket_name = 'waferfaultdetectionproject'
        self.general_log = 'aws_logs/aws_storage_general_log.txt'
        self.error_log = 'aws_logs/aws_storage_error_log.txt'
        self.logger = local_logger()
        if initialize_connection:
            self.session = boto3.session.Session(profile_name=self.profile_name, region_name=self.region_name)
            self.s3_resource = self.session.resource(service_name=self.service_name)
            self.conn = self.s3_resource.Bucket(self.bucket_name)
            self.logger.log(self.general_log, 'Successfully connected to AWS storage!')
        else:
            self.conn = None

    def initialize_connection(self):
        """
        Method to initialize a connection to the given project bucket.

        Output:
            connection to the AWS S3 bucket
        On Failure:
            raise ClientError, Exception
        """
        try:
            session = boto3.session.Session(profile_name=self.profile_name, region_name=self.region_name)
            s3_resource = session.resource(service_name=self.service_name)
            conn = s3_resource.Bucket(self.bucket_name)
            self.logger.log(self.general_log, 'Successfully connected to AWS storage!')
            return conn
        except ClientError as error:
            self.logger.log(self.error_log,
                            'Error while connecting to AWS storage: %s' % error.response['Error']['Code'])
            raise error
        except Exception as e:
            self.logger.log(self.error_log, 'Unexpected error: %s' % e)
            raise e

    def exists(self, file_path):
        """
        Method to check whether the given file exists in the bucket.

        Input Parameters:
            file_path - absolute path of the file in the bucket; str
        Output:
            boolean - whether file exists or not
        On Failure:
            raise ClientError, Exception
        """
        try:
            conn = self.conn if self.conn is not None else self.initialize_connection()
            objs = conn.objects.all()
            all_paths = [obj.key for obj in objs]
            if file_path in all_paths:
                return True
            return False
        except ClientError as error:
            self.logger.log(self.error_log,
                            'Error while accessing the AWS storage: %s' % error.response['Error']['Code'])
            raise error
        except Exception as e:
            self.logger.log(self.error_log, 'Unexpected error: %s' % e)
            raise e

    def exists_folder(self, folder_path):
        """
        Method to check whether the given folder exists in the bucket.

        Input Parameters:
            folder_path - absolute path of the folder in the bucket; str
        Output:
            boolean - whether folder exists or not
        On Failure:
            raise ClientError, Exception
        """
        try:
            conn = self.conn if self.conn is not None else self.initialize_connection()
            objs = conn.objects.all()
            all_paths = [obj.key for obj in objs]
            if any([path.startswith(folder_path) for path in all_paths]):
                return True
            return False
        except ClientError as error:
            self.logger.log(self.error_log,
                            'Error while accessing the AWS storage: %s' % error.response['Error']['Code'])
            raise error
        except Exception as e:
            self.logger.log(self.error_log, 'Unexpected error: %s' % e)
            raise e

    def list_files(self, folder_path):
        """
        Method to list the files inside the given folder.
        Use an empty string to list all the files in the bucket.

        Input Parameters:
            folder_path - absolute path of the folder in the bucket; str
        Output:
            None - if folder_path does not exist, otherwise
            list - list of files
        On Failure:
            raise ClientError, Exception
        """
        try:
            conn = self.conn if self.conn is not None else self.initialize_connection()
            objs = conn.objects.all()
            all_paths = [obj.key for obj in objs]
            if folder_path is '':
                return all_paths
            elif not self.exists_folder(folder_path):
                return None
            else:
                file_paths = [path for path in all_paths if path.startswith(folder_path)]
                return file_paths
        except ClientError as error:
            self.logger.log(self.error_log,
                            'Error while accessing the AWS storage: %s' % error.response['Error']['Code'])
            raise error
        except Exception as e:
            self.logger.log(self.error_log, 'Unexpected error: %s' % e)
            raise e

    def create_file(self, file_path):
        """
        Method to create a new file.
        If file already exists, then makes no changes.

        Input Parameters:
            file_path - absolute path of the file in the bucket; str
        Output:
            None
        On Failure:
            raise ClientError, Exception
        """
        try:
            conn = self.conn if self.conn is not None else self.initialize_connection()
            if not self.exists(file_path):
                obj = conn.Object(file_path)
                obj.put()
            self.logger.log(self.general_log, 'Successfully created a new file!')
            return None
        except ClientError as error:
            self.logger.log(self.error_log,
                            'Error while creating a new file: %s' % error.response['Error']['Code'])
            raise error
        except Exception as e:
            self.logger.log(self.error_log, 'Unexpected error: %s' % e)
            raise e

    def read_file(self, file_path):
        """
        Method to read a file.

        Input Parameters:
            file_path - absolute path of the file in the bucket; str
        Output:
            None - if file_path does not exist, otherwise
            byte - file data as bytes
        On Failure:
            raise ClientError, Exception
        """
        try:
            conn = self.conn if self.conn is not None else self.initialize_connection()
            if not self.exists(file_path):
                return None
            obj = conn.Object(file_path)
            file_data = obj.get()['Body'].read()
            self.logger.log(self.general_log, 'Successfully read the given file!')
            return file_data
        except ClientError as error:
            self.logger.log(self.error_log,
                            'Error while reading a file: %s' % error.response['Error']['Code'])
            raise error
        except Exception as e:
            self.logger.log(self.error_log, 'Unexpected error: %s' % e)
            raise e

    def read_csv(self, file_path):
        """
        Method to read a .CSV file.

        Input Parameters:
            file_path - absolute path of the file in the bucket; str
        Output:
            None - if file_path does not exist, otherwise
            pandas.DataFrame - file data as dataframe
        On Failure:
            raise ClientError, Exception
        """
        try:
            file_data = self.read_file(file_path)
            if file_data is None:
                return None
            csv_data = pd.read_csv(BytesIO(file_data))
            self.logger.log(self.general_log, 'Successfully read the given .CSV file!')
            return csv_data
        except ClientError as error:
            self.logger.log(self.error_log,
                            'Error while reading a .CSV file: %s' % error.response['Error']['Code'])
            raise error
        except Exception as e:
            self.logger.log(self.error_log, 'Unexpected error: %s' % e)
            raise e

    def read_json(self, file_path):
        """
        Method to read a .JSON file.

        Input Parameters:
            file_path - absolute path of the file in the bucket; str
        Output:
            None - if file_path does not exist, otherwise
            dict - file data as dictionary
        On Failure:
            raise ClientError, Exception
        """
        try:
            file_data = self.read_file(file_path)
            if file_data is None:
                return None
            json_data = json.loads(file_data)
            self.logger.log(self.general_log, 'Successfully read the given .JSON file!')
            return json_data
        except ClientError as error:
            self.logger.log(self.error_log,
                            'Error while reading a .JSON file: %s' % error.response['Error']['Code'])
            raise error
        except Exception as e:
            self.logger.log(self.error_log, 'Unexpected error: %s' % e)
            raise e

    def write_file(self, file_path, data_to_write, overwrite=True):
        """
        Method to write a file.
        If 'overwrite' is set to 'True' it will overwrite the existing data;
        otherwise if 'overwrite' is set to 'False' it will append the existing file.

        Input Parameters:
            file_path - absolute path of the file in the bucket; str
            data_to_write - data to write; str
            overwrite - whether to overwrite existing data; boolean (default = True)
        Output:
            None
        On Failure:
            raise ClientError, Exception
        """
        try:
            conn = self.conn if self.conn is not None else self.initialize_connection()
            if overwrite or not self.exists(file_path):
                obj = conn.Object(file_path)
                obj.put(Body=data_to_write)
            else:
                self.append_file(file_path, data_to_write)
            self.logger.log(self.general_log, 'Successfully wrote the given file!')
            return None
        except ClientError as error:
            self.logger.log(self.error_log,
                            'Error while writing a file: %s' % error.response['Error']['Code'])
            raise error
        except Exception as e:
            self.logger.log(self.error_log, 'Unexpected error: %s' % e)
            raise e

    def write_csv(self, file_path, csv_file, overwrite=True):
        """
        Method to write a .CSV file.

        Input Parameters:
            file_path - absolute path of the file in the bucket; str
            csv_file - csv file to write; pandas.DataFrame
            overwrite - whether to overwrite existing data; boolean (default = True)
        Output:
            None
        On Failure:
            raise ClientError, Exception
        """
        try:
            data_to_write = csv_file.to_csv(index=None, header=True)
            if overwrite or not self.exists(file_path):
                self.write_file(file_path, data_to_write)
            else:
                self.append_file(file_path, data_to_write)
            self.logger.log(self.general_log, 'Successfully wrote the given .CSV file!')
            return None
        except ClientError as error:
            self.logger.log(self.error_log,
                            'Error while writing a .CSV file: %s' % error.response['Error']['Code'])
            raise error
        except Exception as e:
            self.logger.log(self.error_log, 'Unexpected error: %s' % e)
            raise e

    def write_png(self, file_path, plt):
        """
        Method to write a .PNG file.

        Input Parameters:
            file_path - absolute path of the file in the bucket; str
            plt - matplotlib.pyplot instance with the required plot state
        Output:
            None
        On Failure:
            raise ClientError, Exception
        """
        try:
            # Prepare image data
            img_data = BytesIO()
            plt.savefig(img_data, format='png')
            img_data.seek(0)
            # Write image data to the storage
            conn = self.conn if self.conn is not None else self.initialize_connection()
            obj = conn.Object(file_path)
            obj.put(Body=img_data, ContentType='image/png')
            self.logger.log(self.general_log, 'Successfully wrote the given .PNG file!')
            return None
        except ClientError as error:
            self.logger.log(self.error_log,
                            'Error while writing a .PNG file: %s' % error.response['Error']['Code'])
            raise error
        except Exception as e:
            self.logger.log(self.error_log, 'Unexpected error: %s' % e)
            raise e

    def append_file(self, file_path, data_to_append):
        """
        Method to append a file.
        It will create and write a new file if file_path does not exist already.

        Input Parameters:
            file_path - absolute path of the file in the bucket; str
            data_to_append - data to append; str
        Output:
            None
        On Failure:
            raise ClientError, Exception
        """
        try:
            old_file_data = self.read_file(file_path)
            if old_file_data is None:
                old_file_data = b''
            data_to_append_bytes = bytes(data_to_append, 'utf-8')
            new_file_data = old_file_data + data_to_append_bytes
            self.write_file(file_path, new_file_data)
            self.logger.log(self.general_log, 'Successfully appended the given file!')
            return None
        except ClientError as error:
            self.logger.log(self.error_log,
                            'Error while appending a file: %s' % error.response['Error']['Code'])
            raise error
        except Exception as e:
            self.logger.log(self.error_log, 'Unexpected error: %s' % e)
            raise e

    def copy_file(self, file_path, new_path):
        """
        Method to copy a file from one location to another location.
        It keeps the original file.

        Input Parameters:
            file_path - absolute path of the file in the bucket; str
            new_path - new (absolute) path of the file; str
        Output:
            None
        On Failure:
            raise ClientError, Exception
        """
        try:
            conn = self.conn if self.conn is not None else self.initialize_connection()
            source_path = {'Bucket': self.bucket_name,
                           'Key': file_path}
            if not self.exists(file_path):
                raise Exception('File not found!')
            obj = conn.Object(new_path)
            obj.copy(source_path)
            self.logger.log(self.general_log, 'Successfully copied the given file!')
            return None
        except ClientError as error:
            self.logger.log(self.error_log,
                            'Error while copying a file: %s' % error.response['Error']['Code'])
            raise error
        except Exception as e:
            self.logger.log(self.error_log, 'Unexpected error: %s' % e)
            raise e

    def delete_file(self, file_path):
        """
        Method to delete a file.

        Input Parameters:
            file_path - absolute path of the file in the bucket; str
        Output:
            None
        On Failure:
            raise ClientError, Exception
        """
        try:
            conn = self.conn if self.conn is not None else self.initialize_connection()
            if not self.exists(file_path):
                raise Exception('File not found!')
            obj = conn.Object(file_path)
            obj.delete()
            self.logger.log(self.general_log, 'Successfully deleted the given file!')
            return None
        except ClientError as error:
            self.logger.log(self.error_log,
                            'Error while deleting a file: %s' % error.response['Error']['Code'])
            raise error
        except Exception as e:
            self.logger.log(self.error_log, 'Unexpected error: %s' % e)
            raise e

    def delete_folder(self, folder_path):
        """
        Method to delete a folder.

        Input Parameters:
            folder_path - absolute path of the folder in the bucket; str
        Output:
            None
        On Failure:
            raise ClientError, Exception
        """
        try:
            conn = self.conn if self.conn is not None else self.initialize_connection()
            if not self.exists_folder(folder_path):
                raise Exception('Folder not found!')
            obj = conn.objects.filter(Prefix=folder_path)
            obj.delete()
            self.logger.log(self.general_log, 'Successfully deleted the given folder!')
            return None
        except ClientError as error:
            self.logger.log(self.error_log,
                            'Error while deleting a folder: %s' % error.response['Error']['Code'])
            raise error
        except Exception as e:
            self.logger.log(self.error_log, 'Unexpected error: %s' % e)
            raise e

    def move_file(self, file_path, new_path):
        """
        Method to move a file from one location to another location.
        It removes the original file.

        Input Parameters:
            file_path - absolute path of the folder in the bucket; str
            new_path - new (absolute) path of the file; str
        Output:
            None
        On Failure:
            raise ClientError, Exception
        """
        try:
            self.copy_file(file_path, new_path)
            self.delete_file(file_path)
            self.logger.log(self.general_log, 'Successfully moved the given file!')
            return None
        except ClientError as error:
            self.logger.log(self.error_log,
                            'Error while moving a file: %s' % error.response['Error']['Code'])
            raise error
        except Exception as e:
            self.logger.log(self.error_log, 'Unexpected error: %s' % e)
            raise e
