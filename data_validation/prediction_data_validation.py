from datetime import datetime
import re
from aws_operations.aws_storage import aws_storage_operations
from application_logging.aws_logging import aws_logger


class validate_prediction_data:
    """
    This class shall be used for handling all the validation done on the prediction data.

    Written By: Prafful Agrawal
    Version: 1.0
    Revisions: None
    """
    def __init__(self, folder_path, good_data_path, bad_data_path, aws_operations=None, logger=None):
        self.batch_directory = folder_path
        self.good_data_path = good_data_path
        self.bad_data_path = bad_data_path
        self.schema_path = 'schema_prediction.json'
        self.archive_folder_path = 'prediction_data_archive/'
        self.log_file = 'prediction_logs/prediction_data_validation_log.txt'
        if aws_operations is not None:
            self.aws_operations = aws_operations
        else:
            self.aws_operations = aws_storage_operations()
        if logger is not None:
            self.logger = logger
        else:
            self.logger = aws_logger(self.aws_operations)

    def values_from_schema(self):
        """
        Method to extract all the relevant information from the pre-defined prediction schema.

        Output:
            length_of_date_stamp_in_file - length of date stamp in the file; int
            length_of_time_stamp_in_file - length of time stamp in the file; int
            number_of_columns - number of columns; int
        On Failure:
            raise ValueError, KeyError, Exception
        """
        self.logger.log(self.log_file, 'Extracting values from prediction schema!')
        try:
            schema = self.aws_operations.read_json(self.schema_path)
            if schema is None:
                raise Exception('Prediction schema file does not exists!')
            length_of_date_stamp_in_file = schema['LengthOfDateStampInFile']
            length_of_time_stamp_in_file = schema['LengthOfTimeStampInFile']
            number_of_columns = schema['NumberOfColumns']
            self.logger.log(self.log_file, 'Values from prediction schema extracted successfully!')
            return length_of_date_stamp_in_file, length_of_time_stamp_in_file, number_of_columns
        except ValueError:
            self.logger.log(self.log_file, 'ValueError: Value not found inside prediction schema!')
            raise ValueError
        except KeyError:
            self.logger.log(self.log_file, 'KeyError: Key value error, incorrect key passed!')
            raise KeyError
        except Exception as e:
            message = 'Error occurred while extracting values from the prediction schema: %s' % e
            self.logger.log(self.log_file, message)
            raise e

    def generate_file_name_regex(self, length_of_date_stamp_in_file, length_of_time_stamp_in_file):
        """
        Method to generate a regex pattern of the file name as per the given schema.
        This regex pattern is used to validate the file name of the prediction data.

        Input parameters:
            length_of_date_stamp_in_file - length of date stamp in the file; int
            length_of_time_stamp_in_file - length of time stamp in the file; int
        Output:
            str - regex pattern of the file name
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Generating a regex pattern of the file name!')
        try:
            if length_of_date_stamp_in_file < 1:
                length_of_date_stamp_in_file = '0,'
            if length_of_time_stamp_in_file < 1:
                length_of_time_stamp_in_file = '0,'
            regex = '^[Ww]afer_[0-9]{' + str(length_of_date_stamp_in_file) + \
                    '}_[0-9]{' + str(length_of_time_stamp_in_file) + '}\.csv$'
            self.logger.log(self.log_file, 'Regex pattern generated successfully!')
            return regex
        except Exception as e:
            self.logger.log(self.log_file, 'Error occurred while generating the regex pattern: %s' % e)
            raise e

    def delete_existing_good_data_folder(self):
        """
        Method to delete the existing good prediction data folder.

        Output:
            None
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Deleting existing good prediction data folder!')
        try:
            if self.aws_operations.exists_folder(self.good_data_path):
                self.aws_operations.delete_folder(self.good_data_path)
            self.logger.log(self.log_file, 'Existing good prediction data folder deleted successfully!')
            return None
        except Exception as e:
            message = 'Error occurred while deleting existing good prediction data folder: %s' % e
            self.logger.log(self.log_file, message)
            raise e

    def delete_existing_bad_data_folder(self):
        """
        Method to delete the existing bad prediction data folder.

        Output:
            None
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Deleting existing bad prediction data folder!')
        try:
            if self.aws_operations.exists_folder(self.bad_data_path):
                self.aws_operations.delete_folder(self.bad_data_path)
            self.logger.log(self.log_file, 'Existing bad prediction data folder deleted successfully!')
            return None
        except Exception as e:
            message = 'Error occurred while deleting existing bad prediction data folder: %s' % e
            self.logger.log(self.log_file, message)
            raise e

    def archive_bad_prediction_data(self):
        """
         Method to move the bad prediction data to an archive.
         We archive bad data to send it back to the client to resolve data issues.

         Output:
             None
         On Failure:
             raise Exception
         """
        self.logger.log(self.log_file, 'Moving bad prediction data to archive!')
        try:
            now = datetime.now()
            date = now.date()
            time = now.strftime('%H%M%S')
            archive_path = self.archive_folder_path + 'bad_data_' + str(date) + '_' + str(time) + '/'
            file_paths = self.aws_operations.list_files(self.bad_data_path)
            if file_paths is None:
                message = 'Directory not found or otherwise empty! Continuing operation!'
                self.logger.log(self.log_file, message)
                return None
            for file_path in file_paths:
                file_name = re.split('/', file_path)[-1]
                new_path = archive_path + file_name
                self.aws_operations.move_file(file_path, new_path)
            self.logger.log(self.log_file, 'Bad training data moved to archive successfully!')
            return None
        except Exception as e:
            message = 'Error occurred while moving bad prediction data to archive: %s' % e
            self.logger.log(self.log_file, message)
            raise e

    def validate_file_names(self, regex):
        """
        Method to validate the file names as per the given prediction schema.
        Files with valid names are copied to a good data folder while
        the files with invalid names are copied to a bad data folder.

        Input parameters:
            regex - regex pattern of the file name; str
        Output:
            None
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Validating prediction file names!')
        try:
            file_paths = self.aws_operations.list_files(self.batch_directory)
            if file_paths is None:
                raise Exception('Directory not found or otherwise empty!')
            for file_path in file_paths:
                file_name = re.split('/', file_path)[-1]
                if re.match(regex, file_name):
                    new_path = self.good_data_path + file_name
                    self.aws_operations.copy_file(file_path, new_path)
                    message = 'Valid file name! File moved to good data folder: %s' % file_name
                    self.logger.log(self.log_file, message)
                else:
                    new_path = self.bad_data_path + file_name
                    self.aws_operations.copy_file(file_path, new_path)
                    message = 'Invalid file name! File moved to bad data folder: %s' % file_name
                    self.logger.log(self.log_file, message)
            self.logger.log(self.log_file, 'File names validated successfully!')
            return None
        except Exception as e:
            self.logger.log(self.log_file, 'Error occurred while validating file names: %s' % e)
            raise e

    def validate_number_of_columns(self, number_of_columns):
        """
        Method to validate the number of columns as per the given prediction schema.
        Files with valid number of columns are kept in the good data folder while
        the files with invalid number of columns are moved to the bad data folder.

        Input parameters:
            number_of_columns - number of columns; int
        Output:
            None
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Validating number of columns in the prediction data!')
        try:
            file_paths = self.aws_operations.list_files(self.good_data_path)
            if file_paths is None:
                raise Exception('Directory not found or otherwise empty!')
            for file_path in file_paths:
                file_name = re.split('/', file_path)[-1]
                new_path = self.bad_data_path + file_name
                csv_file = self.aws_operations.read_csv(file_path)
                if csv_file is None:
                    self.aws_operations.move_file(file_path, new_path)
                    message = 'Empty file! File moved to bad data folder: %s' % file_name
                    self.logger.log(self.log_file, message)
                elif csv_file.shape[1] != number_of_columns:
                    self.aws_operations.move_file(file_path, new_path)
                    message = 'Invalid number of columns! File moved to bad data folder: %s' % file_name
                    self.logger.log(self.log_file, message)
            self.logger.log(self.log_file, 'Number of columns validated successfully!')
            return None
        except Exception as e:
            self.logger.log(self.log_file, 'Error occurred while validating number of columns: %s' % e)
            raise e

    def validate_missing_values_in_columns(self):
        """
        Method to validate if any column in the file has all of its values missing.
        If all values are missing, the file is not suitable for processing and
        hence, will be moved to the bad data folder.

        Output:
            None
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Validating missing values in columns!')
        try:
            file_paths = self.aws_operations.list_files(self.good_data_path)
            if file_paths is None:
                raise Exception('Directory not found or otherwise empty!')
            for file_path in file_paths:
                file_name = re.split('/', file_path)[-1]
                new_path = self.bad_data_path + file_name
                csv_file = self.aws_operations.read_csv(file_path)
                if csv_file is None:
                    self.aws_operations.move_file(file_path, new_path)
                    message = 'Empty file! File moved to bad data folder: %s' % file_name
                    self.logger.log(self.log_file, message)
                    continue
                for column in csv_file.columns:
                    if (len(csv_file[column]) - csv_file[column].count()) == len(csv_file[column]):
                        self.aws_operations.move_file(file_path, new_path)
                        message = 'Invalid column! File moved to bad data folder: %s' % file_name
                        self.logger.log(self.log_file, message)
                        break
            self.logger.log(self.log_file, 'Missing values in columns validation completed!')
            return None
        except Exception as e:
            message = 'Error occurred while validating missing values in columns: %s' % e
            self.logger.log(self.log_file, message)
            raise e
