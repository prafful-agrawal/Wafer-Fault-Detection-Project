import re
from aws_operations.aws_storage import aws_storage_operations
from application_logging.aws_logging import aws_logger


class transform_prediction_data:
    """
    This class shall be used for transforming the good prediction data before loading it in a database.

    Written By: Prafful Agrawal
    Version: 1.0
    Revisions: None
    """
    def __init__(self, good_data_path, bad_data_path, aws_operations=None, logger=None):
        self.good_data_path = good_data_path
        self.bad_data_path = bad_data_path
        self.log_file = 'prediction_logs/prediction_data_transformation_log.txt'
        if aws_operations is not None:
            self.aws_operations = aws_operations
        else:
            self.aws_operations = aws_storage_operations()
        if logger is not None:
            self.logger = logger
        else:
            self.logger = aws_logger(self.aws_operations)

    def replace_missing_values_with_null(self):
        """
        Method to replace missing values with 'NULL' to store the prediction data in the database.

        Output:
            None
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Replacing missing values with NULL!')
        try:
            file_paths = self.aws_operations.list_files(self.good_data_path)
            if file_paths is None:
                raise Exception('Directory not found or otherwise empty!')
            for file_path in file_paths:
                file_name = re.split('/', file_path)[-1]
                csv_file = self.aws_operations.read_csv(file_path)
                if csv_file is None:
                    new_path = self.bad_data_path + file_name
                    self.aws_operations.move_file(file_path, new_path)
                    message = 'Empty file! File moved to bad data folder: %s' % file_name
                    self.logger.log(self.log_file, message)
                else:
                    csv_file = csv_file.fillna('NULL')
                    self.aws_operations.write_csv(file_path, csv_file)
            message = 'Missing values replaced with NULL successfully!'
            self.logger.log(self.log_file, message)
            return None
        except Exception as e:
            message = 'Error occurred while replacing missing values: %s' % e
            self.logger.log(self.log_file, message)
            raise e

    def format_wafer_column(self):
        """
        Method to format the 'Wafer' column in the prediction data.
        The first column is missing its name in the .CSV files. So, this method renames
        the unnamed column as 'Wafer' column. Also, the substring 'Wafer' is dropped
        from the values of this column to keep only the 'Integer' data.

        Output:
            None
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Formatting the wafer column!')
        try:
            file_paths = self.aws_operations.list_files(self.good_data_path)
            if file_paths is None:
                raise Exception('Directory not found or otherwise empty!')
            for file_path in file_paths:
                file_name = re.split('/', file_path)[-1]
                csv_file = self.aws_operations.read_csv(file_path)
                if csv_file is None:
                    new_path = self.bad_data_path + file_name
                    self.aws_operations.move_file(file_path, new_path)
                    message = 'Empty file! File moved to bad data folder: %s' % file_name
                    self.logger.log(self.log_file, message)
                else:
                    csv_file = csv_file.rename(columns={'Unnamed: 0': 'Wafer'})
                    csv_file['Wafer'] = csv_file['Wafer'].str[6:]
                    self.aws_operations.write_csv(file_path, csv_file)
            self.logger.log(self.log_file, 'Wafer column formatted successfully!')
            return None
        except Exception as e:
            message = 'Error occurred while formatting the wafer column: %s' % e
            self.logger.log(self.log_file, message)
            raise e
