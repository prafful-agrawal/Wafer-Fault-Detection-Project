from aws_operations.aws_storage import aws_storage_operations
from application_logging.aws_logging import aws_logger
from data_validation.prediction_data_validation import validate_prediction_data
from data_transformation.prediction_data_transformation import transform_prediction_data
from data_insertion.prediction_data_insertion import insert_prediction_data


class validate_and_insert_prediction_data:
    """
    This class serves as the entry point for validation and insertion of prediction data.

    Written By: Prafful Agrawal
    Version: 1.0
    Revisions: None
    """
    def __init__(self, folder_path):
        self.collection_name = 'prediction_data'
        self.good_data_path = 'prediction_data_validated/good_data/'
        self.bad_data_path = 'prediction_data_validated/bad_data/'
        self.log_file = 'prediction_logs/prediction_data_validation_and_insertion_log.txt'
        self.aws_operations = aws_storage_operations()
        self.logger = aws_logger(self.aws_operations)
        self.data_validation = validate_prediction_data(folder_path, self.good_data_path, self.bad_data_path,
                                                        self.aws_operations, self.logger)
        self.data_transformation = transform_prediction_data(self.good_data_path, self.bad_data_path,
                                                             self.aws_operations, self.logger)
        self.data_insertion = insert_prediction_data(self.good_data_path, self.aws_operations, self.logger)

    def validate_and_insert_data(self):
        """
        Method to validate and insert the prediction data.
        It carries out the following sequence of operations:
            - read the given schema for the prediction data,
            - validate file names according to the schema,
            - validate number of columns according to the schema,
            - validate if any column has all its values missing,
            - replace missing values will 'NULL',
            - format the 'Wafer' column,
            - insert the good data into a MongoDB Atlas database,
            - copy the data from the database to a .CSV file, and
            - archive the remaining bad data.
        The archived bad data is sent back to the client to resolve data issues.

        Output:
            None
        On Failure:
            raise Exception

        Written By: Prafful Agrawal
        Version: 1.0
        Revisions: None
        """
        try:
            self.logger.log(self.log_file, 'Validating the prediction data!')
            # Delete good and bad data folders in case last run was
            # unsuccessful and the folders were not deleted
            self.data_validation.delete_existing_good_data_folder()
            self.data_validation.delete_existing_bad_data_folder()
            # Extract values from the prediction schema
            length_of_date_stamp_in_file, length_of_time_stamp_in_file, number_of_columns = \
                self.data_validation.values_from_schema()
            # Generate a regex pattern to validate the file names
            regex = self.data_validation.generate_file_name_regex(length_of_date_stamp_in_file,
                                                                  length_of_time_stamp_in_file)
            # Validate file names of the prediction data
            self.data_validation.validate_file_names(regex)
            # Validate number of columns in the prediction data
            self.data_validation.validate_number_of_columns(number_of_columns)
            # Validate if any column has all of its values missing
            self.data_validation.validate_missing_values_in_columns()
            self.logger.log(self.log_file, 'Prediction data validated successfully!')

            self.logger.log(self.log_file, 'Transforming the prediction data!')
            # Replace missing values with 'NULL'
            self.data_transformation.replace_missing_values_with_null()
            # Format the 'Wafer' column
            self.data_transformation.format_wafer_column()
            self.logger.log(self.log_file, 'Prediction data transformed successfully!')

            self.logger.log(self.log_file, 'Inserting the prediction data!')
            # Insert the good prediction data into a MongoDB Atlas database
            self.data_insertion.insert_good_data_into_collection(self.collection_name, 'Wafer')
            # Delete the good data folder after loading its data into the database
            self.data_validation.delete_existing_good_data_folder()
            # Export the data from the database to a .CSV file
            self.data_insertion.copy_data_from_collection_to_csv_file(self.collection_name)
            # Move the remaining bad prediction data to an archive
            self.data_validation.archive_bad_prediction_data()
            self.logger.log(self.log_file, 'Prediction data inserted successfully!')
            return None
        except Exception as e:
            self.logger.log(self.log_file, 'Unexpected Error: %s' % e)
            raise e
