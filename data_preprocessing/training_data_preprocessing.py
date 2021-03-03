import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer
from aws_operations.aws_storage import aws_storage_operations
from application_logging.aws_logging import aws_logger


class preprocess_training_data:
    """
    This class shall be used to clean and transform the data before training.

    Written By: Prafful Agrawal
    Version: 1.0
    Revisions: None
    """
    def __init__(self, aws_operations=None, logger=None):
        self.log_file = 'training_logs/training_data_preprocessing_log.txt'
        if aws_operations is not None:
            self.aws_operations = aws_operations
        else:
            self.aws_operations = aws_storage_operations()
        if logger is not None:
            self.logger = logger
        else:
            self.logger = aws_logger(self.aws_operations)

    def remove_columns(self, data, columns_to_remove):
        """
        Method to remove the given columns from the training data.

        Input parameters:
            data - data; pandas.DataFrame
            columns_to_remove - list of column names; list
        Output:
            pandas.DataFrame - new data with the columns removed
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Removing the given columns from the training data!')
        try:
            new_data = data.drop(labels=columns_to_remove, axis=1)
            self.logger.log(self.log_file, 'Columns removed successfully!')
            return new_data
        except Exception as e:
            self.logger.log(self.log_file, 'Error occurred while removing the columns: %s' % e)
            raise e

    def separate_features_and_label(self, data, label_column_name):
        """
        Method to separate the features and the label in the training data.

        Input parameters:
            data - data; pandas.DataFrame
            label_column_name - name of the label column; str
        Output:
            pandas.DataFrame - features, and
            pandas.Series - label column
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Separating features and label in the training data!')
        try:
            features = data.drop(labels=label_column_name, axis=1)
            label = data[label_column_name]
            self.logger.log(self.log_file, 'Features and label separated successfully!')
            return features, label
        except Exception as e:
            message = 'Error occurred while separating features and label: %s' % e
            self.logger.log(self.log_file, message)
            raise e

    def is_null_present(self, data):
        """
        Method to check whether NULL values are present in the data.

        Input parameters:
            data - data; pandas.DataFrame
        Output:
            boolean - whether NULL values are present or not
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Checking for NULL values in the training data!')
        try:
            null_counts = data.isna().sum()
            self.logger.log(self.log_file, 'NULL values checked successfully!')
            if null_counts.sum() > 0:
                return True
            return False
        except Exception as e:
            message = 'Error occurred while checking for NULL values: %s' % e
            self.logger.log(self.log_file, message)
            raise e

    def impute_missing_values(self, data):
        """
        Method to replace all the missing values in the training data using a KNN Imputer.

        Input parameters:
            data - data; pandas.DataFrame
        Output:
            pandas.DataFrame - new data with the missing values replaced
        On Failure:
            raise Exception
        """
        message = 'Imputing missing values in the training data using a KNN imputer!'
        self.logger.log(self.log_file, message)
        try:
            knn_imputer = KNNImputer(n_neighbors=3, weights='uniform', missing_values=np.nan)
            new_array = knn_imputer.fit_transform(data)
            # Convert the nd-array returned above to a dataframe
            new_data = pd.DataFrame(data=new_array, columns=data.columns)
            self.logger.log(self.log_file, 'Missing values imputed successfully!')
            return new_data
        except Exception as e:
            message = 'Error occurred while imputing missing values: %s' % e
            self.logger.log(self.log_file, message)
            raise e

    def columns_with_zero_standard_deviation(self, data):
        """
        Method to find the columns in the training data which have a standard deviation of zero.

        Input parameters:
            data - data; pandas.DataFrame
        Output:
            list - list of column names having zero standard deviation
        On Failure:
            raise Exception
        """
        message = 'Checking for columns in the training data with zero standard deviation!'
        self.logger.log(self.log_file, message)
        try:
            zero_std_dev_columns = []
            for column in data.columns:
                if data[column].std() == 0:
                    zero_std_dev_columns.append(column)
            self.logger.log(self.log_file, 'Standard deviation checked successfully!')
            return zero_std_dev_columns
        except Exception as e:
            message = 'Error occurred while checking standard deviation: %s' % e
            self.logger.log(self.log_file, message)
            raise e
