from aws_operations.aws_storage import aws_storage_operations
from application_logging.aws_logging import aws_logger


class load_prediction_data:
    """
    This class shall be used to obtain data from the source for prediction.

    Written By: Prafful Agrawal
    Version: 1.0
    Revisions: None
    """
    def __init__(self, aws_operations=None, logger=None):
        self.prediction_file_path = 'prediction_file_from_db/input_file_for_prediction.csv'
        self.log_file = 'prediction_logs/prediction_data_loading_log.txt'
        if aws_operations is not None:
            self.aws_operations = aws_operations
        else:
            self.aws_operations = aws_storage_operations()
        if logger is not None:
            self.logger = logger
        else:
            self.logger = aws_logger(self.aws_operations)

    def load_data(self):
        """
        Method to load the prediction data.

        Output:
            None - if prediction data does not exist, otherwise
            pandas.DataFrame - prediction data as a dataframe
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Loading prediction data!')
        try:
            prediction_data = self.aws_operations.read_csv(self.prediction_file_path)
            if prediction_data is None:
                return None
            self.logger.log(self.log_file, 'Prediction data loaded successfully!')
            return prediction_data
        except Exception as e:
            self.logger.log(self.log_file, 'Error occurred while loading prediction data: %s' % e)
            raise e
