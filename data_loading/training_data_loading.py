from aws_operations.aws_storage import aws_storage_operations
from application_logging.aws_logging import aws_logger


class load_training_data:
    """
    This class shall be used to obtain data from the source for training.

    Written By: Prafful Agrawal
    Version: 1.0
    Revisions: None
    """
    def __init__(self, aws_operations=None, logger=None):
        self.training_file_path = 'training_file_from_db/input_file_for_training.csv'
        self.log_file = 'training_logs/training_data_loading_log.txt'
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
        Method to load the training data.

        Output:
            None - if training data does not exist, otherwise
            pandas.DataFrame - training data as a dataframe
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Loading training data!')
        try:
            training_data = self.aws_operations.read_csv(self.training_file_path)
            if training_data is None:
                return None
            self.logger.log(self.log_file, 'Training data loaded successfully!')
            return training_data
        except Exception as e:
            self.logger.log(self.log_file, 'Error occurred while loading training data: %s' % e)
            raise e
