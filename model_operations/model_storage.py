import re
import pickle
from aws_operations.aws_storage import aws_storage_operations
from application_logging.aws_logging import aws_logger


class model_storage_operations:
    """
    This class shall be used to save the model after training and load the saved model for prediction.

    Written By: Prafful Agrawal
    Version: 1.0
    Revisions: None
    """
    def __init__(self, aws_operations=None, logger=None):
        self.model_directory = 'models/'
        self.log_file = 'model_operations_logs/model_storage_log.txt'
        if aws_operations is not None:
            self.aws_operations = aws_operations
        else:
            self.aws_operations = aws_storage_operations()
        if logger is not None:
            self.logger = logger
        else:
            self.logger = aws_logger(self.aws_operations)

    def save_model(self, model_name, model):
        """
        Method to save the model to the storage.
        It will overwrite any existing model.

        Input Parameters:
            model_name - name of the model; str
            model - model instance to save
        Output:
            None
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Saving the model to the storage!')
        try:
            model_file_path = self.model_directory + model_name + '/' + model_name + '.sav'
            data_to_write = pickle.dumps(model)
            self.aws_operations.write_file(model_file_path, data_to_write)
            self.logger.log(self.log_file, 'Model saved successfully!')
            return None
        except Exception as e:
            self.logger.log(self.log_file, 'Error occurred while saving the model: %s' % e)
            raise e

    def load_model(self, model_name):
        """
        Method to load the model from the storage.

        Input Parameters:
            model_name - name of the model; str
        Output:
            None - if model does not exist, otherwise
            model instance
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Loading the model from the storage!')
        try:
            model_file_path = self.model_directory + model_name + '/' + model_name + '.sav'
            model_pkl = self.aws_operations.read_file(model_file_path)
            if model_pkl is None:
                return None
            model = pickle.loads(model_pkl)
            self.logger.log(self.log_file, 'Model loaded successfully!')
            return model
        except Exception as e:
            self.logger.log(self.log_file, 'Error occurred while loading the model: %s' % e)
            raise e

    def select_correct_model(self, cluster_number):
        """
        Method to select the correct model for the given cluster.

        Input Parameters:
            cluster_number - cluster number; int
        Output:
            None - if model does not exist, otherwise
            model instance
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Searching for correct model for the given cluster!')
        try:
            model_paths = self.aws_operations.list_files(self.model_directory)
            if model_paths is None:
                raise Exception('Model directory is empty! No model found!')
            for model_path in model_paths:
                model_name = re.split('/', model_path)[-2]
                if model_name[-1] == str(cluster_number):
                    model = self.load_model(model_name)
                    self.logger.log(self.log_file, 'Correct model found successfully!')
                    return model
            self.logger.log(self.log_file, 'Correct model not found!')
            return None
        except Exception as e:
            message = 'Error occurred while searching for the correct model: %s' % e
            self.logger.log(self.log_file, message)
            raise e
