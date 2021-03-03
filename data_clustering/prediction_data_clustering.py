import numpy as np
from aws_operations.aws_storage import aws_storage_operations
from application_logging.aws_logging import aws_logger
from model_operations.model_storage import model_storage_operations


class cluster_prediction_data:
    """
    This class shall be used to divide the data into clusters before prediction.

    Written By: Prafful Agrawal
    Version: 1.0
    Revisions: None
    """
    def __init__(self, aws_operations=None, logger=None, model_operations=None):
        self.log_file = 'prediction_logs/prediction_data_clustering_log.txt'
        if aws_operations is not None:
            self.aws_operations = aws_operations
        else:
            self.aws_operations = aws_storage_operations()
        if logger is not None:
            self.logger = logger
        else:
            self.logger = aws_logger(self.aws_operations)
        if model_operations:
            self.model_operations = model_operations
        else:
            self.model_operations = model_storage_operations(self.aws_operations, self.logger)

    def generate_clusters(self, data):
        """
        Method to generate clusters for the prediction data.
        The KMeans clustering model from the training stage is loaded from the
        storage and used for generating the clusters for the prediction data.

        Input parameters:
            data - data; pandas.DataFrame
        Output:
            numpy.ndarray - list of cluster names, and
            pandas.Series - cluster labels for the data
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Creating clusters for the prediction data!')
        try:
            kmeans_model = self.model_operations.load_model('KMeans')
            if kmeans_model is None:
                raise Exception('KMeans model not found!')
            cluster_labels = kmeans_model.predict(data)
            list_of_clusters = np.unique(cluster_labels)
            self.logger.log(self.log_file, 'Clusters created successfully!')
            return list_of_clusters, cluster_labels
        except Exception as e:
            message = 'Error occurred while creating clusters: %s' % e
            self.logger.log(self.log_file, message)
            raise e
