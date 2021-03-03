import matplotlib.pyplot as plt
import numpy as np
from sklearn.cluster import KMeans
from kneed import KneeLocator
from aws_operations.aws_storage import aws_storage_operations
from application_logging.aws_logging import aws_logger
from model_operations.model_storage import model_storage_operations


class cluster_training_data:
    """
    This class shall be used to divide the data into clusters before training.

    Written By: Prafful Agrawal
    Version: 1.0
    Revisions: None
    """
    def __init__(self, aws_operations=None, logger=None, model_operations=None):
        self.plot_file_path = 'elbow_plot.PNG'
        self.random_state = 123
        self.log_file = 'training_logs/training_data_clustering_log.txt'
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

    def optimum_number_of_clusters(self, data):
        """
        Method to find the optimum number of clusters for KMeans clustering for the training data.
        An elbow plot is used for deciding the optimum number of clusters.
        A copy of the elbow plot is also saved to the storage.

        Input parameters:
            data - data; pandas.DataFrame
        Output:
            int - optimum number of clusters
        On Failure:
            raise Exception
        """
        message = 'Searching for the optimum number of clusters for the training data!'
        self.logger.log(self.log_file, message)
        try:
            wcss = []
            # Calculate WCSS for k in the range of 1 to 10
            for k in range(1, 11):
                kmeans_model = KMeans(n_clusters=k, init='k-means++',
                                      random_state=self.random_state)
                kmeans_model.fit(data)
                wcss.append(kmeans_model.inertia_)
            # Generate a graph between the WCSS and the number of clusters
            plt.plot(range(1, 11), wcss)
            plt.title('The Elbow Method')
            plt.xlabel('Number of clusters')
            plt.ylabel('WCSS')
            # Write the plot to AWS storage
            self.aws_operations.write_png(self.plot_file_path, plt)
            # Find the optimum number of clusters
            knl = KneeLocator(range(1, 11), wcss, curve='convex', direction='decreasing')
            optimum_number_of_clusters = knl.knee
            self.logger.log(self.log_file, 'Optimum number of clusters found successfully!')
            return optimum_number_of_clusters
        except Exception as e:
            message = 'Error occurred while searching for optimum number of clusters: %s' % e
            self.logger.log(self.log_file, message)
            raise e

    def generate_clusters(self, data, number_of_clusters):
        """
        Method to generate the given number of clusters from the training data.
        The KMeans clustering model is also saved to the storage to be used during prediction stage.

        Input parameters:
            data - data; pandas.DataFrame
            number_of_clusters - number of clusters; int
        Output:
            numpy.ndarray - list of cluster names, and
            pandas.Series - cluster labels for the data
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Creating clusters for the training data!')
        try:
            kmeans_model = KMeans(n_clusters=number_of_clusters, init='k-means++',
                                  random_state=self.random_state)
            cluster_labels = kmeans_model.fit_predict(data)
            list_of_clusters = np.unique(cluster_labels)
            # Save the KMeans clustering model to the storage
            self.model_operations.save_model('KMeans', kmeans_model)
            self.logger.log(self.log_file, 'Clusters created successfully!')
            return list_of_clusters, cluster_labels
        except Exception as e:
            message = 'Error occurred while creating clusters: %s' % e
            self.logger.log(self.log_file, message)
            raise e
