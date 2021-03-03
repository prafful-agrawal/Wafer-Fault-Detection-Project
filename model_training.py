from aws_operations.aws_storage import aws_storage_operations
from application_logging.aws_logging import aws_logger
from model_operations.model_storage import model_storage_operations
from data_loading.training_data_loading import load_training_data
from data_preprocessing.training_data_preprocessing import preprocess_training_data
from data_clustering.training_data_clustering import cluster_training_data
from model_selection.best_model_selection import select_best_model


class train_model:
    """
    This class serves as the entry point for training the model.

    Written By: Prafful Agrawal
    Version: 1.0
    Revisions: None
    """
    def __init__(self):
        self.log_file = 'training_logs/model_training_log.txt'
        self.aws_operations = aws_storage_operations()
        self.logger = aws_logger(self.aws_operations)
        self.model_operations = model_storage_operations(self.aws_operations, self.logger)
        self.data_loading = load_training_data(self.aws_operations, self.logger)
        self.data_preprocessing = preprocess_training_data(self.aws_operations, self.logger)
        self.data_clustering = cluster_training_data(self.aws_operations, self.logger, self.model_operations)
        self.model_selection = select_best_model(self.aws_operations, self.logger)

    def train(self):
        """
        Method to train the model.
        It carries out the following sequence of operations:
            - load the training data from the source,
            - preprocess the training data,
            - cluster the preprocessed data, and
            - train and select the best model for each cluster.
        The selected models are saved to the storage.

        Output:
            None
        On Failure:
            raise Exception

        Written By: Prafful Agrawal
        Version: 1.0
        Revisions: None
        """
        try:
            self.logger.log(self.log_file, 'Loading the training data from the source!')
            data = self.data_loading.load_data()
            if data is None:
                raise Exception('Unable to load the training data!')
            self.logger.log(self.log_file, 'Training data loaded successfully!')

            self.logger.log(self.log_file, 'Preprocessing the training data!')
            # Remove the 'Wafer' column as it does not contribute towards model training
            data = self.data_preprocessing.remove_columns(data, ['Wafer'])
            # Separate features and label columns
            X, y = self.data_preprocessing.separate_features_and_label(data, 'Good/Bad')
            # Check if NULL values are present in the dataset
            is_null_present = self.data_preprocessing.is_null_present(X)
            # If NULL values are present, replace them appropriately
            if is_null_present:
                X = self.data_preprocessing.impute_missing_values(X)
            # Further, check for columns that do not contribute towards model training;
            # if the standard deviation for a column is zero, it means that the column has
            # constant values and it gives the same output for both good and bad sensors.
            # Prepare the list of such columns which will be dropped
            cols_to_drop = self.data_preprocessing.columns_with_zero_standard_deviation(X)
            # Drop these columns
            X = self.data_preprocessing.remove_columns(X, cols_to_drop)
            self.logger.log(self.log_file, 'Training data preprocessed successfully!')

            self.logger.log(self.log_file, 'Applying the clustering approach to training data!')
            # Find the optimum number of clusters using the elbow plot
            optimum_number_of_clusters = self.data_clustering.optimum_number_of_clusters(X)
            # Divide the data into clusters
            list_of_clusters, cluster_labels = \
                self.data_clustering.generate_clusters(X, optimum_number_of_clusters)
            self.logger.log(self.log_file, 'Training data clustered successfully!')

            message = 'Parsing through all the clusters and looking for ' \
                      'the best ML algorithm to fit each individual cluster!'
            self.logger.log(self.log_file, message)
            for i in list_of_clusters:
                # Filter the data for the i-th cluster
                is_ith_cluster = (cluster_labels == i)
                # Prepare the features and label columns
                cluster_features = X[is_ith_cluster]
                cluster_label = y[is_ith_cluster]
                # Select the best model for the i-th cluster
                best_model_name, best_model = \
                    self.model_selection.select_best_model(cluster_features, cluster_label)
                # Save the best model to the directory
                best_model_name = best_model_name + str(i)
                self.model_operations.save_model(best_model_name, best_model)
            self.logger.log(self.log_file, 'Best model for all the clusters found successfully!')
            return None
        except Exception as e:
            self.logger.log(self.log_file, 'Unexpected Error: %s' % e)
            raise e
