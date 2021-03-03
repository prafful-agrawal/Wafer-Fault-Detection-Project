import pandas as pd
from aws_operations.aws_storage import aws_storage_operations
from application_logging.aws_logging import aws_logger
from model_operations.model_storage import model_storage_operations
from data_loading.prediction_data_loading import load_prediction_data
from data_preprocessing.prediction_data_preprocessing import preprocess_prediction_data
from data_clustering.prediction_data_clustering import cluster_prediction_data


class make_predictions:
    """
    This class serves as the entry point for making the predictions.

    Written By: Prafful Agrawal
    Version: 1.0
    Revisions: None
    """
    def __init__(self):
        self.prediction_output_path = 'prediction_results/predictions.csv'
        self.log_file = 'prediction_logs/prediction_from_model_log.txt'
        self.aws_operations = aws_storage_operations()
        self.logger = aws_logger(self.aws_operations)
        self.model_operations = model_storage_operations(self.aws_operations, self.logger)
        self.data_loading = load_prediction_data(self.aws_operations, self.logger)
        self.data_preprocessing = preprocess_prediction_data(self.aws_operations, self.logger)
        self.data_clustering = cluster_prediction_data(self.aws_operations, self.logger, self.model_operations)

    def predict(self):
        """
        Method to make predictions.
        It carries out the following sequence of operations:
            - load the prediction data from the source,
            - preprocess the prediction data,
            - cluster the preprocessed data, and
            - select the correct model and make predictions for each cluster.
        The prediction results are saved to the storage.

        Output:
            None
        On Failure:
            raise Exception

        Written By: Prafful Agrawal
        Version: 1.0
        Revisions: None
        """
        try:
            self.logger.log(self.log_file, 'Loading the prediction data from the source!')
            data = self.data_loading.load_data()
            if data is None:
                raise Exception('Unable to load the prediction data!')
            self.logger.log(self.log_file, 'Prediction data loaded successfully!')

            self.logger.log(self.log_file, 'Preprocessing the prediction data!')
            # Separate the 'Wafer' column from the prediction data
            X = self.data_preprocessing.remove_columns(data, ['Wafer'])
            # Check if NULL values are present in the dataset
            is_null_present = self.data_preprocessing.is_null_present(X)
            # If NULL values are present, replace them appropriately
            if is_null_present:
                X = self.data_preprocessing.impute_missing_values(X)
            # Further, check for columns that do not contribute towards making predictions;
            # if the standard deviation for a column is zero, it means that the column has
            # constant values and it gives the same output for both good and bad sensors.
            # Prepare the list of such columns which will be dropped
            cols_to_drop = self.data_preprocessing.columns_with_zero_standard_deviation(X)
            # Drop these columns
            X = self.data_preprocessing.remove_columns(X, cols_to_drop)
            self.logger.log(self.log_file, 'Prediction data preprocessed successfully!')

            self.logger.log(self.log_file, 'Applying the clustering approach to prediction data!')
            # Divide the data into clusters
            list_of_clusters, cluster_labels = self.data_clustering.generate_clusters(X)
            self.logger.log(self.log_file, 'Prediction data clustered successfully!')

            message = 'Parsing through all the clusters and making predictions ' \
                      'using the best algorithm found for that cluster during training!'
            self.logger.log(self.log_file, message)
            # Initialize variables to store predictions
            wafers = pd.Series(name='Wafer')
            outputs = pd.Series(name='Output')
            for i in list_of_clusters:
                # Filter the data for the i-th cluster
                is_ith_cluster = (cluster_labels == i)
                # Prepare the features and wafer columns
                cluster_features = X[is_ith_cluster]
                cluster_wafer = data['Wafer'][is_ith_cluster]
                # Select the correct model for the i-th cluster
                correct_model = self.model_operations.select_correct_model(i)
                if correct_model is None:
                    raise Exception('Best model for cluster %s not found!' % i)
                # Predict the output for the i-th cluster
                cluster_output = correct_model.predict(cluster_features)
                wafers = wafers.append(cluster_wafer, ignore_index=True)
                outputs = outputs.append(pd.Series(cluster_output), ignore_index=True)
            self.logger.log(self.log_file, 'Predictions made for all the clusters successfully!')

            self.logger.log(self.log_file, 'Saving the predictions to the storage!')
            result = pd.DataFrame({'Wafer': wafers, 'Output': outputs}).sort_values('Wafer')
            # Save the results to the storage
            self.aws_operations.write_csv(self.prediction_output_path, result)
            self.logger.log(self.log_file, 'Predictions saved to the storage successfully!')
            # Return the output path and a few predictions
            return self.prediction_output_path, result.head().to_json(orient='records')
        except Exception as e:
            self.logger.log(self.log_file, 'Unexpected Error: %s' % e)
            raise e

