from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.model_selection import GridSearchCV
from aws_operations.aws_storage import aws_storage_operations
from application_logging.aws_logging import aws_logger


class select_best_model:
    """
    This class shall be used to select the model with the best accuracy and AUC score.

    Written By: Prafful Agrawal
    Version: 1.0
    Revisions: None
    """
    def __init__(self, aws_operations=None, logger=None):
        self.test_size = 0.25
        self.random_state = 123
        self.rfc = RandomForestClassifier(random_state=self.random_state)
        self.xgbc = XGBClassifier(objective='binary:logistic',
                                  random_state=self.random_state)
        self.log_file = 'training_logs/best_model_selection_log.txt'
        if aws_operations is not None:
            self.aws_operations = aws_operations
        else:
            self.aws_operations = aws_storage_operations()
        if logger is not None:
            self.logger = logger
        else:
            self.logger = aws_logger(self.aws_operations)

    def train_test_split_data(self, X, y):
        """
        Method to split the given data into train and test sets.

        Input parameters:
            X - features; pandas.DataFrame
            y - label; pandas.Series
        Output:
            pandas.DataFrame - train dataset of features,
            pandas.DataFrame - test dataset of features,
            pandas.Series - train dataset of label, and
            pandas.Series - test dataset of label
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Splitting data into Train-Test sets!')
        try:
            X_train, X_test, y_train, y_test = \
                train_test_split(X, y, test_size=self.test_size, random_state=self.random_state)
            self.logger.log(self.log_file, 'Data split successfully!')
            return X_train, X_test, y_train, y_test
        except Exception as e:
            self.logger.log(self.log_file, 'Error occurred while splitting data: %s' % e)
            raise e

    def find_best_random_forest_model(self, X_train, y_train):
        """
        Method to find the best random forest model using grid search cross validation method.

        Input parameters:
            X_train - train dataset of features; pandas.DataFrame
            y_train - train dataset of label; pandas.Series
        Output:
            RandomForestClassifier - an instance of the best random forest model
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Searching for the best random forest model!')
        try:
            # Define the combination of parameters
            param_grid = {'n_estimators': [10, 50, 100, 130],
                          'criterion': ['gini', 'entropy'],
                          'max_depth': range(2, 4, 1),
                          'max_features': ['auto', 'log2']}
            gscv = GridSearchCV(estimator=self.rfc, param_grid=param_grid, cv=5, verbose=3)
            gscv.fit(X_train, y_train)
            # Initialize an instance using the best estimator
            rfc_best = gscv.best_estimator_
            rfc_best.fit(X_train, y_train)
            self.logger.log(self.log_file, 'Best random forest model found successfully!')
            return rfc_best
        except Exception as e:
            message = 'Error occurred while searching for the best random forest model: %s' % e
            self.logger.log(self.log_file, message)
            raise e

    def find_best_xgboost_model(self, X_train, y_train):
        """
        Method to find the best XGBoost model using grid search cross validation method.

        Input parameters:
            X_train - train dataset of features; pandas.DataFrame
            y_train - train dataset of label; pandas.Series
        Output:
            XGBClassifier - an instance of the best XGBoost model
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Searching for the best XGBoost model!')
        try:
            # Define the combination of parameters
            param_grid = {'learning_rate': [0.5, 0.1, 0.01, 0.001],
                          'max_depth': [3, 5, 10, 20],
                          'n_estimators': [10, 50, 100, 200]}
            gscv = GridSearchCV(estimator=self.xgbc, param_grid=param_grid, cv=5, verbose=3)
            gscv.fit(X_train, y_train)
            # Initialize an instance using the best estimator
            xgbc_best = gscv.best_estimator_
            xgbc_best.fit(X_train, y_train)
            self.logger.log(self.log_file, 'Best XGBoost model found successfully!')
            return xgbc_best
        except Exception as e:
            message = 'Error occurred while searching for the best XGBoost model: %s' % e
            self.logger.log(self.log_file, message)
            raise e

    def select_best_model(self, features, label):
        """
        Method to select the model with the best accuracy and AUC score.
        It searches between the random forest and the XGBoost algorithms.

        Input parameters:
            features - features; pandas.DataFrame
            label - label; pandas.Series
        Output:
            str - name of the best model, and
            RandomForestClassifier or XGBClassifier - an instance of the best model
        On Failure:
            raise Exception
        """
        self.logger.log(self.log_file, 'Searching for the best model for the given data!')
        try:
            # Train-Test split the data
            X_train, X_test, y_train, y_test = self.train_test_split_data(features, label)

            # Find best random forest model
            random_forest_model = self.find_best_random_forest_model(X_train, y_train)
            random_forest_predictions = random_forest_model.predict(X_test)
            # If there is only one label in y, then roc_auc_score returns error;
            # Use accuracy_score in that case
            if y_test.nunique() == 1:
                random_forest_score = accuracy_score(y_test, random_forest_predictions)
                self.logger.log(self.log_file, 'Accuracy for RF:\t' + str(random_forest_score))
            else:
                random_forest_score = roc_auc_score(y_test, random_forest_predictions)
                self.logger.log(self.log_file, 'AUC for RF:\t' + str(random_forest_score))

            # Find best XGBoost model
            xgboost_model = self.find_best_xgboost_model(X_train, y_train)
            xgboost_predictions = xgboost_model.predict(X_test)
            # If there is only one label in y, then roc_auc_score returns error;
            # Use accuracy_score in that case
            if y_test.nunique() == 1:
                xgboost_score = accuracy_score(y_test, xgboost_predictions)
                self.logger.log(self.log_file, 'Accuracy for XGBoost:\t' + str(xgboost_score))
            else:
                xgboost_score = roc_auc_score(y_test, xgboost_predictions)
                self.logger.log(self.log_file, 'AUC for XGBoost:\t' + str(xgboost_score))

            # Compare the two models
            self.logger.log(self.log_file, 'Best model for the given data found successfully!')
            if random_forest_score > xgboost_score:
                return 'RandomForest', random_forest_model
            else:
                return 'XGBoost', xgboost_model
        except Exception as e:
            message = 'Error occurred while searching for the best model: %s' % e
            self.logger.log(self.log_file, message)
            raise e
