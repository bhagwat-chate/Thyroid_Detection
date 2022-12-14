from sklearn.model_selection import train_test_split
from data_preprocessing.preprocessing import Preprocessor
from data_preprocessing.clustering import KMeansClustering
from best_model_finder import tuner
from file_operations import file_methods
from application_logging.logger import App_logger
import pandas as pd
import numpy as np
from Training_Log.clear_log import truncate_file
from data_ingestion.data_loader import data_getter

class trainModel:
    def __init__(self, trainData, labelColumnName, imbalanceThresholdPercentage):
        self.trainData = trainData
        self.labelColumnName = labelColumnName
        self.imbalanceThresholdPercentage = imbalanceThresholdPercentage
        self.logger_object = App_logger()
        self.file_object = open("Model_Log/Model_Training_Log.txt", "a+")
    def trainingModel(self):
        self.logger_object.log(self.file_object, "start of model training")
        self.logger_object.log(self.file_object, "start of data preprocessing\n")
        try:

            data = data_getter.get_data(self, self.trainData)

            preprocessor = Preprocessor(self.file_object, self.logger_object)
            data = preprocessor.drop_unnecessary_columns(data, ['TSH_measured','T3_measured','TT4_measured','T4U_measured','FTI_measured','TBG_measured','TBG','TSH'])
            data = preprocessor.replace_invalid_value_with_null(data)
            data = preprocessor.encode_categorical_values(data)
            is_null_present = preprocessor.is_null_present(data)

            if is_null_present:
                data = preprocessor.impute_missing_value(data)

            data.to_csv("test/after_impute_missing_value.csv", index=False)
            data = preprocessor.handle_imbalance_dataset(data, self.imbalanceThresholdPercentage)
            data.to_csv("test/after_handle_imbalance_dataset.csv", index=False)

            X, Y = preprocessor.separate_label_feature(data, self.labelColumnName)

            self.logger_object.log(self.file_object, "data preprocessing complete\n")
            self.logger_object.log(self.file_object, "start of clustering operation")

            cluster = KMeansClustering(self.file_object, self.logger_object)
            numberOfClusters = cluster.elbow_plot(X)
            X = cluster.create_clusters(X, numberOfClusters)

            #create a new column in the dataset consisting of the corresponding cluster assignments.
            X['Labels']=Y
            list_of_clusters = X['cluster'].unique()

            """parsing all the clusters and looking for the best ML algorithm to fit on individual cluster"""

            for i in list_of_clusters:
                cluster_data = X[X['cluster'] == i]
                cluster_features = cluster_data.drop(['Labels','cluster'], axis=1)
                cluster_label = cluster_data['Labels']

                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size=1/3, random_state=355)
                model_finder = tuner.Model_Finder(self.file_object, self.logger_object)

                best_model_name, best_model = model_finder.get_best_model(x_train, y_train, x_test, y_test)

            self.logger_object.log(self.file_object, "Model training complete")
            self.logger_object.log(self.file_object, "Exited the trainingModel method of the trainModel class")
        except Exception as e:
            self.logger_object.log(self.file_object, '*** Exception occurred in trainingModel method of the trainModel class. Exception: ' + str(e))
            self.logger_object.log(self.file_object, 'Exited the trainingModel method of the trainModel class')


