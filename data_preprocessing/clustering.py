import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from file_operations.file_methods import File_Operation
import numpy as np

class KMeansClustering:
    """
    This class shall be used to divide the data into clusters before training.
    Written By: Bhagwat Chate
    Version: 1.0
    Revisions: None
    """
    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object

    def elbow_plot(self, data):
        self.logger_object.log(self.file_object, 'Entered the elbow_plot method of the KMeansClustering class')
        wcss = []
        try:
            for i in range(1, 11):
                kmeans = KMeans(n_clusters=i, init="k-means++", random_state=42)
                kmeans.fit(data)
                wcss.append(kmeans.inertia_)
            plt.plot(range(1,11), wcss)
            plt.title("The Elbow Method")
            plt.xlabel("Number of clusters")
            plt.ylabel("WCSS")
            plt.savefig("data_preprocessing/KMeans_elbow_plot.PNG")
            self.knn = KneeLocator(range(1, 11), wcss, curve='convex', direction='decreasing')
            self.logger_object.log(self.file_object, "Optimal number of clusters finding complete")
            self.logger_object.log(self.file_object, "Exited the elbow_plot method of the KMeansClustering class\n")
            return self.knn.knee
        except Exception as e:
            self.logger_object.log(self.file_object,'*** Exception occurred in elbow_plot method of the KMeansClustering class. Exception: ' + str(e))
            self.logger_object.log(self.file_object,'Exited the elbow_plot method of the KMeansClustering class')

    def create_clusters(self, data, number_of_clusters):
        self.logger_object.log(self.file_object, 'Entered the create_clusters method of the KMeansClustering class')
        self.data = data
        try:
            self.kmeans = KMeans(n_clusters=number_of_clusters, init="k-means++", random_state=42)
            self.y_kmeans = self.kmeans.fit_predict(data)
            self.file_op = File_Operation(self.file_object, self.logger_object)
            self.save_model = self.file_op.save_model(self.kmeans, 'KMeans')
            self.data['cluster'] = self.y_kmeans
            self.logger_object.log(self.file_object, "Clusters created, cluster count: "+str(len(np.unique(self.y_kmeans))))
            self.logger_object.log(self.file_object, "Exited the create_clusters method of the KMeansClustering class")
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object, '*** Exception occurred in ******* method of the KMeansClustering class. Exception:  ' + str(e))
            self.logger_object.log(self.file_object, 'Exited the create_clusters method of the KMeansClustering class')