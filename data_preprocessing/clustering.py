import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from file_operations.file_methods import File_Operation

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
            plt.xlable("Number of clusters")
            plt.ylabel("WCSS")
            plt.savefig("data_preprocessing/KMeans_elbow_plot.PNG")
            self.kn = KneeLocator(range(1, 11), wcss, curvw = 'convex', direction='decreasing')
            self.logger_object.log(self.file_object, "done")
            self.logger_object.log(self.file_object, "Exited the elbow_plot method of the KMeansClustering class")
            return self.knn.knee
        except Exception as e:
            self.logger_object.log(self.file_object,'*** Exception occurred in elbow_plot method of the KMeansClustering class. Exception:  ' + str(e))
            self.logger_object.log(self.file_object,'Exited the elbow_plot method of the KMeansClustering class')