from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import roc_auc_score, accuracy_score
from sklearn.neighbors import KNeighborsClassifier

class Model_Finder:

    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.clf = RandomForestClassifier()
        self.knn = KNeighborsClassifier()

    def get_best_params_for_random_forest(self, train_x, train_y):
        """
        Method Name: get_best_params_for_random_forest
        Description: get the parameters for Random Forest Algorithm which give the best accuracy. Use Hyper Parameter Tuning.
        Output: The model with the best parameters
        On Failure: Raise Exception

        Written By: Bhagwat Chate
        Version: 1.0
        Revisions: None
        """
        self.logger_object.log(self.file_object, 'Entered the get_best_params_for_random_forest method of the Model_Finder class')
        try:

            self.param_grid = {'n_estimators': [10, 50, 100, 130], 'criterion': ['gini', 'entropy'], 'max_depth': range(2, 4, 1), 'max_features': ['auto', 'log2']}
            self.grid = GridSearchCV(estimator=self.clf, param_grid=self.param_grid, cv=5, verbose=3)
            self.grid.fit(train_x, train_y)

            self.n_estimators = self.grid.best_params_['n_estimators']
            self.criterion = self.grid.best_params_['criterion']
            self.max_depth = self.grid.best_params_['max_depth']
            self.max_features = self.grid.best_params_['max_features']

            self.clf = RandomForestClassifier(n_estimators=self.n_estimators, max_depth=self.max_depth, max_features=self.max_features, criterion=self.criterion)
            self.clf,fit(train_x, train_y)
            self.logger_object.log(self.file_object, "Random Forest Model best parameters: "+str(self.grid.best_params_))
            self.logger_object.log(self.file_object, "Exited the get_best_params_for_random_forest method of the Model_Finder class")
            return self.clf
        except Exception as e:
            self.logger_object.log(self.file_object, '*** Exception occurred in get_best_params_for_random_forest method of the Model_Finder class. Exception:  ' + str(e))
            self.logger_object.log(self.file_object, 'Exited the get_best_params_for_random_forest method of the Model_Finder class')

    def get_best_params_for_knn(self, train_x, train_y):
        self.logger_object.log(self.file_object, 'Entered the get_best_params_for_knn method of the Model_Finder class')
        try:
            self.param_grid_knn = {
                'algorithm': ['ball_tree', 'kd_tree', 'brute'],
                'leaf_size': [10, 17, 24, 28, 30, 35],
                'n_neighbours': [4, 5, 8, 10, 1],
                'p': [1, 2]
            }
            self.grid = GridSearchCV(self.knn, self.param_grid_knn, verbose=3, cv=5)
            self.grid.fit(train_x, train_y)

            self.algorithm = self.grid.best_params_['algorithm']
            self.leaf_size = self.grid.best_params_['leaf_size']
            self.n_neighbours = self.grid.best_params_['n_neighbours']
            self.p = self.grid.best_params_['p']

            self.knn = KNeighborsClassifier(algorithm=self.algorithm, leaf_size=self.leaf_size, n_neighbors=self.n_neighbours, p=self.p, n_jobs=-1)
            self.knn.fit(train_x, train_y)
            self.logger_object.log(self.file_object, " KNN best parameters "+str(self.grid.best_params_))
            self.logger_object.log(self.file_object, "Exited the get_best_params_for_knn method of the Model_Finder class")
            return self.knn
        except Exception as e:
            self.logger_object.log(self.file_object,'*** Exception occurred in get_best_params_for_knn method of the Model_Finder class. Exception:  ' + str(e))
            self.logger_object.log(self.file_object, 'Exited the get_best_params_for_knn method of the Model_Finder class')
