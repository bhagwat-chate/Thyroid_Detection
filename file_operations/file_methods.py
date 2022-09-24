import pickle
import os
import shutil

class File_Operation:
    """
    This class shall be used to save the model after training and load the saved model for prediction.
    Written By: Bhagwat Chate
    Version: 1.0
    Revisions: None
    """

    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.model_directory = "models/"

    def save_model(self, model, filename):
        self.logger_object.log(self.file_object, 'Entered the save_model method of the File_Operation class')
        try:
            path = os.path.join(self.model_directory, filename)
            if os.path.isdir(path):
                shutil.rmtree(self.model_directory)
                os.makedirs(path)
            else:
                os.makedirs(path)
            with open(path + "/" + filename + ".sav", "wb") as f:
                pickle.dump(model, f)
            self.logger_object.log(self.file_object, 'model file %s saved' %filename)
            self.logger_object.log(self.file_object, 'Exit from save_model method of the File_Operation class')
            return 'success'
        except Exception as e:
            self.logger_object.log(self.file_object, '*** Exception occurred in save_model method of the File_Operation class. Exception:  ' + str(e))
            self.logger_object.log(self.file_object, 'Exited the save_model method of the File_Operation class')
            raise Exception()

    def load_model(self, filename):
        self.logger_object.log(self.file_object, 'Entered the save_model method of the File_Operation class')
        try:
            with open(self.model_directory + filename + "/" + filename + ".sav") as f:
                self.logger_object.log(self.file_object, 'model file %s loaded' % filename)
                self.logger_object.log(self.file_object, "Exited the load_model method of the File_Operation class")
                return pickle.load(f)
        except Exception as e:
            self.logger_object.log(self.file_object,'*** Exception occurred in save_model method of the File_Operation class. Exception:  ' + str(e))
            self.logger_object.log(self.file_object,'Exited the save_model method of the File_Operation class')

    def find_correct_model_file(self, cluster_number):
        self.logger_object.log(self.file_object, 'Entered the find_correct_model_file method of the File_Operation class')
        try:
            self.cluster_number = cluster_number
            self.directory_name = self.model_directory
            self.list_of_model_files = []
            self.list_of_files = os.listdir(self.directory_name)
            for self.file in self.list_of_files:
                try:
                    if self.file.index(str(self.cluster_number)) != 1:
                        self.model_name = self.file
                except:
                    continue
            self.model_name = self.model_name.split(".")[0]
            self.logger_object.log(self.file_object, 'model file %s loaded' % filename)
            self.logger_object.log(self.file_object, "Exited the find_correct_model_file method of the File_Operation class")
            return self.model_name
        except Exception as e:
            self.logger_object.log(self.file_object,'*** Exception occurred in find_correct_model_file method of the File_Operation class. Exception:  ' + str(e))
            self.logger_object.log(self.file_object,'Exited the find_correct_model_file method of the File_Operation class')




   