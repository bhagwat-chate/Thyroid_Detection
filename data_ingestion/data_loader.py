import pandas as pd

class data_getter:
    """
    This class shall  be used for obtaining the data from the source for training.
    Written By: Bhagwat Chate
    Version: 1.0
    Revisions: None
    """
    def __init__(self, file_object, logger_object):
        self.training_file = 'Training_FileFromDB/InputFile.csv'
        self.file_object = file_object
        self.logger_object = logger_object

    def get_data(self, trainData):
        """
        Method Name: get_data
        Description: This method reads the data from source.
        Output: A pandas DataFrame.
        On Failure: Raise Exception

        Written By: Bhagwat Chate
        Version: 1.0
        Revisions: None
        """
        self.logger_object.log(self.file_object, "Entered into the get_data method of the class data_getter")
        try:
            self.data = pd.read_csv(trainData)
            self.logger_object.log(self.file_object, "Data load successful")
            self.logger_object.log(self.file_object, "Exit the get_data method\n")
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object, "*** Exception occurred in get_data method, error: %s" % e)
