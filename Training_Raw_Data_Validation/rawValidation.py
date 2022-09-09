import shutil
import pandas as pd
from datetime import datetime
from os import listdir
import os
import re
import json
from application_logging.logger import App_logger

class Raw_Data_Validation:
    """
    Description: This class shall be used for raw data validation.
    Written By: Bhagwat Chate
    Version: 1.0
    Revision: None
    """

    def __init__(self, path):
        self.batch_directory = path
        self.schema_path = 'schema_training.json'
        self.logger = App_logger()

    def valuesFromSchema(self):
        """
        Method Name: valueFromSchema
        Description: This method extract details from predefined schema file for validation purpose.
        Written By: Bhagwat Chate
        Version: 1.0
        Revision: None
        """
        message = "Entered into the method 'valuesFromSchema' of class 'Raw_Data_Validation'."
        file_object = open("Training_Log/Training_Raw_File_Validation_Log.txt", "a")
        self.logger.log(file_object, message)
        try:
            with open(self.schema_path, "r") as f:
                dic = json.load(f)
                f.close()
                pattern = dic["SampleFileName"]
                LengthOfDateStampInFile = dic["LengthOfDateStampInFile"]
                LengthOfTimeStampInFile = dic["LengthOfTimeStampInFile"]
                NumberOfColumns = dic["NumberOfColumns"]

                message = "Length Of Date Stamp In File:: %s" %LengthOfDateStampInFile + "\t" + "Length Of Time Stamp In File:: %s" %LengthOfTimeStampInFile+"\t"+"Number Of Columns:: %s" %NumberOfColumns
                file_object = open("Training_Log/Training_Raw_File_Validation_Log.txt", "a")
                self.logger.log(file_object, message)

                message = "Exited from the method 'valuesFromSchema' of class 'Raw_Data_Validation'."
                file_object = open("Training_Log/Training_Raw_File_Validation_Log.txt", "a")
                self.logger.log(file_object, message+'\n')
                file_object.close()

        except Exception as e:
            message = "*** Exception occurred in the method 'valuesFromSchema' of class 'Raw_Data_Validation'. \n {v}".format(v=e)
            file_object = open("Training_Log/Training_Raw_File_Validation_Log.txt", "a")
            self.logger.log(file_object, message)
            file_object.close()

    def manualRegexCreation(self):
        """
            Method Name: manualRegexCreation
            Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                         This Regex is used to validate the filename of the training data.
            Output: Regex pattern
            On Failure: None

            Written By: Bhagwat Chate
            Version: 1.0
            Revisions: None
        """
        regex = "['hypothyroid']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryGoodBadRawData(self):
        """
        Method Name: createDirectoryForGoodBadRawData
        Description: This method creates directories to store the Good Data and Bad Data after validating the training data.

        Output: None
        On Failure: OSError

        Written By: Bhagwat Chate
        Version: 1.0
        Revisions: None
        """
        message = "Entered into the method 'createDirectoryGoodBadRawData' of class 'Raw_Data_Validation'."
        file_object = open("Training_Log/Training_Raw_File_Validation_Log.txt", "a")
        self.logger.log(file_object, message)
        try:
            path = os.path.join("Training_Raw_Files_Validated/", "Good_Raw")
            if not os.path.isdir(path):
                os.makedirs(path)

            path = os.path.join("Training_Raw_Files_Validated/", "Bad_Raw")
            if not os.path.isdir(path):
                os.makedirs(path)
            message = "Training_Raw_Files_Validated/Good_Raw & Bad_Raw directory created."
            file_object = open("Training_Log/Training_Raw_File_Validation_Log.txt", "a")
            self.logger.log(file_object, message)

            message = "Exited from the method 'createDirectoryGoodBadRawData' of class 'Raw_Data_Validation'."
            file_object = open("Training_Log/Training_Raw_File_Validation_Log.txt", "a")
            self.logger.log(file_object, message+'\n')
            file_object.close()

        except Exception as e:
            message = "*** Exception occurred in the method 'createDirectoryGoodBadRawData' of class 'Raw_Data_Validation'. \n {v}".format(v=e)
            file_object = open("Training_Log/Training_Raw_File_Validation_Log.txt", "a")
            self.logger.log(file_object, message)
            file_object.close()

    def deleteExistingGoodDataTrainingFolder(self):
        """
        Method Name: deleteExistingGoodDataTrainingFolder
        Description: This method deletes the directory made  to store the Good Data after loading the data in the table. Once the good files are
                      loaded in the DB,deleting the directory ensures space optimization.
        Output: None
        On Failure: OSError

        Written By: Bhagwat Chate
        Version: 1.0
        Revisions: None
        """
        file_object = open("Training_Log/Training_Raw_File_Validation_Log.txt", "a")
        message = "Entered into the method 'deleteExistingGoodDataTrainingFolder' of class 'Raw_Data_Validation'."
        self.logger.log(file_object, message)
        try:
            path = "Training_Raw_Files_Validated/"
            if os.path.isdir(path + "/Good_Raw"):
                shutil.rmtree(path + "/Good_Raw")
                message = "Good_Raw directory deleted successfully."
                self.logger.log(file_object, message)
            else:
                self.logger.log(file_object, "'Good_Raw' directory not available for delete")
            message = "Exited from the method 'deleteExistingGoodDataTrainingFolder' of class 'Raw_Data_Validation'."
            self.logger.log(file_object, message+'\n')
            file_object.close()
        except Exception as e:
            message = "*** Exception occurred in the method 'deleteExistingGoodDataTrainingFolder' of class 'Raw_Data_Validation'. \n {v}".format(v=e)
            self.logger.log(file_object, message)
            file_object.close()

    def deleteExistingBadDataTrainingFolder(self):
        """
        Method Name: deleteExistingBadDataTrainingFolder
        Description: This method deletes the directory made  to store the Good Data after loading the data in the table. Once the good files are
                      loaded in the DB,deleting the directory ensures space optimization.
        Output: None
        On Failure: OSError

        Written By: Bhagwat Chate
        Version: 1.0
        Revisions: None
        """
        file_object = open("Training_Log/Training_Raw_File_Validation_Log.txt", "a")
        message = "Entered into the method 'deleteExistingBadDataTrainingFolder' of class 'Raw_Data_Validation'."
        self.logger.log(file_object, message)
        try:
            path = "Training_Raw_Files_Validated/"
            if os.path.isdir(path + "/Bad_Raw"):
                shutil.rmtree(path + "/Bad_Raw")
                message = "'Bad_Raw' directory deleted successfully."
                file_object = open("Training_Log/Training_Raw_File_Validation_Log.txt", "a")
                self.logger.log(file_object, message)
            else:
                self.logger.log(file_object, "'Bad_Raw' directory not available for delete")
            message = "Exited from the method 'deleteExistingBadDataTrainingFolder' of class 'Raw_Data_Validation'."
            self.logger.log(file_object, message+'\n')
            file_object.close()
        except Exception as e:
            message = "*** Exception occurred in the method 'deleteExistingBadDataTrainingFolder' of class 'Raw_Data_Validation'. \n {v}".format(v=e)
            self.logger.log(file_object, message)
            file_object.close()

    def validateColumnLength(self, numberOfColumn):
        log_file_object = open("Training_Log/Training_Raw_File_Validation_Log.txt", "a")
        log_file_column_length = open("Training_Log/Training_Raw_File_Col_Length.txt", "a")
        message = "Entered into the method 'validateColumnLength' of class 'Raw_Data_Validation'."
        self.logger.log(log_file_object, message)
        try:
            for file in listdir("Training_Raw_Files_Validated/Good_Raw/"):
                csv = pd.read_csv("Training_Raw_Files_Validated/Good_Raw/"+file)
                if csv.shape[1] == numberOfColumn:
                    self.logger.log(log_file_column_length, "valid column length for the file: %s" % file)
                else:
                    shutil.move("Training_Raw_Files_Validated/Good_Raw/"+file, "Training_Raw_Files_Validated/Bad_Raw/")
                    self.logger.log(log_file_column_length, "invalid column length for the file: {v}, moved to Bad_Raw directory".format(v=file))
            log_file_column_length.close()
            self.logger.log(log_file_object, "Column length validation complete!")
            self.logger.log(log_file_object, "Exited from the method 'validateColumnLength' of class 'Raw_Data_Validation'."+'\n')
            log_file_object.close()
        except Exception as e:
            message = "*** Exception occurred in the method 'validateColumnLength' of class 'Raw_Data_Validation'. \n {v}".format(v=e)
            self.logger.log(log_file_object, message)
            log_file_object.close()
            log_file_column_length.close()
