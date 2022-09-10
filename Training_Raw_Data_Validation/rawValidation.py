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
        self.LengthOfDateStampInFile = 0
        self.LengthOfTimeStampInFile = 0
        self.NumberOfColumns = 0
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
                self.LengthOfDateStampInFile = dic["LengthOfDateStampInFile"]
                self.LengthOfTimeStampInFile = dic["LengthOfTimeStampInFile"]
                self.NumberOfColumns = dic["NumberOfColumns"]

                message = "Length Of Date Stamp In File:: %s" %self.LengthOfDateStampInFile + "\t" + "Length Of Time Stamp In File:: %s" %self.LengthOfTimeStampInFile+"\t"+"Number Of Columns:: %s" %self.NumberOfColumns
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

    def validateColumnLength(self):
        log_file_object = open("Training_Log/Training_Raw_File_Validation_Log.txt", "a")
        log_file_column_length = open("Training_Log/Training_Raw_File_Col_Length.txt", "a")
        message = "Entered into the method 'validateColumnLength' of class 'Raw_Data_Validation'."
        self.logger.log(log_file_object, message)
        try:
            for file in listdir("Training_Batch_Files/"):
                csv = pd.read_csv("Training_Batch_Files/"+file)
                if csv.shape[1] == self.NumberOfColumns:
                    shutil.copy("Training_Batch_Files/"+file, "Training_Raw_Files_Validated/Good_Raw/")
                    self.logger.log(log_file_column_length, "valid column length for the file: {v}, moved to Good_Raw directory".format(v=file))
                else:
                    shutil.copy("Training_Batch_Files/"+file, "Training_Raw_Files_Validated/Bad_Raw/")
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

    def validationFileNameRaw(self):
        """
        Method Name: validationFileNameRaw
        Description: This function validates the name of the training csv files as per given name in the schema!
                    Regex pattern is used to do the validation.If name format do not match the file is moved
                    to Bad Raw Data folder else in Good raw data.
        Output: None
        On Failure: Exception
        Written By: Bhagwat Chate
        Version: 1.0
        Revisions: None
        """
        log_file = open("Training_Log/Training_Raw_File_Validation_Log.txt", "a")
        message = "Entered into the method 'validationFileNameRaw' of class 'Raw_Data_Validation'."
        self.logger.log(log_file, message)

        # self.deleteExistingGoodDataTrainingFolder()
        # self.deleteExistingBadDataTrainingFolder()
        # self.createDirectoryGoodBadRawData()

        onlyfiles = [f for f in listdir("Training_Raw_Files_Validated/Good_Raw/")]
        regex = self.manualRegexCreation()

        try:
            f = open("Training_Log/Training_Raw_File_Name_Log.txt", "a")
            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))

                    if (len(splitAtDot[1]) == self.LengthOfDateStampInFile) and (len(splitAtDot[2]) == self.LengthOfTimeStampInFile):
                        self.logger.log(f, "valid file name: {v}".format(v=filename))
                    else:
                        shutil.move("Training_Raw_Files_Validated/Good_Raw/"+filename, "Training_Raw_Files_Validated/Bad_Raw/")
                        self.logger.log(f, "invalid file name: {v}, file moved to Bad_Raw".format(v=filename))
                else:
                    shutil.move("Training_Raw_Files_Validated/Good_Raw/" + filename, "Training_Raw_Files_Validated/Bad_Raw/")
                    self.logger.log(f, "invalid file name: {v}, file moved to Bad_Raw".format(v=filename))

            self.logger.log(log_file, "validation of file name raw complete!")
            self.logger.log(log_file, "Exited from the method 'validationFileNameRaw' of class 'Raw_Data_Validation'." + '\n')
            f.close()
            log_file.close()
        except Exception as e:
            message = "*** Exception occurred in the method 'validationFileNameRaw' of class 'Raw_Data_Validation'. \n {v}".format(v=e)
            self.logger.log(log_file, message)
            log_file.close()

    def validateMissingValuesInWholeColumn(self):
        """
        Method Name: validateMissingValuesInWholeColumn
        Description: This function validates if any column in the csv file has all values missing.
                     If all the values are missing, the file is not suitable for processing.
                     SUch files are moved to bad raw data.
        Output: None
        On Failure: Exception
        Written By: Bhagwat Chate
        Version: 1.0
        Revisions: None
        """
        log_file = open("Training_Log/Training_Raw_File_Validation_Log.txt", "a")
        log_col_missing_value = open("Training_Log/Training_Raw_File_Col_Value_Len.txt", "a")
        message = "Entered into the method 'validateMissingValuesInWholeColumn' of class 'Raw_Data_Validation'."
        self.logger.log(log_file, message)

        try:
            for file in listdir("Training_Raw_Files_Validated/Good_Raw/"):
                csv = pd.read_csv("Training_Raw_Files_Validated/Good_Raw/"+file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count = count + 1
                        shutil.move("Training_Raw_Files_Validated/Good_Raw/" + file, "Training_Raw_Files_Validated/Bad_Raw/")
                        self.logger.log(log_col_missing_value, "invalid column length in file: {v}, file moved to Bad_Raw".format(v=file))
                        break
                if count == 0:
                    self.logger.log(log_col_missing_value, "no column with 100% missing values in file: {v}".format(v=file))

            self.logger.log(log_file, "validation of missing values in whole column complete.")
            self.logger.log(log_file,"Exited from the method 'validateMissingValuesInWholeColumn' of class 'Raw_Data_Validation'." + '\n')
            log_col_missing_value.close()
            log_file.close()
        except Exception as e:
            message = "*** Exception occurred in the method 'validateMissingValuesInWholeColumn' of class 'Raw_Data_Validation'. \n {v}".format(v=e)
            self.logger.log(log_col_missing_value, message)
            self.logger.log(log_file, message)
            log_file.close()
            log_col_missing_value.close()

    # def moveBadFilesToArchiveBad(self):
    #     """
    #     Method Name: moveBadFilesToArchiveBad
    #     Description: This method deletes the directory made  to store the Bad Data
    #                  after moving the data in an archive folder. We archive the bad
    #                  files to send them back to the client for invalid data issue.
    #     Output: None
    #     On Failure: OSError
    #     Written By: Bhagwat Chate
    #     Version: 1.0
    #     Revisions: None
    #     """
    #     log_file = open("Training_Log/Training_Raw_File_Validation_Log.txt", "a")
    #     message = "Entered into the method 'moveBadFilesToArchiveBad' of class 'Raw_Data_Validation'."
    #     self.logger.log(log_file, message)
    #
    #     now = datetime.now()
    #     date = now.date()
    #     time = now.strftime("%H:%M:%S")
    #
    #     try:
    #         source = "Training_Raw_Files_Validated/Bad_Raw/"
    #         if os.path.isdir(source):
    #             path = "TrainingArchiveBadData"
    #             if not os.path.isdir(path):
    #                 os.makedirs(path)
    #             dest = "TrainingArchiveBadData/BadData_"+str(date)+"_"+str(time)
    #             if not os.path.isdir(dest):
    #                 os.makedirs(dest)
    #             files = os.listdir(source)
    #             for f in files:
    #                 if f not in os.listdir(dest):
    #                     shutil.move(source + f, dest)
    #             self.logger.log(log_file, "Bad files moved to archive")
    #             if os.path.isdir(path + 'Bad_Raw/'):
    #                 os.rmtree(path + 'Bad_Raw')
    #             self.logger.log(log_file, "Bad Raw data directory deleted.")
    #         self.logger.log(log_file,"Exited from the method 'moveBadFilesToArchiveBad' of class 'Raw_Data_Validation'." + '\n')
    #         log_file.close()
    #     except Exception as e:
    #         message = "*** Exception occurred in the method 'moveBadFilesToArchiveBad' of class 'Raw_Data_Validation'. \n {v}".format(v=e)
    #         self.logger.log(log_file, message)
    #         log_file.close()

    def moveBadFilesToArchiveBad(self):
        """
        Method Name: moveBadFilesToArchiveBad
        Description: This method delete the directory made to store the Bad Data after moving the data in an archive
                     folder. We archive the bad data files to send them back to the client for invalid data issue.
        Output: None
        On Failure: OSError

        Written By: Bhagwat Chate
        Version: 1.0
        Revision: None
        """
        file = open("Training_Log/Training_Raw_File_Validation_Log.txt", "a")
        message = "Entered into the method 'moveBadFilesToArchiveBad' of class 'Raw_Data_Validation'."
        self.logger.log(file, message)

        now = datetime.now()
        date = now.date()
        time = now.time()

        try:
            source = "Training_Raw_Files_Validated/Bad_Raw/"
            if os.path.isdir(source):
                path = "TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                time = str(time)
                time = time.replace(":", "_")
                dest = "TrainingArchiveBadData/BadData_" + str(date) + "_" + str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
                self.logger.log(file, "Files successfully moved to: "+str(dest))
                self.deleteExistingBadDataTrainingFolder()
            else:
                self.logger.log(file, "Nothing for archive!")
            self.logger.log(file, "Training Bad Raw Data archive complete, Bad Raw directory deleted.")
            self.logger.log(file,"Exited from the method 'moveBadFilesToArchiveBad' of class 'Raw_Data_Validation'." + '\n')
            file.close()
        except OSError as e:
            file = open("Training_Log/Training_Raw_File_Validation_Log.txt", "a")
            message = "OS Error occurred while creating "+str(dest)+ " and moving files from " + str(source)
            self.logger.log(file, message)
            file.close()
        except Exception as e:
            file = open("Training_Log/Training_Raw_File_Validation_Log.txt", "a")
            message = "Exception occurred while creating " + str(dest) + " and moving files from " + str(source)
            self.logger.log(file, message)
            self.logger.log(file, "Exception: {v}".format(v=e))
            file.close()