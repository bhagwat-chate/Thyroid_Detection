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
        file_object = open("Training_Log/Log_Values_From_Schema.txt", "a")
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
                file_object = open("Training_Log/Log_Values_From_Schema.txt", "a")
                self.logger.log(file_object, message)

                message = "Exited from the method 'valuesFromSchema' of class 'Raw_Data_Validation'."
                file_object = open("Training_Log/Log_Values_From_Schema.txt", "a")
                self.logger.log(file_object, message)
                file_object.close()

        except Exception as e:
            message = "*** Exception occurred in the method 'valuesFromSchema' of class 'Raw_Data_Validation'. \n {v}".format(v=e)
            file_object = open("Training_Log/Log_Values_From_Schema.txt", "a")
            self.logger.log(file_object, message)
            file_object.close()