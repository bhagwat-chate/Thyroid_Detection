from Training_Raw_Data_Validation.rawValidation import Raw_Data_Validation
from DataType_Validation_Insertion_Training.DataTypeValidation import DBOperation
from Training_Log.clear_log import truncate_file
from application_logging.logger import App_logger

class train_validation:
    def __init__(self, dbName, tableName):

        self.raw_data_validation = Raw_Data_Validation("Thyroid")
        self.dbops = DBOperation()
        self.goodRaw = "Training_Raw_Files_Validated/Good_Raw"
        self.dbName = dbName
        self.tableName = tableName
        self.log_file = open("Training_Log/01_Final_Training_Log.txt", "a+")
        self.log_object = App_logger()

    def train_validation(self):

        cl = truncate_file()
        cl.truncate_content()

        self.log_object.log(self.log_file, "Training raw data file validation start!")

        self.raw_data_validation.deleteExistingGoodDataTrainingFolder()
        self.log_object.log(self.log_file, "Delete existing good data training directory (if any)")

        self.raw_data_validation.deleteExistingBadDataTrainingFolder()
        self.log_object.log(self.log_file, "Delete existing bad data training directory (if any)")

        self.raw_data_validation.createDirectoryGoodBadRawData()
        self.log_object.log(self.log_file, "Delete existing good & bad data training directories")

        self.raw_data_validation.valuesFromSchema()
        self.log_object.log(self.log_file, "Fetch values from schema file")

        self.raw_data_validation.validateColumnLength()
        self.log_object.log(self.log_file, "validate Column Length")

        self.raw_data_validation.validationFileNameRaw()
        self.log_object.log(self.log_file, "validation file name raw")

        self.raw_data_validation.validateMissingValuesInWholeColumn()
        self.log_object.log(self.log_file, "validate missing values in whole column")

        self.raw_data_validation.moveBadFilesToArchiveBad()
        self.log_object.log(self.log_file, "move bad files to archive Bad directory")
        self.log_object.log(self.log_file, "Training raw data file validation complete!\n")

        self.log_object.log(self.log_file, "Database operations start!")

        self.dbops.createDatabaseConnection(self.dbName)
        self.log_object.log(self.log_file, "Create database")

        self.dbops.createTable(self.dbName, self.tableName)
        self.log_object.log(self.log_file, "Create table")

        self.dbops.missingValueImpute(self.goodRaw, "hypothyroid_0211198_0102062112016.csv")
        self.log_object.log(self.log_file, "Impute missing values")

        self.dbops.insertIntoTable(self.dbName, self.tableName)
        self.log_object.log(self.log_file, "Load data in to table")

        self.dbops.exportDataForTraining(self.tableName)
        self.log_object.log(self.log_file, "Export data for training")
        self.log_object.log(self.log_file, "Database operations complete!")