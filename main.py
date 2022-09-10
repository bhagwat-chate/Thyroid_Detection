from Training_Raw_Data_Validation.rawValidation import Raw_Data_Validation
from DataType_Validation_Insertion_Training.DataTypeValidation import DBOperation
from Training_Log.clear_log import truncate_file

if __name__ == '__main__':

    cl = truncate_file()
    cl.truncate_content()

    obj = Raw_Data_Validation("Thyroid")

    obj.deleteExistingGoodDataTrainingFolder()
    obj.deleteExistingBadDataTrainingFolder()
    obj.createDirectoryGoodBadRawData()
    obj.valuesFromSchema()
    obj.validateColumnLength()
    obj.validationFileNameRaw()
    obj.validateMissingValuesInWholeColumn()
    obj.moveBadFilesToArchiveBad()

    obj = DBOperation()
    obj.createDatabaseConnection('test')


    print("DONE")
