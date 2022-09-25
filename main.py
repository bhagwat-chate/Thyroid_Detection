from training_validation_insertion import train_validation
from trainingModel import trainModel
from Training_Log.clear_log import truncate_file

if __name__ == '__main__':
    cl = truncate_file()
    cl.truncate_content()

    dbName = 'test'
    tableName = 'Thyroid'
    trainData = "Training_FileFromDB/InputFile.csv"
    labelColumnName = "Class"
    imbalanceThresholdPercentage = 28
    obj = train_validation(dbName, tableName)
    obj.train_validation()

    obj = trainModel(trainData, labelColumnName, imbalanceThresholdPercentage)
    obj.trainingModel()

    print("DONE")
