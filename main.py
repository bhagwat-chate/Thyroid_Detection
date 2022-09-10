from Training_Raw_Data_Validation.rawValidation import Raw_Data_Validation

if __name__ == '__main__':

    obj = Raw_Data_Validation("Thyroid")

    obj.deleteExistingGoodDataTrainingFolder()
    obj.deleteExistingBadDataTrainingFolder()
    obj.createDirectoryGoodBadRawData()
    obj.valuesFromSchema()
    obj.validateColumnLength()
    obj.validationFileNameRaw()
    obj.validateMissingValuesInWholeColumn()

    print("DONE")
