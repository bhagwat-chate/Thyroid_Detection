from Training_Raw_Data_Validation.rawValidation import Raw_Data_Validation

if __name__ == '__main__':

    obj = Raw_Data_Validation("Thyroid")
    obj.valuesFromSchema()
    obj.createDirectoryGoodBadRawData()
    obj.deleteExistingGoodDataTrainingFolder()

    print("DONE")
