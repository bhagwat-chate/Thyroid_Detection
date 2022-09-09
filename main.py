from Training_Raw_Data_Validation.rawValidation import Raw_Data_Validation

if __name__ == '__main__':

    obj1 = Raw_Data_Validation("Thyroid")
    obj1.valuesFromSchema()
    obj1.createDirectoryGoodBadRawData()

    print("DONE")
