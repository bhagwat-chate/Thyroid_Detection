# from training_validation_insertion import train_validation
# from Training_Raw_Data_Validation.rawValidation import Raw_Data_Validation
from Training_Raw_Data_Validation.rawValidation import Raw_Data_Validation


if __name__ == '__main__':

    obj = Raw_Data_Validation()
    t = obj.valuesFromSchema()
    print(t)

    # obj = train_validation()
    # obj.train_validation()