from sklearn.model_selection import train_test_split
from data_ingestion import data_loader
from data_preprocessing.preprocessing import Preprocessor
# from data_preprocessing import clustering
# from best_model_finder import tuner
# from file_operations import file_methods
from application_logging.logger import App_logger
import pandas as pd
from Training_Log.clear_log import truncate_file


# class trainModel:
#     def __init__(self):
#         pass

if __name__ == "__main__":
        cl = truncate_file()
        cl.truncate_content()

        log_object = App_logger()
        file_object = open("Model_Log/Data_Preprocessing.txt", "a+")
        col = ["on_thyroxine", "sick",	"thyroid_surgery", "I131_treatment", "query_hyperthyroid",	"lithium", "goitre", "tumor", "hypopituitary"]

        obj = Preprocessor(file_object, log_object)
        data = pd.read_csv("Training_FileFromDB/InputFile.csv")
        obj.remove_columns(data, col)
        obj.separate_label_feature(data, "Class")
        obj.drop_unnecessary_columns(data, ['age', 'sex'])
        obj.replaceInvalidValuesWithNull(data)

        print("DONE")
