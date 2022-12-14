import pandas as pd
import numpy as np
import pickle
from sklearn.preprocessing import LabelEncoder
from sklearn.impute import KNNImputer
from sklearn.utils import resample

class Preprocessor:
    """
        This class shall  be used to clean and transform the data before training.

        Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None
    """

    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object

    def remove_columns(self, data, columns):
        self.logger_object.log(self.file_object, "Entered into the remove_columns method of the class Preprocessing")
        self.data = data
        self.columns = columns

        try:
            self.useful_data = self.data.drop(labels=self.columns, axis = 1)
            self.logger_object.log(self.file_object, 'Column removal successful')
            self.logger_object.log(self.file_object, 'Exited the remove_columns method of the Preprocessor class\n')

            return self.useful_data
        except Exception as e:
            self.logger_object.log(self.file_object, '*** Exception occurred in remove_columns method of the Preprocessor class. Exception message:  '+str(e))
            self.logger_object.log(self.file_object, 'Column removal Unsuccessful. Exited the remove_columns method of the Preprocessor class')

    def separate_label_feature(self, data, label_column_name):
        self.logger_object.log(self.file_object, 'Entered the separate_label_feature method of the Preprocessor class')
        try:
            self.X = data.drop(labels=label_column_name, axis=1)
            self.Y = data[label_column_name]
            self.logger_object.log(self.file_object, 'data and label separation complete')
            self.logger_object.log(self.file_object, 'Exited the separate_label_feature method of the Preprocessor class\n')
            return self.X, self.Y
        except Exception as e:
            self.logger_object.log(self.file_object,'*** Exception occurred in separate_label_feature method of the Preprocessor class. Exception message:  '+str(e))
            self.logger_object.log(self.file_object,'Label Separation Unsuccessful. Exited the separate_label_feature method of the Preprocessor class')

    def drop_unnecessary_columns(self, data, columnNameList):
        self.logger_object.log(self.file_object, 'Entered the drop_unnecessary_columns method of the Preprocessor class')
        try:
            if len(columnNameList) != 0:
                data = data.drop(columnNameList, axis=1)
                self.logger_object.log(self.file_object, 'Unnecessary columns deleted')
                self.logger_object.log(self.file_object, 'Exited the drop_unnecessary_columns method of the Preprocessor class\n')

            else:
                self.logger_object.log(self.file_object, 'Do not have columns for delete')
                self.logger_object.log(self.file_object, 'Exited the drop_unnecessary_columns method of the Preprocessor class\n')
            return data
        except Exception as e:
            self.logger_object.log(self.file_object, '*** Exception occurred in drop_unnecessary_columns method of the Preprocessor class. Exception:  '+str(e))
            self.logger_object.log(self.file_object, 'Exited the drop_unnecessary_columns method of the Preprocessor class')

    def replace_invalid_value_with_null(self, data):
        self.logger_object.log(self.file_object, 'Entered the replace_invali_value_with_null method of the Preprocessor class')
        try:
            for column in data.columns:
                count = data[column][data[column]=='?'].count()
                if count != 0:
                    data[column] = data[column].replace('?', np.nan)
            self.logger_object.log(self.file_object, 'Invalid values replaced with np.nan')
            self.logger_object.log(self.file_object, 'Exited the replace_invali_value_with_null method of the Preprocessor class\n')
            return data
        except Exception as e:
            self.logger_object.log(self.file_object, '*** Exception occurred in replace_invali_value_with_null method of the Preprocessor class. Exception:  '+str(e))
            self.logger_object.log(self.file_object, 'Exited the replace_invali_value_with_null method of the Preprocessor class')

    def is_null_present(self, data):
        self.logger_object.log(self.file_object, 'Entered the is_null_present method of the Preprocessor class')
        self.null_present = False
        try:
            for i in data.isna().sum():
                if i > 0:
                    self.null_present = True
                    break
            if self.null_present:
                df_with_null = pd.DataFrame()
                df_with_null['columns'] = data.columns
                df_with_null['missing_values_count'] = np.asarray(data.isna().sum())
                df_with_null.to_csv("data_preprocessing/null_values.csv", index=False)
                self.logger_object.log(self.file_object, 'NULL value present, NULL value check complete')
                self.logger_object.log(self.file_object, 'Exited the is_null_present method of the Preprocessor class\n')
            else:
                self.logger_object.log(self.file_object, 'No NULL value, NULL value check complete')
                self.logger_object.log(self.file_object, 'Exited the is_null_present method of the Preprocessor class\n')
            return self.null_present
        except Exception as e:
            self.logger_object.log(self.file_object,'*** Exception occurred in is_null_present method of the Preprocessor class. Exception:  ' + str(e))
            self.logger_object.log(self.file_object,'Exited the is_null_present method of the Preprocessor class')

    def encode_categorical_values(self, data):
        self.logger_object.log(self.file_object, 'Entered the encode_categorical_values method of the Preprocessor class')
        self.data = data
        try:
            self.data['sex'] = self.data['sex'].map({'F': 0, 'M': 1})
            for column in self.data.columns:
                if len(self.data[column].unique()) == 2:
                    self.data[column] = self.data[column].map({'f': 0, 't': 1})
            self.data = pd.get_dummies(self.data, columns=['referral_source'])
            encode = LabelEncoder().fit(self.data['Class'])
            self.data['Class'] = encode.transform(self.data['Class'])
            # self.data['TBG_measured'] = self.data['TBG_measured'].map({'f': 0, 't': 1})

            with open('EncoderPickle/enc.pickle', 'wb') as file:
                pickle.dump(encode, file)
            self.logger_object.log(self.file_object, 'Categorical feature encoding complete')
            self.logger_object.log(self.file_object, 'Exited the encode_categorical_values method of the Preprocessor class\n')
            self.data = self.data.reset_index().drop('index', axis=1)
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,'*** Exception occurred in encode_categorical_values method of the Preprocessor class. Exception:  ' + str(e))
            self.logger_object.log(self.file_object, 'Exited the encode_categorical_values method of the Preprocessor class')
            raise Exception()

    def encode_categorical_values_prediction(self, data):
        self.logger_object.log(self.file_object, 'Entered the encode_categorical_values_prediction method of the Preprocessor class')
        self.data = data
        try:
            self.data['sex'] = self.data['sex'].map({'F':0, 'M':1})
            cat_data = self.data.drop(['age','T3','TT4','T4U','FTI','sex'],axis=1)
            for column in cat_data.column:
                if self.data[column].nunique() == 1:
                    if self.data[column].unique()[0] == 'f' or self.data[column].unique()[0] == 'F':
                        self.data[column] = self.data[column].map({self.data[column].unique()[0]: 0})
                    else:
                        self.data[column] = self.data[column].map({self.data[column].unique()[0]: 1})
                elif self.data[column].nunique() == 2:
                    self.data[column] = self.data[column].map({'f': 0, 't': 1})
            self.data = pd.get_dummies(self.data, columns=['referral_source'])
            self.logger_object.log(self.file_object, 'Categorical feature encoding complete')
            self.logger_object.log(self.file_object,'Exited the encode_categorical_values_prediction method of the Preprocessor class\n')
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,'*** Exception occurred in encode_categorical_values_prediction method of the Preprocessor class. Exception:  ' + str(e))
            self.logger_object.log(self.file_object,'Exited the encode_categorical_values_prediction method of the Preprocessor class')

    def handle_imbalance_dataset(self, data, imb_threshold_percentage):
        self.logger_object.log(self.file_object, 'Entered the handle_imbalance_dataset method of the Preprocessor class')
        self.data = data
        self.imb_threshold_percentage = imb_threshold_percentage
        try:
            data_0 = self.data[self.data['Class'] == 0]
            data_1 = self.data[self.data['Class'] == 1]
            data_2 = self.data[self.data['Class'] == 2]
            data_3 = self.data[self.data['Class'] == 3]
            class_record_len = [len(data_0), len(data_1), len(data_2), len(data_3)]

            data_0_per = len(data_0)/len(self.data)*100
            data_1_per = len(data_1)/len(self.data)*100
            data_2_per = len(data_2)/len(self.data)*100
            data_3_per = len(data_3)/len(self.data)*100
            class_record_per = [data_0_per, data_1_per, data_2_per, data_3_per]

            if data_0_per < max(class_record_per) and (data_0_per < self.imb_threshold_percentage):
                data_0_up = resample(data_0, replace=True, n_samples=max(class_record_len)-len(data_0), random_state=1234)

            # if data_1_per < max(class_record_per) and (data_1_per < self.imb_threshold_percentage): # this is the major class. here conditions fails & get error access before assign
            #     data_1_up = resample(data_1, replace=True, n_samples=max(class_record_len)-len(data_1), random_state=1234)

            if data_2_per < max(class_record_per) and (data_2_per < self.imb_threshold_percentage):
                data_2_up = resample(data_2, replace=True, n_samples=max(class_record_len)-len(data_2), random_state=1234)

            if data_3_per < max(class_record_per) and (data_3_per < self.imb_threshold_percentage):
                data_3_up = resample(data_3, replace=True, n_samples=max(class_record_len)-len(data_3), random_state=1234)

            self.data = pd.concat([data_0_up, data_2_up, data_3_up])
            self.logger_object.log(self.file_object, 'Exited the handle_imbalance_dataset method of the Preprocessor class\n')
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object, '*** Exception occurred in handle_imbalance_dataset method of the Preprocessor class. Exception:  ' + str(e))
            self.logger_object.log(self.file_object, 'Exited the handle_imbalance_dataset method of the Preprocessor class')

    def impute_missing_value(self, data):
        self.logger_object.log(self.file_object, "Entered into impute_missing_value method of Preprocessor class")
        self.data = data
        try:
            imputer = KNNImputer(n_neighbors=3, weights='uniform', missing_values=np.nan)
            self.new_array = imputer.fit_transform(self.data)
            self.new_data = pd.DataFrame(data=self.new_array, columns=self.data.columns)
            self.logger_object.log(self.file_object, 'Impute data complete')
            self.logger_object.log(self.file_object, 'Exited the impute_missing_value method of the Preprocessor class\n')
            return self.new_data
        except Exception as e:
            self.logger_object.log(self.file_object, '*** Exception occurred in impute_missing_value method of the Preprocessor class. Exception:  ' + str(e))
            self.logger_object.log(self.file_object, 'Exited the impute_missing_value method of the Preprocessor class')

    def get_columns_with_zero_std_deviation(self, data):
        self.logger_object.log(self.file_object, "Entered into get_columns_with_zero_std_deviation method of Preprocessor class")
        self.data = data
        self.columns = self.data.columns
        self.data_n = self.data.describe()
        self.col_to_drop = []
        try:
            for col in self.columns:
                if (self.data_n[col]['std'] == 0):
                    self.col_to_drop.append(col)
            self.logger_object.log(self.file_object, "search of columns with zero standard deviation complete")
            self.logger_object.log(self.file_object, "Exit from get_columns_with_zero_std_deviation method of Preprocessor class\n")

            return self.col_to_drop
        except Exception as e:
            self.logger_object.log(self.file_object,'*** Exception occurred in get_columns_with_zero_std_deviation method of the Preprocessor class. Exception:  ' + str(e))
            self.logger_object.log(self.file_object, 'Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
