import pandas as pd
import numpy as np
import pickle
from sklearn.preprocessing import LabelEncoder
from sklearn.impute import KNNImputer
from imblearn.over_sampling import RandomOverSampler

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
            self.X = data.drop(labels = label_column_name, axis = 1)
            self.Y = data[label_column_name]
            self.logger_object.log(self.file_object, 'data and label separation done')
            self.logger_object.log(self.file_object, 'Exited the separate_label_feature method of the Preprocessor class\n')
            return self.X, self.Y
        except Exception as e:
            self.logger_object.log(self.file_object,'*** Exception occurred in separate_label_feature method of the Preprocessor class. Exception message:  '+str(e))
            self.logger_object.log(self.file_object,'Label Separation Unsuccessful. Exited the separate_label_feature method of the Preprocessor class')
            raise Exception()

    def drop_unnecessary_columns(self, data, columnNameList):
        self.logger_object.log(self.file_object, 'Entered the drop_unnecessary_columns method of the Preprocessor class')
        try:
            data = data.drop(columnNameList, axis=1)
            self.logger_object.log(self.file_object, 'Unnecessary columns deleted')
            self.logger_object.log(self.file_object, 'Exited the drop_unnecessary_columns method of the Preprocessor class\n')
            return data
        except Exception as e:
            self.logger_object.log(self.file_object, '*** Exception occurred in drop_unnecessary_columns method of the Preprocessor class. Exception:  '+str(e))
            self.logger_object.log(self.file_object, 'Exited the drop_unnecessary_columns method of the Preprocessor class')
            raise Exception()

    def replace_invali_value_with_null(self, data):
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
            raise Exception()

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
                self.logger_object.log(self.file_object, 'NULL value check complete')
                self.logger_object.log(self.file_object, 'Exited the is_null_present method of the Preprocessor class\n')
                return self.null_present
        except Exception as e:
            self.logger_object.log(self.file_object,'*** Exception occurred in is_null_present method of the Preprocessor class. Exception:  ' + str(e))
            self.logger_object.log(self.file_object,'Exited the is_null_present method of the Preprocessor class')
            raise Exception()

    def encode_categorical_values(self, data):
        self.logger_object.log(self.file_object, 'Entered the encode_categorical_values method of the Preprocessor class')
        try:
            data['sex'] = data['sex'].map({'F':0, 'M':1})
            for column in data.columns:
                if len(data[column].unique()) == 2:
                    data[column] = data[column].map({'f':0, 't':1})
            data = pd.get_dummies(data, columns=['referral_source'])
            encode = LabelEncoder().fit(data['Class'])
            data['Class'] = encode.transform(data['Class'])

            with open('EncoderPickle/enc.pickle', 'wb') as file:
                pickle.dump(encode, file)
            self.logger_object.log(self.file_object, 'Categorical feature encoding complete')
            self.logger_object.log(self.file_object, 'Exited the encode_categorical_values method of the Preprocessor class\n')
            return data
        except Exception as e:
            self.logger_object.log(self.file_object,'*** Exception occurred in encode_categorical_values method of the Preprocessor class. Exception:  ' + str(e))
            self.logger_object.log(self.file_object, 'Exited the encode_categorical_values method of the Preprocessor class')
            raise Exception()

    def encode_categorical_values_prediction(self, data):
        self.logger_object.log(self.file_object, 'Entered the encode_categorical_values_prediction method of the Preprocessor class')
        try:
            data['sex'] = data['sex'].map({'F':0, 'M':1})
            cat_data = data.drop(['age','T3','TT4','T4U','FTI','sex'],axis=1)
            for column in cat_data.column:
                if data[column].nunique() == 1:
                    if data[column].unique()[0] == 'f' or data[column].unique()[0] == 'F':
                        data[column] = data[column].map({data[column].unique()[0] : 0})
                    else:
                        data[column] = data[column].map({data[column].unique()[0]: 1})
                elif data[column].unique() == 2:
                    data[column] = data[column].map({'f':0, 't':1})
            data = pd.get_dummies(data, columns=['referral_source'])
            self.logger_object.log(self.file_object, 'Categorical feature encoding complete')
            self.logger_object.log(self.file_object,'Exited the encode_categorical_values_prediction method of the Preprocessor class\n')
            return data
        except Exception as e:
            self.logger_object.log(self.file_object,'*** Exception occurred in encode_categorical_values_prediction method of the Preprocessor class. Exception:  ' + str(e))
            self.logger_object.log(self.file_object,'Exited the encode_categorical_values_prediction method of the Preprocessor class')
            raise Exception()
    def handle_imbalance_dataset(self, X, Y):
        self.logger_object.log(self.file_object, 'Entered the handle_imbalance_dataset method of the Preprocessor class')
        try:
            rdsample = RandomOverSampler()
            X_tx, Y_tx = rdsample._fit_resample(X, Y)
            self.logger_object.log(self.file_object, 'Imbalance dataset handle complete')
            self.logger_object.log(self.file_object, 'Exited the handle_imbalance_dataset method of the Preprocessor class\n')
            return X_tx, Y_tx
        except Exception as e:
            self.logger_object.log(self.file_object, '*** Exception occurred in handle_imbalance_dataset method of the Preprocessor class. Exception:  ' + str(e))
            self.logger_object.log(self.file_object, 'Exited the handle_imbalance_dataset method of the Preprocessor class')
            raise Exception()
