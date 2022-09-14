import json
from os import listdir
import os
import csv
import mysql.connector as conn
import pandas as pd
from application_logging.logger import App_logger

class DBOperation:
    def __init__(self):
        self.path = "Training_Database/"
        self.badFilePath = "Training_Raw_Files_Validated/Bad_Raw/"
        self.goodFilePath = "Training_Raw_Files_Validated/Good_Raw/"
        self.logger = App_logger()

    def createDatabaseConnection(self, dbname):
        """
        Method Name: dataBaseConnection
        Description: This method creates the database with the given name and if Database already exists then opens the connection to the DB.
        Output: Connection to the DB
        On Failure: Raise ConnectionError

        Written By: Bhagwat Chate
        Version: 1.0
        Revisions: None
        """
        try:
            log_file = open("Training_Log/DBOperation_Log.txt", "a")
            self.logger.log(log_file, "Entered in to the method 'createDatabaseConnection' of class 'DBOperation'.")
            db_connection_log = open("Training_Log/DB_Connection_Log.txt", "a")
            self.logger.log(db_connection_log, "Connecting with MySQL server")

            mydb = conn.connect(host='localhost', user='bhagwat', passwd='1234')
            self.logger.log(db_connection_log, "Connected with MySQL server")

            self.logger.log(db_connection_log, "Check database '{v}' existence".format(v=dbname))
            cursor = mydb.cursor()
            query = "SHOW DATABASES LIKE '{value}'".format(value=dbname)
            cursor.execute(query)

            if len(cursor.fetchall()) == 1:
                self.logger.log(db_connection_log, "Database '{v}' exist".format(v=dbname))
            else:
                self.logger.log(db_connection_log, "Database '{v}' don't exist, need to create".format(v=dbname))
                query = "CREATE DATABASE {v}".format(v=dbname)
                cursor.execute(query)
                self.logger.log(db_connection_log, "Database '{v}' created successfully.".format(v=dbname))

            self.logger.log(db_connection_log,"Database "+dbname+" connection created successfully")
            self.logger.log(log_file, "Exited from the method 'createDatabaseConnection' of class 'DBOperation'." + '\n')
            log_file.close()
            db_connection_log.close()
            return conn.connect(host='localhost', user='bhagwat', passwd='1234', database=dbname)
        except Exception as e:
            message = "*** Exception occurred in the method 'createDatabaseConnection' of class 'DBOperation'. \n {v}".format(v=e)
            log_file = open("Training_Log/DBOperation_Log.txt", "a")
            self.logger.log(log_file, message)
            log_file.close()
            db_connection_log = open("Training_Log/DB_Connection_Log.txt", "a")
            self.logger.log(db_connection_log, message)
            db_connection_log.close()

    def createTable(self, dbname, tablename):
        self.flage = 0
        try:
            log_file = open("Training_Log/DBOperation_Log.txt", "a")
            db_connection_log = open("Training_Log/DB_Connection_Log.txt", "a")
            db_table_log = open("Training_Log/DB_create_table_Log.txt", "a")
            self.logger.log(log_file, "Entered in to the method 'createTable' of class 'DBOperation'.")

            connection = self.createDatabaseConnection(dbname)
            self.logger.log(db_table_log, "connected with database '{v}'".format(v=dbname))

            cursor = connection.cursor()
            query = "SHOW TABLES IN {v}".format(v=dbname)
            cursor.execute(query)

            table_list = []
            for i in cursor.fetchall():
                for j in i:
                    table_list.append(j)

            if tablename in table_list:
                self.logger.log(db_table_log, "Table '{v1}' already exist in database '{v2}'".format(v1=tablename, v2=dbname))
            else:
                self.logger.log(db_table_log, "Table '{v1}' doesn't exist in database '{v2}'".format(v1=tablename, v2=dbname))
                with open("schema_training.json", "r") as f:
                    dic = json.load(f)
                    f.close()
                cursor = connection.cursor()
                self.flage = 0
                for key in dic["ColName"].keys():
                    dtype = dic["ColName"][key]
                    if self.flage == 0:
                        self.flage = 1
                        query = "CREATE TABLE " + dbname + "." + tablename + "(" + key + " " + dtype + ")"
                        cursor.execute(query)
                    else:
                        query = "ALTER TABLE "+tablename+" ADD "+str(key)+" "+str(dtype)
                        cursor.execute(query)
            if self.flage == 1:
                self.logger.log(db_table_log, "table '{v1}' in database '{v2}' created successfully".format(v1=tablename, v2=dbname))

        except Exception as e:
            message = "*** Exception occurred in the method 'createTable' of class 'DBOperation'.:  "+str(e)
            log_file = open("Training_Log/DBOperation_Log.txt", "a")
            db_table_log = open("Training_Log/DB_create_table_Log.txt", "a")
            self.logger.log(log_file, message)
            self.logger.log(db_table_log, message)
            db_table_log.close()
            log_file.close()
        finally:
            connection.close()
            log_file = open("Training_Log/DBOperation_Log.txt", "a")
            db_connection_log = open("Training_Log/DB_Connection_Log.txt", "a")
            db_table_log = open("Training_Log/DB_create_table_Log.txt", "a")
            self.logger.log(db_connection_log, "database '{v}' connection closed".format(v=dbname))
            self.logger.log(db_table_log, "database '{v}' connection closed".format(v=dbname))
            self.logger.log(log_file, "table '{v1}' database '{v2}' connection closed".format(v1=tablename, v2=dbname))
            self.logger.log(log_file, "Exited from the method 'createTable' of class 'DBOperation'." + '\n')
            db_connection_log.close()
            db_table_log.close()
            log_file.close()

    def missingValueImpute(self, filepath, file):
        try:
            log_file = open("Training_Log/DB_Imputation_Log.txt", "a")
            df = pd.read_csv(filepath+"/"+file)
            df = df.replace("?", 0)
            df.to_csv(filepath+"/"+file, index=False)
            self.logger.log(log_file, file+" : imputation completed.")
            log_file.close()
        except Exception as e:
            log_file = open("Training_Log/DB_Imputation_Log.txt", "a")
            self.logger.log(log_file, "Exception in training raw data file imputation: "+str(e))
            log_file.close()

    def insertIntoTable(self, dbname, tablename):
        try:
            log_file = open("Training_Log/DBOperation_Log.txt", "a")
            db_insert_into_table_log = open("Training_Log/DB_insert_into_table_log.txt", "a")
            self.logger.log(log_file,"Entered into method 'insertIntoTable' of class 'DBOperation'")
            connection = self.createDatabaseConnection(dbname)
            cursor = connection.cursor()
            self.logger.log(db_insert_into_table_log, "connected with database '{v}'".format(v=dbname))
            self.logger.log(log_file, "connected with database '{v}'".format(v=dbname))

            onlyfiles = [f for f in listdir(self.goodFilePath)]
            for file in onlyfiles:
                self.missingValueImpute(self.goodFilePath, file)
                try:
                    with open(self.goodFilePath+'/'+file, "r") as f:
                        next(f)
                        reader = csv.reader(f, delimiter="\n")
                        for line in enumerate(reader):
                            for list_ in (line[1]):
                                try:
                                    print(file+'\n'+(list_))
                                    cursor.execute("INSERT INTO {v1}.{v2} VALUES({v3})".format(v1=dbname, v2=tablename, v3=(list_)))
                                    self.logger.log(db_insert_into_table_log,"file: {v1} in table: {v2} load successful.".format(v1=file, v2=tablename))
                                except Exception as e:
                                    raise e
                        connection.commit()
                except Exception as e:
                    raise e
        except Exception as e:
            db_insert_into_table_log = open("Training_Log/DB_insert_into_table_log.txt", "a")
            self.logger.log(db_insert_into_table_log,"Exception while loading data into table: Thyroid database: '{v1}' error: {v2}".format(v1=dbname, v2=str(e)))
            db_insert_into_table_log.close()
        finally:
            connection.close()
            db_log = open("Training_Log/DB_Connection_Log.txt", 'a')
            log_file = open("Training_Log/DBOperation_Log.txt", "a")
            db_insert_into_table_log = open("Training_Log/DB_insert_into_table_log.txt", "a")
            self.logger.log(db_log, "database: '" + dbname + "' connection closed successfully")
            self.logger.log(log_file, "database: '" + dbname + "' connection closed successfully")
            self.logger.log(log_file, "Exited from the method 'insertIntoTable' of class 'DBOperation'")
            self.logger.log(db_insert_into_table_log, "data loaded in table: '"+tablename+"' successful")
            db_log.close()
            log_file.close()
            db_insert_into_table_log.close()
