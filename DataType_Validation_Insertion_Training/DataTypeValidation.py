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

