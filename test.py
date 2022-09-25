from data_ingestion.data_loader import data_getter
from application_logging.logger import App_logger
from Training_Log.clear_log import truncate_file

if __name__ == '__main__':
    cl = truncate_file()
    cl.truncate_content()
    
    log_writer = App_logger()
    log_file = open("Model_Log/Model_Training_Log.txt", "a+")

    obj = data_getter(log_file, log_writer)
    data = obj.get_data()