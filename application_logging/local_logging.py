from datetime import datetime
import os


class local_logger:
    """
    This class shall be used for handling all the logging operations to the local storage.

    Written By: Prafful Agrawal
    Version: 1.0
    Revisions: None
    """
    def __init__(self):
        pass

    @staticmethod
    def log(file_path, log_message):
        """
        Method to write a log message to the given file.
        It also adds the current date and time to the log.

        Input Parameters:
            file_path - local path of the log file; str
            log_message - message to log; str
        Output:
            None
        On Failure:
            raise Exception
        """
        try:
            now = datetime.now()
            date = now.date()
            current_time = now.strftime('%H:%M:%S')
            message = str(date) + '/' + str(current_time) + '\t\t' + log_message + '\n'
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'a+') as f:
                f.write(message)
                f.close()
            return None
        except Exception as e:
            raise e
