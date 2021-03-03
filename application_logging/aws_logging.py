from datetime import datetime
from aws_operations.aws_storage import aws_storage_operations


class aws_logger:
    """
    This class shall be used for handling all the logging operations to the AWS storage.

    Written By: Prafful Agrawal
    Version: 1.0
    Revisions: None
    """
    def __init__(self, aws_operations=None):
        if aws_operations is not None:
            self.aws_operations = aws_operations
        else:
            self.aws_operations = aws_storage_operations()

    def log(self, file_path, log_message):
        """
        Method to write a log message to the given file.
        It also adds the current date and time to the log.

        Input Parameters:
            file_path - absolute path of the log file in the bucket; str
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
            self.aws_operations.append_file(file_path, message)
            return None
        except Exception as e:
            raise e
