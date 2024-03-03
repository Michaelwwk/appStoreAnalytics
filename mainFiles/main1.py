from ..dataSources.deleteRowsAppleGoogle import deleteRowsAppleGoogle
from ..dataSources.dataIngestionApple import dataIngestionApple
from ..dataSources.dataIngestionGoogle import dataIngestionGoogle

import os
# Get the absolute path of the current file (my_main_file.py)
current_file_path = os.path.abspath(__file__)
# Get the directory name of the current file (folder containing my_main_file.py)
project_root = os.path.dirname(current_file_path)
print(current_file_path)
print(project_root)

deleteRowsAppleGoogle()
dataIngestionApple(noOfSlices = 10, subDf = 1)
dataIngestionGoogle(noOfSlices = 0)