import sys
from dataSources.deleteRowsAppleGoogle import deleteRowsAppleGoogle
from dataSources.dataIngestionApple import dataIngestionApple
from dataSources.dataIngestionGoogle import dataIngestionGoogle

def main_functionality():
    print("called from actions1.yml")
    # deleteRowsAppleGoogle()
    # dataIngestionApple(noOfSlices = 10, subDf = 1)
    # dataIngestionGoogle(noOfSlices = 0)

def additional_functionality():
    print("called from actions2.yml")
    # dataIngestionApple(noOfSlices = 10, subDf = 2)
    # dataIngestionGoogle(noOfSlices = 0)

# Check which YAML file is calling the script
# if sys.argv[0] == 'actions1.yml':
#     # Execute main functionality for action1.yml
#     main_functionality()
# elif sys.argv[0] == 'actions2.yml':
#     # Execute additional functionality for action2.yml
#     additional_functionality()
# else:
#     print("Invalid YAML file specified.")
    
print(sys.argv[0])
print(sys.argv)
print(sys)