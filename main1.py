from dataSources.deleteRowsAppleGoogle import deleteRowsAppleGoogle
from dataSources.dataIngestionApple import dataIngestionApple
from dataSources.dataIngestionGoogle import dataIngestionGoogle

deleteRowsAppleGoogle()
dataIngestionApple(noOfSlices = 10, subDf = 1)
dataIngestionGoogle(noOfSlices = 0)