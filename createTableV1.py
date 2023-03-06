import mysql.connector
import string
import random
import math
import datetime

class MysqlTableCreation:
    
    def __init__(self):
        
        self.mycursor = None
        self.mydb = mysql.connector.connect(
            host="10.0.3.90",
            user="root",
            password="dtC5&CFiQ$9j",
            database="training"
        )
        self.isConnected = self.checkConnection()
        self.tablesList = self.getExistingTables()
        self.amtOfRecords = ""
        self.tableNameToCreate = ""
        self.recordCount = 0
        self.batchAmount = 0
        self.dataTypes = []
        self.acceptedDataTypes = ["int","varchar", "date", "bool"]

    def checkConnection(self):
        if self.mydb.is_connected():
            self.mycursor = self.mydb.cursor()
            return True
        else:
            print("Error connecting to database\n")
            return False

    def getExistingTables(self):
        existingTables = []
        self.mycursor.execute("SHOW TABLES")
        for tables in self.mycursor:
            existingTables.append(tables[0])
        return existingTables
    
    def generateVARCHAR(self):
        val = ""
        letterCnt = 0
        while letterCnt < 10:
            val = val + random.choice(string.ascii_letters)
            letterCnt = letterCnt + 1
        return val

    def generateDate(self):
        return datetime.date(random.randint(1000, 9999), random.randint(1, 12), random.randint(1, 28))

    def generateRecord(self): 
        retvals = []
        retvals.append(self.recordCount)
        for point in self.dataTypes:
            if point[1] == "int":
                retvals.append(random.randint(1, 100))
            if point[1] == "varchar":
                retvals.append(self.generateVARCHAR())
            if point[1] == "bool":
                options = [True, False]
                pick = random.randint(0, 1)
                retvals.append(options[pick])
            if point[1] == "date":
                retvals.append(self.generateDate())

        self.recordCount = self.recordCount + 1

        return retvals

    def generateRemainingRecords(self, remaining):
        x = 0
        valsToExecute = []
        while x < remaining:
            valsToExecute.append(self.generateRecord())
            x = x + 1
        self.uploadBatch(valsToExecute)

    def generateUploadString(self):
        baseStr = "INSERT INTO " + self.tableNameToCreate + " (id,"
        for name in self.dataTypes:
            baseStr = baseStr + str(name[0]) + ","
        baseStr = baseStr[:-1] + ") VAlUES ("
        for x in range(0,len(self.dataTypes)):
            baseStr = baseStr + "%s,"
        baseStr = baseStr + "%s,"
        baseStr = baseStr[:-1] + ")"
        print(baseStr)
        return baseStr

    def uploadBatch(self, valsToExecute):
        insertStr = self.generateUploadString()
        sql = insertStr
        self.mycursor.executemany(sql, valsToExecute)
        self.mydb.commit()
        print(str(self.recordCount) + " Records uploaded")

    def generateUploadBatch(self):
        valsToExecute = []
        z = 0
        while z < self.batchAmount:
            valsToExecute.append(self.generateRecord())
            z = z + 1
        self.uploadBatch(valsToExecute)
    
    def generateCreateTableString(self):
        baseStr = "CREATE TABLE " + self.tableNameToCreate + " (id INT PRIMARY KEY,"
        for point in self.dataTypes:
            point1 = point[1]
            if point1 == "varchar":
                point1 = "VARCHAR(255)"
            baseStr = baseStr + str(point[0]) + " " + str(point1).upper() + "," 
        baseStr = baseStr[:-1] + ")"
        return baseStr

    def generateData(self):
        createTableStr = self.generateCreateTableString()
        print(createTableStr)
        self.mycursor.execute(createTableStr)
        numberOfBatchesToRun = math.floor(int(self.amtOfRecords)/int(self.batchAmount))
        remaining = int(self.amtOfRecords) - (int(numberOfBatchesToRun)*int(self.batchAmount))
        x = 0
        while x < numberOfBatchesToRun:
            self.generateUploadBatch()
            x = x + 1
        self.generateRemainingRecords(remaining)
      
    def checkIfTableExists(self, tablename):
        for tables in self.tablesList:
            if tablename == tables:
                return False
        self.tableNameToCreate = tablename
        return True

    def getRecordNumber(self):
        amtOfRecords = input("\nHow many records do you want to generate? Minimum: 2\n")
        amtOfRecords.strip("\r\n")
        if amtOfRecords.isdigit() and int(amtOfRecords) > 1:
            self.amtOfRecords = amtOfRecords
        else:
            print("\nNot a valid record number, Please try again\n")
            amtOfRecords = ""
            self.getRecordNumber()

    def setBatchAmount(self):
        
        amt = int(self.amtOfRecords)
        batchSize = 0
        if amt <= 100:
            self.batchAmount = 10
        else:
            batchSize = round(amt/10)
            if batchSize > 15000:
                self.batchAmount = 15000
            else:
                self.batchAmount = batchSize
        print("batch amount: ", self.batchAmount)
        
        # if amt <= 100:
        #     self.batchAmount = 10
        # elif amt > 100 and amt <= 1000:
        #     self.batchAmount = 100
        # elif amt > 1000 and amt <= 10000:
        #     self.batchAmount = 1000
        # elif amt > 10000 and amt <= 100000:
        #     self.batchAmount = 2500
        # elif amt > 100000 and amt <= 1000000:
        #     self.batchAmount = 5000
        # else:
        #     self.batchAmount = 15000
        
    def getTableName(self):
        tablename = input("Enter new table name to be created\n")
        if self.checkIfTableExists(tablename):
            self.getRecordNumber()
            self.setBatchAmount()
        else:
            print("\n !!! Table already exists in database !!!\n")
            self.getTableName()

    def checkNotEmpty(self, str):
        return str != ""
    
    def getDataType(self):
        dataType = "" 
        x = 0
        while x < 1:
            print("Please enter a data type from the list")
            dataType = input("\nWhat variable type is this? (int, varchar, date, or bool)\n")
            if dataType.lower() in self.acceptedDataTypes:
                break
            else:
                for dt in self.acceptedDataTypes:
                    if dataType.lower() in dt:
                        print("Did you mean " + str(dt) + "? (y/n)\n")
                        ans = input()
                        if ans == "y":
                            dataType = dt
                            x = 2
        return dataType

    def getDataTypeName(self):
        print("\nWarning: There will be a unique ID attached to this dataset with the column name: 'id'")
        x = 0
        while x < 15:
            tempArr = []
            name = input("\nEnter variable name: (Type 'Done' to be finished.)\n")
            if self.checkNotEmpty(name):
                if name.lower() == "done":
                    break
                else:
                    dataType = self.getDataType()
                    tempArr.append(name)
                    tempArr.append(dataType)
                    self.dataTypes.append(tempArr)
                    x = x + 1
            else:
                print("please enter valid name\n")
                name = ""



create = MysqlTableCreation()
if create.checkConnection():
    create.getTableName()
    create.getDataTypeName()
    if len(create.dataTypes) < 1:
        print("No Data Entered. Exiting Program.")
    else:
        print("\nAre you sure you want to generate " + str(create.amtOfRecords) + " records with these data types? (y/n)")
        for x in create.dataTypes:
            print(x)
        confirm = input()
        if confirm == "y":
            print("\ngenerating data...")
            create.generateData()






