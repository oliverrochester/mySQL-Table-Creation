
import mysql.connector
import string
import random
import math
import datetime

class MySQLConnection:
    
    def __init__(self, hostname, username, password, database):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.database = database

    def establishConnection(self):

        mydb = mysql.connector.connect(
            host=self.hostname,
            user=self.username,
            password=self.password,
            database=self.database
        )

        if mydb.is_connected():
            return mydb
        else:
            return None
        
    def createTable(self, mycursor, tablename, schemaObject):
        baseStr = "CREATE TABLE " + tablename + " ("
        for point in schemaObject.schema:
            point1 = point[1]
            if point1 == "varchar":
                point1 = "VARCHAR(255)"
            baseStr = baseStr + str(point[0]) + " " + str(point1).upper() + "," 
        baseStr = baseStr[:-1] + ")"
        mycursor.execute(baseStr)
        
    def generateUploadString(self, tableDetails):
        baseStr = "INSERT INTO " + tableDetails.tablename + " ("
        for name in tableDetails.schema:
            baseStr = baseStr + str(name[0]) + ","
        baseStr = baseStr[:-1] + ") VAlUES ("
        for x in range(0,len(tableDetails.schema)):
            baseStr = baseStr + "%s,"
        baseStr = baseStr + "%s,"
        baseStr = baseStr[:-1] + ")"
        return baseStr
    
    def uploadBatch(self, valsToExecute, sqlInsert, mycursor, mydb, tabledetails):
        mycursor.executemany(sqlInsert, valsToExecute)
        mydb.commit()
        print(tabledetails.uploaded + " Records uploaded")

class RandomSchema:
    def __init__(self):
        
        self.acceptedDataTypes = ["int", "varchar", "date", "bool"]
        self.label = "random"
        self.schema = [["id", "int"]]

    def generateSchema(self):
        for i in range(0,7):
            tempArr = []
            dataTypeSelection = random.randint(0,3)
            tempArr.append("data" + str(i))
            tempArr.append(self.acceptedDataTypes[dataTypeSelection])
            self.schema.append(tempArr)

    def getSchema(self):
        return self.schema

class CustomSchema:
    def __init__(self):
        
        self.acceptedDataTypes = ["int", "varchar", "date", "bool"]
        self.label = "custom"
        self.schema = [["id", "int"]]

    def helper_getDataTypeName(self):
        name = input("\nEnter variable name: (Type 'Done' to be finished.)\n")
        if self.helper_checkNotEmpty(name):
            if name.lower() == "done":
                return "done"
            else:
                return name
        else:
            print("please enter valid name\n")
            self.helper_getDataTypeName()

    def helper_getDataType(self):
        dtReturn = ""
        print("Please enter a data type from the list")
        dataType = input("\nWhat variable type is this? (int, varchar, date, or bool)\n")
        if dataType.lower() in self.acceptedDataTypes:
            return dataType
        else:
            print("\nData type not currently supported\n")
            self.helper_getDataType()       
  
    def helper_checkNotEmpty(self, str):
        return str != ""

    def generateSchema(self):
        x = 0
        while x < 15:
            tempAr2 = []
            typeName = self.helper_getDataTypeName()
            if typeName == "done":
                break
            else:
                typeOf = self.helper_getDataType()
                tempAr2.append(typeName)
                tempAr2.append(typeOf)
                self.schema.append(tempAr2)
                x = x + 1

    def getSchema(self):
        return self.schema

class TableDetails:

    def __init__(self, mycursor):
        self.mycursor = mycursor
        self.tablename = ""
        self.amtOfRecords = 0
        self.batchAmount = 0
        self.numberOfBatchesToRun = 0
        self.remainingRecords = 0
        self.schemaType = ""
        self.schema = []
        self.uploaded = 0

    def getTableName(self):
        return self.tablename
    
    def getRecordAmount(self):
        return self.amtOfRecords

    #todo: check to see if record number entered is acutally a number, if not, prompt again
    def generateRecordAmount(self):
        recordAmt = input("\nHow many records would you like to create?  \n")
        if int(recordAmt) < 1:
            print("Can not enter a value less than 1!")
            self.generateRecordAmount()
        elif int(recordAmt) > 50000000:
            print("Please enter a number lower than 50,000,000")
            self.generateRecordAmount()
        else:
            self.amtOfRecords = int(recordAmt)
  
    def createTableName(self):
        tablename = input("\nEnter table name to be created: \n")
        if self.helper_CheckIfTableExists(tablename):
            print("\n !!! Table already exists in database !!!")
            self.createTableName()
        else:
            self.tablename = tablename

    def helper_CheckIfTableExists(self, tablename):
        self.mycursor.execute("SHOW TABLES")
        for table in self.mycursor:
            if tablename == table[0]:
                return True
        return False
    
    def setTableSchemaType(self):
        schematype = input("\nWould you like a random schema? Or would you like to customize your own? (Type 'Random' or 'Custom')\n")
        if schematype.lower() == "random":
            self.schemaType = "random"
        elif schematype.lower() == "custom":
            self.schemaType = "custom"
        else:
            print("\nPlease select Random or Custom")
            self.setTableSchemaType()

    def getSchemaType(self):
        return self.schemaType

    def setBatchAmount(self):
        batchSize = 0
        if self.amtOfRecords <= 100:
            self.batchAmount = 10
        else:
            batchSize = round(self.amtOfRecords/10)
            if batchSize > 15000:
                self.batchAmount = 15000
            else:
                self.batchAmount = batchSize

        self.numberOfBatchesToRun = math.floor(int(self.amtOfRecords)/int(self.batchAmount))
        self.remainingRecords = int(self.amtOfRecords) - (int(self.numberOfBatchesToRun)*int(self.batchAmount))

    def getBatchAmount(self):
        return self.batchAmount

    def getNumberOfBatchesToRun(self):
        return self.numberOfBatchesToRun
    
    def getRemainingRecords(self):
        return self.remainingRecords



def mainHelper_ConfirmDesiredSchema(schemaObject, recordAmount):
    schema = []
    while True:
        schema = schemaObject.getSchema()
        print("\nAre you sure you want to generate " + str(recordAmount) + " records with the following schema: (y/n, or 'r' to generte a new Schema)\n")
        for x in schema:
            print(x)
        ans = input()
        if ans == "y":
            return True
        if ans == "n":
            exit()
        if ans == "r":
            schemaObject.schema = [["id", "int"]]
            schemaObject.generateSchema()
        else:
            return False

def mainHelper_generateRecord(tableDetails):
    retvals = []
    retvals.append(tableDetails.uploaded)
    for point in tableDetails.schema:
        if point[1] == "int":
            retvals.append(random.randint(1, 100))
        if point[1] == "varchar":
            retvals.append("testing")
        if point[1] == "bool":
            retvals.append(random.randint(0, 1))
        if point[1] == "date":
            retvals.append(datetime.date(random.randint(1000, 9999), random.randint(1, 12), random.randint(1, 28)))
    tableDetails.uploaded = tableDetails.uploaded + 1
    return retvals

def mainHelper_GenerateUploadBatch(batchAmount, tableDetails, mydb, mycursor, dbConnection, sqlInsert):
    valsToExecute = []
    z = 0
    while z < batchAmount:
        valsToExecute.append(mainHelper_generateRecord(tableDetails))
        z = z + 1
    dbConnection.uploadBatch(valsToExecute, sqlInsert, mycursor, mydb, tableDetails)

def mainHelper_GenerateRemainingRecords(remaining, dbConnection, tableDetails):
    x = 0
    valsToExecute = []
    while x < remaining:
        valsToExecute.append(mainHelper_generateRecord(tableDetails))
        x = x + 1
    dbConnection.uploadBatch(valsToExecute)

def mainHelper_GenerateAndExportData(mydb, mycursor, dbConnection, tableDetails, sqlInsert):
    x = 0
    while x < tableDetails.numberOfBatchesToRun:
        mainHelper_GenerateUploadBatch(tableDetails.batchAmount, tableDetails.recordAmount, mydb, mycursor, dbConnection, sqlInsert)
        x = x + 1
    mainHelper_GenerateRemainingRecords(tableDetails.remainingRecords, dbConnection, tableDetails)


def main():
    dbConnection = MySQLConnection("xxxx", "xxxx", "xxxx", "xxxx")
    mydb = dbConnection.establishConnection()
    mycursor = mydb.cursor()

    tableDetails = TableDetails(mycursor)
    tableDetails.createTableName()
    tablename = tableDetails.getTableName()
    tableDetails.generateRecordAmount()
    recordAmount = tableDetails.getRecordAmount()
    tableDetails.setBatchAmount()
    batchAmount = tableDetails.getBatchAmount()
    tableDetails.setTableSchemaType()
    schemaType = tableDetails.getSchemaType()
    numberOfBatchesToRun = tableDetails.getNumberOfBatchesToRun()
    remaining = tableDetails.getRemainingRecords()

    schemaObject = None
    if schemaType == "custom":
        schemaObject = CustomSchema()
        schemaObject.generateSchema()
    if schemaType == "random":
        schemaObject = RandomSchema()
        schemaObject.generateSchema()

    if mainHelper_ConfirmDesiredSchema(schemaObject, recordAmount):
        dbConnection.createTable(mycursor, tablename, schemaObject)
        sqlInsert = dbConnection.generateUploadString(tableDetails)
        mainHelper_GenerateAndExportData(mydb, mycursor, dbConnection, tableDetails, sqlInsert)
    

    print()

main()