#!/usr/bin/env python3

import psycopg2
import datetime

#####################################################
##  Database Connection
#####################################################

'''
Connect to the database using the connection string
'''
def openConnection():
    # connection parameters - ENTER YOUR LOGIN AND PASSWORD HERE
    userid = "y22s2c9120_xzho6094"
    passwd = "422514"
    myHost = "soit-db-pro-2.ucc.usyd.edu.au"

    # Create a connection to the database
    conn = None
    try:
        # Parses the config file and connects using the connect string
        conn = psycopg2.connect(database=userid,
                                    user=userid,
                                    password=passwd,
                                    host=myHost)
    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)
    
    # return the connection to use
    return conn

'''
Validate administrator based on login and password
'''
def checkAdmCredentials(login, password):

    try:
        conn = openConnection()
        mycur1 = conn.cursor()
        mycur1.execute(f"select* from Administrator where login = '{login}' and password = '{password}'")
        myadmin1 = mycur1.fetchall()
        if len(myadmin1) == 0:
            return None
        else:
            return myadmin1[0]

    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)

    finally:
        conn.close()

'''
List all the associated instructions in the database by administrator
'''
def findInstructionsByAdm(login):

    try:
        conn = openConnection()
        mycur2 = conn.cursor()

        sqlquery = f'''
        SELECT InstructionId, Amount, FrequencyDesc, TO_CHAR(ExpiryDate :: DATE, 'dd-mm-yyyy'), FirstName, LastName, Name, Notes
        FROM InvestInstruction i, Customer c, ETF e, Frequency f   
        WHERE i.Customer = c.Login and i.Frequency = f.FrequencyCode and i.code = e.code and i.Administrator = '{login}'
        ORDER BY ExpiryDate ASC, (FirstName, LastName) DESC
        '''

        mycur2.execute(sqlquery)
        result = mycur2.fetchall()

        if len(result) == 0:
            return [None]
        else:
            return [
                {
                    'instruction_id': str(row[0]),
                    'amount': str(row[1]),
                    'frequency': str(row[2]),
                    'expirydate': str(row[3]),
                    'customer': str(row[4] + " " + row[5]),
                    'etf': str(row[6]),
                    'notes': str(row[7] if row[7] != None else ""),
                }
                    for row in result
            ]

    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)

    finally:
        conn.close()

'''
Find a list of instructions based on the searchString provided as parameter
See assignment description for search specification
'''
def findInstructionsByCriteria(searchString):

    try:
        conn = openConnection()
        mycur3 = conn.cursor()
        mystr1 = searchString.lower()
        sqlquery2 = f'''
        SELECT InstructionId, Amount, FrequencyDesc, TO_CHAR(ExpiryDate :: DATE, 'dd-mm-yyyy'), FirstName, LastName, Name, Notes, Administrator
        FROM InvestInstruction i, Customer c, ETF e, Frequency f   
        WHERE i.Customer = c.Login and i.Frequency = f.FrequencyCode and i.code = e.code and 
	        (LOWER(CONCAT(FirstName,' ',LastName)) LIKE '%%{mystr1}%%' OR
	        LOWER(Name) LIKE '%%{mystr1}%%' OR
	        LOWER(Notes) LIKE '%%{mystr1}%%'
	        )
        ORDER BY ExpiryDate ASC
        '''

        mycur3.execute(sqlquery2)
        result = mycur3.fetchall()
        mydate = datetime.datetime.now()

        if len(result) == 0:
            return []
        else:
            newresult = []

            for row in result:

                extime = datetime.datetime.strptime(str(row[3]),'%d-%m-%Y')

                if row[8] == None and extime > mydate:
                    newresult.append(
                        {
                            'instruction_id': str(row[0]),
                            'amount': str(row[1]),
                            'frequency': str(row[2]),
                            'expirydate': str(row[3]),
                            'customer': str(row[4] + " " + row[5]),
                            'etf': str(row[6]),
                            'notes': str(row[7] if row[7] != None else ""),
                        }
                    )

            for row in result:
                extime = datetime.datetime.strptime(str(row[3]),'%d-%m-%Y')

                if row[8] != None and extime > mydate:
                    newresult.append(
                        {
                            'instruction_id': str(row[0]),
                            'amount': str(row[1]),
                            'frequency': str(row[2]),
                            'expirydate': str(row[3]),
                            'customer': str(row[4] + " " + row[5]),
                            'etf': str(row[6]),
                            'notes': str(row[7] if row[7] != None else ""),
                        }
                    )

            return newresult

    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)

    finally:
        conn.close()

'''
Add a new instruction
'''
def addInstruction(amount, frequency, customer, administrator, etf, notes):

    conn = openConnection()
    mycur4 = conn.cursor()
    mydate = datetime.datetime.now()
    defaultex = "%d/%d/%d"(mydate.day, mydate.month, mydate.year + 1)


'''
Update an existing instruction
'''
def updateInstruction(instructionid, amount, frequency, expirydate, customer, administrator, etf, notes):

    return
