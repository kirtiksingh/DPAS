#!/usr/bin/python2.7
#
# Interface for the assignement
#

from sys import flags
import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cursor = openconnection.cursor()
    dropTable = "DROP TABLE IF EXISTS " + ratingstablename
    createTable = "CREATE TABLE " + ratingstablename + " (UserID INT, MovieID INT, Rating FLOAT);"

    cursor.execute(dropTable)
    cursor.execute(createTable)
    openconnection.commit() # Required as most Python interpreters do not autocommit

    with open(ratingsfilepath, "r") as file:
        for row in file:
            [userId, movieId, rating, timestamp] = row.split("::")

            loadData = "INSERT INTO {}(UserID, MovieID, Rating) VALUES({},{},{});".format(userId, movieId, rating)
            cursor.execute(loadData)
    openconnection.commit()
    cursor.close()

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor()

    max_rate = 5.0
    partitionRange = float(max_rate / numberofpartitions)

    for i in range(0, numberofpartitions):
        partitionName = 'range_part' + str(i)
        dropTable = "DROP TABLE IF EXISTS {};".format(partitionName)
        cursor.execute(dropTable)

        j = float(i)

        if i == 0:
            createPartition = "CREATE TABLE {} AS SELECT * FROM {} WHERE Rating >= {} AND Rating <= {} ;".format(partitionName, ratingstablename, str(j*partitionRange), str((j+1)*partitionRange))
        else:
            createPartition = "CREATE TABLE {} AS SELECT * FROM {} WHERE Rating > {} AND Rating <= {} ;".format(partitionName, ratingstablename, str(j*partitionRange), str((j+1)*partitionRange))

        cursor.execute(createPartition)
    cursor.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor()

    selectAllRows = "SELECT * FROM {}".format(ratingstablename)
    cursor.execute(selectAllRows)
    rows = cursor.fetchall()

    partition = 0

    while partition < numberofpartitions:
      rrTableName = "rrobin_part" + str(partition)
      createRrTable = "CREATE TABLE IF NOT EXISTS {} (UserID INT, MovieID INT, Rating FLOAT)".format(rrTableName)
      cursor.execute(createRrTable)
      partition += 1

    partition = 1
    for r in rows:
        rrTableName = "rrobin_part" + str(partition)
        insertRrData = "INSERT INTO {}(UserID, MovieID, Rating) VALUES ({}, {}, {})".format(rrTableName, r[0], r[1], r[2]) 
        cursor.execute(insertRrData)

        partition = (partition + 1) % numberofpartitions

        cursor.execute("CREATE TABLE IF NOT EXISTS partitions_rr(partition INT)")
        cursor.execute("INSERT INTO partitions_rr VALUES(%s)" % (numberofpartitions))

    openconnection.commit()
    cursor.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()

    # Can be used if we want to use the temporary table made above
    cursor.execute("SELECT * FROM partitions_rr") 
    noOfPartitions = cursor.fetchone()[0]

    selectAllRowsCount = "SELECT COUNT (*) FROM {};".format(ratingstablename)
    cursor.execute(selectAllRowsCount)
    totalRows = int(cursor.fetchone()[0])

    newRowID = (totalRows % noOfPartitions)

    insertNewRow = "INSERT INTO rrobin_part{} VALUES({}, {}, {})".format(newRowID, userid, itemid, rating)
    cursor.execute(insertNewRow)

    openconnection.commit()
    cursor.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()

    tableInsert = "INSERT INTO {} VALUES ({},{},{})".format(ratingstablename, userid, itemid, rating)
    cursor.execute(tableInsert)

    partitionSelect = "SELECT * FROM information_schema.tables WHERE table_name LIKE 'range_part%' "
    cursor.execute(partitionSelect)

    noOfPartitions = len(cursor.fetchall())
    max_rating = 5.0
    partitionRange = float(max_rating / noOfPartitions)

    insertionQuery = "INSERT INTO range_part{} VALUES ({}, {}, {})"
        
    for partitionNumber in range(0, noOfPartitions):
        if partitionNumber == 0:
            if rating >= (partitionNumber*partitionRange) and rating <= (partitionNumber+1)*partitionRange:
                insertionString = insertionQuery.format(partitionNumber, userid, itemid, rating)
                cursor.execute(insertionString)
        else:
            if rating > (partitionNumber*partitionRange) and rating <= (partitionNumber+1)*partitionRange:
                insertionString = insertionQuery.format(partitionNumber, userid, itemid, rating)
                cursor.execute(insertionString)
    openconnection.commit()
    cursor.close()

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
