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

            loadData = "INSERT INTO " + ratingstablename + "(UserID, MovieID, Rating) VALUES(%s,%s,%s);" % (userId, movieId, rating)
            cursor.execute(loadData)
    openconnection.commit()
    cursor.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor()

    max_rate = 5.0
    range = float(max_rate / numberofpartitions)

    for i in range(0, numberofpartitions):
        partition_name = 'range_part' + str(i)
        dropTable = "DROP TABLE IF EXISTS "+ partition_name + ";"
        cursor.execute(dropTable)

        j = float(i)

        if i == 0:
            createPartition = "CREATE TABLE {} AS SELECT * FROM {} WHERE Rating >= {} AND Rating <= {} ;".format(partition_name, ratingstablename, str(j*range), str((j+1)*range))
        else:
            createPartition = "CREATE TABLE {} AS SELECT * FROM {} WHERE Rating > {} AND Rating <= {} ;".format(partition_name, ratingstablename, str(j*range), str((j+1)*range))
        
        cursor.execute(createPartition)

    cursor.close()


    






































def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    pass

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
