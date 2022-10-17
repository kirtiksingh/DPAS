#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS " + ratingstablename)
    cur.execute("CREATE TABLE " + ratingstablename + "(UserID int, MovieID int, Rating float)" )
    with open(ratingsfilepath, "r") as ratingfile:
        for row in ratingfile:
            [UserId, MovieId, Rating, Timestamp] = row.split("::")
            cur.execute("INSERT INTO {0} VALUES ({1},{2},{3})".format(ratingstablename, UserId, MovieId, Rating))
    ratingfile.close()
    openconnection.commit()
    cur.close()

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    n=numberofpartitions
    sizeofeachpartition = float(5.0/n)

    for i in range(0,n):
        if i == 0:
            cur.execute("CREATE TABLE range_part{0} AS SELECT * FROM {1} WHERE Rating >= {2} AND Rating <= {3};".format(i,ratingstablename, i*sizeofeachpartition, (i+1)*sizeofeachpartition))
        else:
            cur.execute("CREATE TABLE range_part{0} AS SELECT * FROM {1} WHERE Rating > {2} AND Rating <= {3};".format(i,ratingstablename, i *sizeofeachpartition, (i+1) *sizeofeachpartition))
    
    openconnection.commit()
    cur.close()



def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions<=0:
        return
    cur = openconnection.cursor()
    for i in range(0,numberofpartitions):
        cur.execute("CREATE TABLE rrobin_part{0} AS SELECT userid,movieid,rating FROM (SELECT userid, movieid, rating, ROW_NUMBER() OVER() as rid FROM {1}) AS t WHERE (t.rid-1)%{2}) = {3}".format(i, ratingstablename, numberofpartitions, i))
    cur.close()



def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    
    cur.execute("INSERT INTO {0} VALUES ({1},{2},{3})".format(ratingstablename, userid, itemid, rating))
    
    cur.execute("SELECT * FROM {0}".format(ratingstablename))
    rec = len(cur.fetchall())
    
    cur.execute("SELECT * FROM information_schema.tables WHERE table_name LIKE 'rrobin_part%' ")
    parts = len(cur.fetchall())

    rr_partid = (rec-1)%parts

    cur.execute('''INSERT INTO rrobin_part{0} VALUES ({1},{2},{3})'''.format(rr_partid, userid, itemid, rating))
    cur.close()
    


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    if rating < 0 or rating > 5:
        return
    
    cur = openconnection.cursor()
   
    cur.execute("INSERT INTO {} (Userid, Movieid, Rating) VALUES ({}, {}, {})".format(ratingstablename, userid, itemid, rating))

    cur.execute("SELECT * FROM information_schema.tables WHERE table_name LIKE 'range_part%' ")
    numberofpartitions = len(cur.fetchall())
    sizeofeachpartition = float(5.0/numberofpartitions)
    for i in range(0,numberofpartitions):
        if i==0:
            if rating >=0 and rating <= sizeofeachpartition:
                cur.execute("inser into range_part{} values({},{},{})".format(0,userid, itemid, rating))
        else:
            if(rating > i*sizeofeachpartition and rating <= (i+1)*sizeofeachpartition):
                cur.execute("inser into range_part{} values({},{},{})".format(i,userid, itemid, rating))
            
    cur.execute("INSERT INTO range_part{} (Userid, Movieid, Rating) VALUES ({}, {}, {})".format(group, userid, itemid, rating))
    cur.close()

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
