#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2


# def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
def getOpenConnection(user='postgres', password='Vanessa.825', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    print("---Reading File Contents");
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS " + ratingstablename)
    cur.execute("CREATE TABLE " + ratingstablename + " (UserID INT, MovieID INT, Rating FLOAT);")
    openconnection.commit()

    r = open(ratingsfilepath, 'r').readlines()
    for line in r:
        values = line.split('::')
        command = "INSERT INTO ratings(UserID, MovieID, Rating) VALUES(%s, %s, %s);" % (values[0], values[1], values[2])
        cur.execute(command)

    openconnection.commit()
    cur.close()

    print("Data loaded.")


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    print("---Creating partitions...")
    maxrate = 5.0
    range = maxrate/numberofpartitions

    cur = openconnection.cursor()
    s = 0.0
    e = s + range
    p = 0

    # create partition meta-data
    cur.execute("CREATE TABLE IF NOT EXISTS partitions(partition INT, low_limit FLOAT, high_limit FLOAT)")

    while s < maxrate:
        p_name = 'range_part' + str(p)
        if s == 0:
            command = ("CREATE TABLE %s AS SELECT * FROM %s WHERE rating >= %f AND rating <= %f;" % (p_name, ratingstablename, s, e))
            cur.execute("INSERT INTO partitions VALUES(%s, %f, %f)" % (p, s, e))
        else:
            command = ("CREATE TABLE %s AS SELECT * FROM %s WHERE rating > %f AND rating <= %f;" % (p_name, ratingstablename, s, e))
            cur.execute("INSERT INTO partitions VALUES(%s, %f, %f)" % (p, s, e))
        cur.execute(command)
        s = e
        e = s + range
        p += 1

    openconnection.commit()
    cur.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    print("---Round Robin Partitioning...")

    cur = openconnection.cursor()
    cur.execute("SELECT * FROM %s" % ratingstablename)
    # rows = get all rows
    rows = cur.fetchall()

    n = 0
    while n < numberofpartitions:
      r_name = "rrobin_part" + str(n)
      cur.execute("CREATE TABLE IF NOT EXISTS %s (UserID INT, MovieID INT, Rating FLOAT)" % r_name)
      n += 1

    n = 1
    for r in rows:
        r_name = "rrobin_part" + str(n)
        cur.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES (%d, %d, %f)" % (r_name, r[0], r[1], r[2]))
        n = (n + 1) % numberofpartitions

        # create meta-data table
        cur.execute("CREATE TABLE IF NOT EXISTS partitions_rrobin(partition INT)")
        cur.execute("INSERT INTO partitions_rrobin VALUES(%s)" % (numberofpartitions))

    openconnection.commit()
    cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    print("---Round Robin Insert")

    cur = openconnection.cursor()
    cur.execute("SELECT * FROM partitions_rrobin")
    num_partitions = cur.fetchone()[0]

    n = 0
    for i in range(num_partitions):
        command = "SELECT count(*) from rrobin_part%s" % (str(i))
        cur.execute(command)
        o = cur.fetchone()[0]
        n = n + o

    x = (n % num_partitions)
    command = "INSERT INTO rrobin_part%s VALUES(%s, %s, %s)" % (x, userid, itemid, rating)
    cur.execute(command)

    openconnection.commit()
    cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    print("---Range Insert")
    max_rating = 5.0

    cur = openconnection.cursor()

    cur.execute("SELECT COUNT(*) FROM partitions")
    num_partitions = cur.fetchone()[0]
    range = max_rating / num_partitions

    # search between previously stored upper and lower bounds
    if rating == max_rating:
        command = "SELECT partition FROM partitions WHERE high_limit=%s" % rating
        cur.execute(command)
        p = cur.fetchone()
        partition = p[0]
    elif rating > 0:
        command = "SELECT * FROM partitions WHERE %s>low_limit AND %s<=high_limit" % (rating, rating)
        cur.execute(command)
        p = cur.fetchone()
        partition = p[0]
    else:
        command = "SELECT partition FROM partitions WHERE %s>=low_limit AND %s<=high_limit" % (rating, rating + range)
        cur.execute(command)
        p = cur.fetchone()
        partition = p[0]

    cur.execute("INSERT INTO range_part%s VALUES(%s, %s, %s)" % (partition, userid, itemid, rating))

    openconnection.commit()
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
