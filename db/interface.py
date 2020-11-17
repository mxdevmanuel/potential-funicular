import mysql.connector as mysql
import functools
import operator
import os
import re


def zipExtractor(filename):
    m = re.search('\d{5}', filename)
    if m:
        return m.group(0)


def zipFilter(row):
    return row[0] is not None


class DBConnection:
    def __init__(self):
        self.db = mysql.connect(host=os.environ.get('MYSQL_HOST', 'localhost'), user=os.environ.get(
            'MYSQL_USER', 'root'), passwd=os.environ.get('MYSQL_PASS', 'password'), pool_name="pypool", pool_size=2)
        self.cursor = self.db.cursor(buffered=True)

        self.cursor.execute("CREATE DATABASE IF NOT EXISTS kmlserver;")
        self.cursor.execute("SHOW DATABASES")
        self.cursor.execute("USE kmlserver;")
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS kml (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, zipcode VARCHAR(10) NOT NULL, uri VARCHAR(255) NOT NULL);")
        self.cursor.execute("SHOW TABLES")
        self.cursor.execute("SELECT * FROM kml;")
        rows = self.cursor.fetchall()

        if len(rows) == 0:
            zipPath= os.path.join(os.getcwd(), 'zips')
            files = [(zipExtractor(x), os.path.join(zipPath, x))
                     for x in os.listdir(zipPath)]
            files = filter(zipFilter, files)
            query = "INSERT INTO kml (zipcode, uri) VALUES (%s, %s)"
            self.cursor.executemany(query, [x for x in files])
            self.db.commit()
            print(self.cursor.rowcount, "records inserted")

    def search(self, zipcode):
        query = "SELECT zipcode, uri FROM kml WHERE zipcode LIKE '%{}%'".format(zipcode)
        self.cursor.execute(query)
        m = self.cursor.fetchall()
        if len(m) > 0:
            return m[0]
        else:
            return None

