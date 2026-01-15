import os
import mysql.connector
class Model:
    def __init__(self, table_columns={}, table_name=None, data=None, host=os.environ['localhost'], user=os.environ['root'], password=os.environ['admin'], database=os.environ['employees']):
        self.db = None
        self.cursor = None
        self.table_name = table_name
        self.table_columns = table_columns
        self.data = data
        if host is not None and user is not None and password is not None and database is not None:
            try:
                self.db = mysql.connector.connect(
                    host='localhost',
                    user='root',
                    password='admin',
                    database='employees'
                )
            except Exception as e:
                print(e)
                #print("One or more credentials were incorrect! Could not connect to you database!")
        if self.db is not None:
            self.cursor = self.db.cursor()
            if table_name is not None:
                self.cursor.execute("SHOW TABLES;")
                for e in self.cursor.fetchall():
                    if e[0] != self.table_name:
                        pk = [[e, self.table_columns[e]] for e in self.table_columns if "primary key" in e.lower()]
                        if len(pk) == 1:
                            pk = " ".join(pk) + ", "
                            del self.table_columns[pk[0]]
                        else:
                            pk = ""
                        try:
                            table_string = 'CREATE TABLE "' + self.table_name + '"(' + pk + ", ".join([" ".join(['"' + "_".join(c.split()) + '"', self.table_columns[c].upper()]) for c in self.table_columns]) + ');'
                            self.cursor.execute(table_string)
                            print("Created table with name: " + self.table_name)
                        except Exception as e:
                            self.db.rollback()
                            print(e)

    def insert(self):
        if self.data is not None:
            pkname = ""
            try:
                self.cursor.execute('SHOW KEYS FROM "(%s)" WHERE Key_name = (%s);', (self.table_name, 'PRIMARY'))
                pkname = self.cursor.fetchall()[0]
                if pkname in self.table_columns and pkname not in self.data:
                    del self.table_columns[pkname]
                elif pkname not in self.table_columns and pkname in self.data:
                    del  self.table_columns[pkname]
            except Exception as e:
                print("Could not get primary key name!")
                print(e)
            try:
                self.cursor.execute('SHOW COLUMNS FROM "' + self.table_name + '";')
                self.table_columns = {e: "" for e in self.cursor.fetchall()}
            except Exception as e:
                self.db.rollback()
                print("Could not find table with name " + self.table_name)
                print(e)
            flag = True
            for e in self.data:
                if e not in self.table_columns:
                    flag = False
            if flag:
                if len(self.data) == len(self.table_columns):
                    col = ["'" + e + "'" if e[0] != "'" and e[-1] !="'" else e for e in self.data]
                    data = [self.data[e] for e in self.data]
                    sql = "INSERT INTO %s (%s) VALUES (%s)"
                    val = ('"' + self.table_name + '"', ", ".join(col), ", ".join(data))
                    try:
                        self.cursor.execute(sql, val)
                        self.save()
                    except Exception as e:
                        print("Could not insert into " + self.table_name)
                        print(e)
            else:
                print("Found unexpected data. Try an insert or update query.")

    def save(self):
        committed = False
        try:
            self.db.commit()
            committed = True
        except Exception as e:
            self.db.rollback()
            print(e)
        if committed:
            self.db.close()
            self.db = None
            self.cursor = None