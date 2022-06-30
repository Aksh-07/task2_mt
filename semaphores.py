import time
from threading import Semaphore, get_ident
import sqlite3
import concurrent.futures

s = time.time()
data_list1 = []
data_list2 = []
data_list3 = []
data_list4 = []

def create_table():
    con = sqlite3.connect("practice.db")
    c = con.cursor()
    c.execute("""CREATE TABLE my_table (
    column1 INTEGER,
    column2 INTEGER,
    column3 INTEGER,
    column4 TEXT
    )""")
    con.commit()
    con.close()


def delete_table():
    con = sqlite3.connect("practice.db")
    c = con.cursor()
    c.execute("DROP TABLE my_table")
    con.commit()
    con.close()


def task(list_, lock_):
    con = sqlite3.connect("practice.db")
    lock_.acquire()
    c = con.cursor()
    print(f"Thread_{get_ident()}, Pushing data into Database")
    c.executemany("INSERT INTO my_table VALUES (?, ?, ?, ?)", list_)
    lock_.release()
    con.commit()
    con.close()
    print(f"completed_{get_ident()}")
    if lock_._value == 4:
        fetch()


def fetch():
    con = sqlite3.connect("practice.db")
    c = con.cursor()
    c.execute("SELECT rowid, * FROM my_table")
    print(f"Thread_{get_ident()}, Fetching data from Database")

    items = c.fetchall()
    for item in items:
        print(f"{item[0]}: {item[1]} {item[2]} {item[3]} {item[4]}")
    con.close()


def convert_data(data):
    new_data = []

    for item_tuple in data:
        item_tuple = eval(item_tuple)
        item_list = list(item_tuple)
        item_list[3] = str(item_list[3])
        new_data.append(item_list)
    l = len(new_data)

    data_list1.extend(new_data[:int(l/4)])
    data_list2.extend(new_data[int(l/4):int(l/2)])
    data_list3.extend(new_data[int(l/2):int(l*(3/4))])
    data_list4.extend(new_data[int(l*(3/4)):])


lock = Semaphore(4)

delete_table()
create_table()

with open("dataset.txt", "rt") as file:
    data_ = file.readlines()

convert_data(data_)

all_data_list = [data_list1, data_list2, data_list3, data_list4]

with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [executor.submit(task, data, lock) for data in all_data_list]


f = time.time()
print(f"Total time: {f - s}")

