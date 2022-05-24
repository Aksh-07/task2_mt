import time
from threading import Semaphore, get_ident
import sqlite3
import concurrent.futures

s = time.time()


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
    new_item_list = []

    for item_tuple in list_:
        item_list = list(item_tuple)
        item_list[3] = str(item_list[3])
        new_item_tuple = tuple(item_list)
        new_item_list.append(new_item_tuple)

    con = sqlite3.connect("practice.db")
    lock_.acquire()
    c = con.cursor()
    print(f"Thread_{get_ident()}, Pushing data into Database")
    c.executemany("INSERT INTO my_table VALUES (?, ?, ?, ?)", new_item_list)
    con.commit()
    lock_.release()

    c.execute("SELECT rowid, * FROM my_table")
    print(f"Thread_{get_ident()}, Fetching data from Database")

    items = c.fetchall()
    for item in items:
        print(f"{item[0]}: {item[1]} {item[2]} {item[3]} {item[4]}")

    print(f"Thread_{get_ident()} completed")
    con.close()


lock = Semaphore(4)

# delete_table()
# create_table()

data_list1 = [(1, 2, 3, [17, 7, 1998]), (4, 5, 6, [12, 2, 1996]), (7, 8, 9, [25, 12, 1990])]
data_list2 = [(101, 202, 203, ["aksh", "datta", "garry"]), (85, 89, 90, ["jim", "tim", "jerry"]), (64, 65, 68, ["ram", "lakhan", "bheem"])]
data_list3 = [(10, 20, 30, [15, 9, 1990]), (22, 33, 44, [12, 2, 1996]), (64, 65, 68, [25, 12, 1990])]
data_list4 = [(1001, 2002, 2003, ["aksh", "datta", "garry"]), (805, 809, 900, ["jim", "tim", "jerry"]), (604, 605, 608, ["ram", "lakhan", "bheem"])]

all_data_list = [data_list1, data_list2, data_list3, data_list4]

with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [executor.submit(task, data, lock) for data in all_data_list]


f = time.time()
print(f"Total time: {f - s}")

