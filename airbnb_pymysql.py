from typing import Dict
import pymysql.cursors
import pymysql.connections
from pprint import pprint


def create_product(con: pymysql.connections.Connection, name: str, price: int, quant: int):
    with con.cursor() as cursor:
        sql = "INSERT INTO Products(price, productName, stockQuantity) VALUES (%s, %s, %s)"
        cursor.execute(sql, (price, name, quant))
        con.commit()


def get_all_product(con: pymysql.connections.Connection) -> Dict:
    with con.cursor() as cursor:
        sql = "SELECT * FROM Products"
        cursor.execute(sql)
        return cursor.fetchall()


if __name__ == "__main__":
    con = pymysql.connect(
        host="localhost",
        user="root",
        password="oz-password",
        db="airbnb",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )

    print("============== BEFORE ============")
    for book in get_all_product(con):
        pprint(book)
    
    create_product(con, "우아아아", 10000, 10)
    print("============== AFTER ============")
    for book in get_all_product(con):
        pprint(book)
