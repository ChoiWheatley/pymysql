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


def update_product(con: pymysql.connections.Connection, id: int):
    """제품 재고 업데이트: 제품이 주문될 때마다 'Products' 테이블의 해당 재고를 감소시키는 스크립트"""
    with con.cursor() as cursor:
        sql = "UPDATE Products SET stockQuantity = stockQuantity - %s WHERE productID = %s"
        cursor.execute(sql, (1, id))
        con.commit()

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

    # create_product(con, "우아아아", 10000, 10)
    # print("============== AFTER CREATE ============")
    # for book in get_all_product(con):
    #     pprint(book)
    
    update_product(con, 11)
    print("============== AFTER UPDATE ============")
    for book in get_all_product(con):
        pprint(book)
