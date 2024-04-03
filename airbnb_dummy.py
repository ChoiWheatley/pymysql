import pymysql
from faker import Faker
import random

# Faker 객체 초기화
fake = Faker()

# 데이터베이스 연결 설정
conn = pymysql.connect(
    host="localhost",  # 데이터베이스 서버 주소
    user="root",  # 데이터베이스 사용자 이름
    password="oz-password",  # 데이터베이스 비밀번호
    db="airbnb",  # 데이터베이스 이름
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)


# Products 테이블을 위한 더미 데이터 생성
def generate_product_data(n):
    for _ in range(n):
        product_name = fake.word().capitalize() + " " + fake.word().capitalize()
        price = round(random.uniform(10, 100), 2)
        stock_quantity = random.randint(10, 100)
        create_date = fake.date_time_this_year()
        yield (product_name, price, stock_quantity, create_date)


# Customers 테이블을 위한 더미 데이터 생성
def generate_customer_data(n):
    for _ in range(n):
        customer_name = fake.name()
        email = fake.email()
        address = fake.address()
        create_date = fake.date_time_this_year()
        yield (customer_name, email, address, create_date)


# Orders 테이블을 위한 더미 데이터 생성
def generate_order_data(n, customer_ids):
    for _ in range(n):
        customer_id = random.choice(customer_ids)
        order_date = fake.date_time_this_year()
        total_amount = round(random.uniform(20, 500), 2)
        yield (customer_id, order_date, total_amount)


# 데이터베이스에 데이터 삽입
with conn.cursor() as cursor:
    # Products 데이터 삽입
    products_sql = "INSERT INTO Products (productName, price, stockQuantity, createDate) VALUES (%s, %s, %s, %s)"
    for data in generate_product_data(10):
        cursor.execute(products_sql, data)
    conn.commit()

    # Customers 데이터 삽입
    customers_sql = (
        "INSERT INTO Customers (customerName, email, address, createDate) VALUES (%s, %s, %s, %s)"
    )
    for data in generate_customer_data(5):
        cursor.execute(customers_sql, data)
    conn.commit()

    # Orders 데이터 삽입
    # Customers 테이블에서 ID 목록을 얻어옵니다.
    cursor.execute("SELECT customerID FROM Customers")
    customer_ids = [row["customerID"] for row in cursor.fetchall()]

    orders_sql = "INSERT INTO Orders (customerID, orderDate, totalAmount) VALUES (%s, %s, %s)"
    for data in generate_order_data(15, customer_ids):
        cursor.execute(orders_sql, data)
    conn.commit()

# 데이터베이스 연결 종료
conn.close()
