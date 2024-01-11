### Utility Functions
import pandas as pd
import sqlite3
import datetime
from sqlite3 import Error

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows

def step1_create_region_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION
    conn = create_connection(normalized_database_filename)
    with conn:
        cur=conn.cursor()
        create_table_sql = """
            CREATE TABLE Region (
                RegionID INTEGER NOT NULL PRIMARY KEY,
                Region TEXT NOT NULL 
            );"""
        create_table(conn, create_table_sql, drop_table_name='Region')
        sql_insert_Region = ''' INSERT INTO Region(Region)
                  VALUES(?)'''
        regions =set()
        header= None
        with open(data_filename) as f:
            for line in f:
                if not header:
                    header=line
                    continue
                line = line.strip()
                if not line:
                    continue
                line = line.split('\t')
                region = line[4]
                regions.add(region)
            mydata=[(ele,) for ele in regions]
            mydata=sorted(mydata)
        cur.executemany(sql_insert_Region,mydata)           
        conn.commit()
    conn.close()
    pass
    ### END SOLUTION

def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        sql_statement="SELECT RegionID,Region FROM Region"
        mydata=execute_sql_statement(sql_statement,conn)
        mydict= {}
        for row in mydata:
            key, value = row
            mydict[value] = key
    conn.close()
    return mydict
    pass

    pass

    ### END SOLUTION


def step3_create_country_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        cur=conn.cursor()
        create_sql='''CREATE TABLE Country(
        [CountryID] INTEGER NOT NULL PRIMARY KEY,
        [Country] TEXT NOT NULL,
        [RegionID] INTEGER NOT NULL,
        FOREIGN KEY(RegionID) REFERENCES Region(RegionID) ON DELETE CASCADE);'''
        insert_sql='''INSERT INTO Country(Country,RegionID) VALUES(?,?)'''
        create_table(conn,create_sql,drop_table_name='Country')
        fk_lookup=step2_create_region_to_regionid_dictionary(normalized_database_filename)
        countries=set()
        header= None
        with open(data_filename) as f:
            for line in f:
                if not header:
                    header=line
                    continue
                line = line.strip()
                if not line:
                    continue
                line = line.split('\t')
                country = line[3]
                region = line[4]
                countries.add((country,fk_lookup[region]))
            mydata=[ele for ele in countries]
            mydata=sorted(mydata)
        cur.executemany(insert_sql,mydata)
        conn.commit()
    conn.close()
    pass
         
    ### END SOLUTION


def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        get_country_sql="SELECT CountryID,Country FROM Country"
        country_val=execute_sql_statement(get_country_sql,conn)
        fk_lookup_country = {}
        for row in country_val:
            key, value = row
            fk_lookup_country[value] = key
    conn.close()
    return fk_lookup_country
    pass

    ### END SOLUTION
        
        
def step5_create_customer_table(data_filename, normalized_database_filename):

    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        cur=conn.cursor()
        create_sql='''CREATE TABLE Customer( 
        [CustomerID] integer not null Primary Key, 
        [FirstName] Text not null, 
        [LastName] Text not null, 
        [Address] Text not null, 
        [City] Text not null, 
        [CountryID] integer not null,
        foreign key(CountryID) REFERENCES Country(CountryID) ON DELETE CASCADE); '''
        insert_sql='''INSERT INTO Customer(FirstName,LastName,Address,City,CountryID) VALUES(?,?,?,?,?)'''
        create_table(conn,create_sql,'Customer')
        fk_lookup=step4_create_country_to_countryid_dictionary(normalized_database_filename)
        customers = set()
        header = None
        with open(data_filename) as f:
            for line in f:
                if not header:
                    header=line
                    continue
                line = line.strip()
                if not line:
                    continue
                line = line.split('\t')
                country = line[3]
                name = line[0].split(' ',1)
                firstname = name[0]
                lastname = name[1]
                Address = line[1]
                city = line[2]
                customers.add((firstname,lastname,Address,city,fk_lookup[country]))
            mydata=[ele for ele in customers ]
            mydata=sorted(mydata)
        cur.executemany(insert_sql,mydata)
        conn.commit()
    conn.close()
    pass

    ### END SOLUTION


def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        get_customer_sql="SELECT CustomerID,FirstName,LastName FROM Customer"
        country_val=execute_sql_statement(get_customer_sql,conn)
        fk_lookup_customer = {}
        for row in country_val:
            key = row[1]+' '+ row[2]
            value = row[0]
            fk_lookup_customer[key] = value
    conn.close()
    return fk_lookup_customer

    pass

    ### END SOLUTION
        
def step7_create_productcategory_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        cur=conn.cursor()
        create_sql='''CREATE TABLE ProductCategory(
        [ProductCategoryID] integer not null Primary Key,
        [ProductCategory] Text not null,
        [ProductCategoryDescription] Text not null);'''
        insert_sql='''INSERT INTO ProductCategory(ProductCategory,ProductCategoryDescription) VALUES(?,?)'''
        create_table(conn,create_sql,'ProductCategory')
        productcategories =set()
        header= None
        with open(data_filename) as f:
            for line in f:
                if not header:
                    header=line
                    continue
                line = line.strip()
                if not line:
                    continue
                line = line.split('\t')
                productcategory= line[6].split(';')
                description = line[7].split(';')
                productcategories=productcategories|set(list(zip(productcategory,description)))
            mydata=sorted(productcategories)
        cur.executemany(insert_sql,mydata)           
        conn.commit()
    conn.close()


    pass
   
    ### END SOLUTION

def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    
    
    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        sql_statement="SELECT ProductCategoryID,ProductCategory FROM ProductCategory"
        mydata=execute_sql_statement(sql_statement,conn)
        mydict= {}
        for row in mydata:
            key, value = row
            mydict[value] = key
    conn.close()
    return mydict

    pass

    ### END SOLUTION
        

def step9_create_product_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        cur=conn.cursor()
        create_sql='''CREATE TABLE Product(
        [ProductID] integer not null Primary Key,
        [ProductName] Text not null,
        [ProductUnitPrice] Real not null,
        [ProductCategoryID] integer not null,
        foreign key(ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID) ON DELETE CASCADE);'''
        insert_sql='''INSERT INTO Product(ProductName,ProductUnitPrice,ProductCategoryID) VALUES(?,?,?)'''
        create_table(conn,create_sql,'Product')
        fk_lookup=step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)
        products =set()
        header= None
        with open(data_filename) as f:
            for line in f:
                if not header:
                    header=line
                    continue
                line = line.strip()
                if not line:
                    continue
                line = line.split('\t')
                productname= line[5].split(';')
                productunitprice = line[8].split(';')
                productcategory= line[6].split(';')
                products=products|set(list(zip(productname,productunitprice,[fk_lookup[ele] for ele in productcategory])))
            mydata=sorted(products)
        cur.executemany(insert_sql,mydata)           
        conn.commit()
    conn.close()


    
    pass
   
    ### END SOLUTION


def step10_create_product_to_productid_dictionary(normalized_database_filename):
    
    ### BEGIN SOLUTION
    conn=create_connection(normalized_database_filename)
    with conn:
        sql_statement="SELECT ProductID,ProductName FROM Product"
        mydata=execute_sql_statement(sql_statement,conn)
        mydict= {}
        for row in mydata:
            key, value = row
            mydict[value] = key
    conn.close()
    return mydict
    pass

    ### END SOLUTION
        

def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    conn=create_connection(normalized_database_filename)
    
    with conn:
        cur=conn.cursor()
        create_sql='''CREATE TABLE OrderDetail(
        [OrderID] integer not null Primary Key,
        [CustomerID] integer not null,
        [ProductID] integer not null,
        [OrderDate] integer not null,
        [QuantityOrdered] integer not null,
        foreign key(CustomerID) REFERENCES Customer(CustomerID) ON DELETE CASCADE,
        foreign key(ProductID) REFERENCES Product(ProductID) ON DELETE CASCADE);'''
        insert_sql='''INSERT INTO OrderDetail(CustomerID,ProductID,OrderDate,QuantityOrdered) VALUES(?,?,?,?)'''
        create_table(conn,create_sql,'OrderDetail')
        fk_lookup_customerid = step6_create_customer_to_customerid_dictionary(normalized_database_filename) 
        fk_lookup_productid = step10_create_product_to_productid_dictionary(normalized_database_filename)
        header= None
        with open(data_filename) as f:
            for line in f:
                if not header:
                    header=line
                    continue
                line = line.strip()
                if not line:
                    continue
                line = line.split('\t')
                quantityordered= line[9].split(';')
                date = line[10].split(';')
                name = line[0]
                productcategory= line[5].split(';')
                date =list(map(lambda ele:datetime.datetime.strptime(ele, '%Y%m%d').strftime('%Y-%m-%d'),date))
                mydata=list(zip([fk_lookup_customerid[name]]*len([fk_lookup_productid[ele] for ele in productcategory]),[fk_lookup_productid[ele] for ele in productcategory],date,quantityordered))
                cur.executemany(insert_sql,mydata)           
        conn.commit()
    conn.close()

    
    ### BEGIN SOLUTION
    
    pass
    ### END SOLUTION


def ex1(conn, CustomerName):
    
    # Simply, you are fetching all the rows for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    sql_statement = """SELECT c.FirstName || ' ' || c.LastName as Name,p.ProductName,o.OrderDate,p.ProductUnitPrice,o.QuantityOrdered,
    round(p.ProductUnitPrice*o.QuantityOrdered,2) as Total 
    FROM OrderDetail o JOIN Customer c on o.CustomerID=c.CustomerID
    JOIN Product p on o.ProductID=p.ProductID WHERE Name='"""+CustomerName+"'"
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex2(conn, CustomerName):
    
    # Simply, you are summing the total for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    sql_statement = """SELECT c.FirstName || ' ' || c.LastName as Name,
    round(sum(p.ProductUnitPrice*o.QuantityOrdered),2) as Total 
    FROM OrderDetail o JOIN Customer c on o.CustomerID=c.CustomerID
    JOIN Product p on o.ProductID=p.ProductID WHERE Name='"""+CustomerName+"'"
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex3(conn):
    
    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION
    sql_statement = """SELECT c.FirstName || ' ' || c.LastName as Name,
    round(sum(p.ProductUnitPrice*o.QuantityOrdered),2) as Total 
    FROM OrderDetail o
    JOIN Customer c on o.CustomerID=c.CustomerID 
    JOIN Product p on o.ProductID=p.ProductID 
    GROUP BY o.CustomerID 
    ORDER BY Total DESC
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex4(conn):
    
    # Simply, find the total for all the region
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, Country, and 
    # Region tables.
    # Pull out the following columns. 
    # Region
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """SELECT r.Region,
    round(sum(p.ProductUnitPrice*o.QuantityOrdered),2) as Total 
    FROM OrderDetail o
    JOIN Customer c on o.CustomerID=c.CustomerID
    JOIN Product p on o.ProductID=p.ProductID 
    JOIN Country co on c.CountryID=co.CountryID
    JOIN Region r on co.RegionID=r.RegionID
    GROUP BY r.Region 
    ORDER BY Total DESC  
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex5(conn):
    
     # Simply, find the total for all the countries
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, and Country table.
    # Pull out the following columns. 
    # Country
    # CountryTotal -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """SELECT co.Country,
    round(sum(p.ProductUnitPrice*o.QuantityOrdered)) as CountryTotal 
    FROM OrderDetail o 
    JOIN Customer c on o.CustomerID=c.CustomerID
    JOIN Product p on o.ProductID=p.ProductID 
    JOIN Country co on c.CountryID=co.CountryID
    GROUP BY co.Country 
    ORDER BY CountryTotal DESC
    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex6(conn):
    
    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    ### BEGIN SOLUTION

    sql_statement = """SELECT 
        r.Region,
        co.Country,
        ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered)) AS CountryTotal,
        DENSE_RANK() OVER (
            PARTITION BY r.Region
            ORDER BY SUM(p.ProductUnitPrice * o.QuantityOrdered) DESC
        ) AS CountryRegionalRank
        FROM OrderDetail o
        JOIN Customer c ON o.CustomerID = c.CustomerID
        JOIN Product p ON o.ProductID = p.ProductID 
        JOIN Country co ON c.CountryID = co.CountryID
        JOIN Region r ON co.RegionID = r.RegionID
        GROUP BY r.Region, co.Country"""
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement



def ex7(conn):
    
   # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"
    ### BEGIN SOLUTION

    sql_statement = """WITH CountryRegion AS(
    SELECT r.Region,co.Country, round(sum(p.ProductUnitPrice*o.QuantityOrdered)) as CountryTotal,
    RANK() OVER ( 
            PARTITION BY r.Region
            ORDER BY round(sum(p.ProductUnitPrice*o.QuantityOrdered)) DESC
        ) AS CountryRegionalRank
    FROM OrderDetail o
    JOIN Customer c on o.CustomerID=c.CustomerID
    JOIN Product p on o.ProductID=p.ProductID 
    JOIN Country co on c.CountryID=co.CountryID
    JOIN Region r on co.RegionID=r.RegionID
    GROUP BY r.Region,co.Country
    )
    SELECT * from CountryRegion WHERE CountryRegionalRank=1"""
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex8(conn):
    
    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    ### BEGIN SOLUTION

    sql_statement = """WITH customertemp as (
    SELECT 
    CASE 
    WHEN cast(strftime('%m', o.OrderDate) as integer) IN (1,2,3) THEN 'Q1'
    WHEN cast(strftime('%m', o.OrderDate) as integer) IN(4,5,6) THEN 'Q2'
    WHEN cast(strftime('%m', o.OrderDate) as integer) IN(7,8,9) THEN 'Q3'
    ELSE 'Q4' END as Quarter,
    CAST(strftime('%Y', o.OrderDate) as INTEGER) as Year,o.CustomerID,round(sum(p.ProductUnitPrice*o.QuantityOrdered)) as Total
    FROM OrderDetail o
    JOIN Product p on o.ProductID = p.ProductID
    GROUP BY Quarter,Year,o.CustomerID
    )
    SELECT *
    FROM customertemp
    ORDER BY Year"""
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex9(conn):
    
    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()
    ### BEGIN SOLUTION

    sql_statement = """WITH customertemp as (
    SELECT 
    CASE 
    WHEN cast(strftime('%m', o.OrderDate) as integer) IN (1,2,3) THEN 'Q1'
    WHEN cast(strftime('%m', o.OrderDate) as integer) IN (4,5,6) THEN 'Q2'
    WHEN cast(strftime('%m', o.OrderDate) as integer) IN (7,8,9) THEN 'Q3'
    ELSE 'Q4' END as Quarter,
    CAST(strftime('%Y', o.OrderDate) as INTEGER) as Year,o.CustomerID,round(sum(p.ProductUnitPrice*o.QuantityOrdered)) as Total
    FROM OrderDetail o
    JOIN Product p on o.ProductID = p.ProductID
    GROUP BY Quarter,Year,o.CustomerID
    ),
    ranktemp as (
    SELECT *,
    RANK() OVER ( PARTITION BY ct.Quarter,ct.Year
                ORDER BY ct.Total DESC
            ) CustomerRank
            FROM customertemp as ct
    )
    SELECT *
    FROM ranktemp 
    WHERE ranktemp.CustomerRank<6
    ORDER BY Year"""
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex10(conn):
    
    # Rank the monthly sales
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    ### BEGIN SOLUTION

    sql_statement = """WITH customers as (
    SELECT 
    strftime('%m', OrderDate) as m,OrderDate,sum(round(p.ProductUnitPrice*o.QuantityOrdered)) as Total
    FROM OrderDetail o
    JOIN Product p on o.ProductID = p.ProductID
    GROUP BY m
    ),
    ranks as (
    SELECT CASE
            WHEN strftime('%m', ct.OrderDate) = '01' THEN 'January'
            WHEN strftime('%m', ct.OrderDate) = '02' THEN 'February'
            WHEN strftime('%m', ct.OrderDate) = '03' THEN 'March'
            WHEN strftime('%m', ct.OrderDate) = '04' THEN 'April'
            WHEN strftime('%m', ct.OrderDate) = '05' THEN 'May'
            WHEN strftime('%m', ct.OrderDate) = '06' THEN 'June'
            WHEN strftime('%m', ct.OrderDate) = '07' THEN 'July'
            WHEN strftime('%m', ct.OrderDate) = '08' THEN 'August'
            WHEN strftime('%m', ct.OrderDate) = '09' THEN 'September'
            WHEN strftime('%m', ct.OrderDate) = '10' THEN 'October'
            WHEN strftime('%m', ct.OrderDate) = '11' THEN 'November'
            WHEN strftime('%m', ct.OrderDate) = '12' THEN 'December'
        END Month,ct.Total,
    RANK() OVER ( 
                ORDER BY ct.Total DESC
            ) TotalRank
            FROM customers as ct
    )
    SELECT *
    FROM ranks """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex11(conn):
    
    # Find the MaxDaysWithoutOrder for each customer 
    # Output Columns: 
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate, 
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag

    ### BEGIN SOLUTION
    sql_statement = """ WITH lagt AS(
    SELECT c.CustomerID,c.FirstName,c.LastName,co.Country,o.OrderDate,
    lag(o.OrderDate, 1) over (order by c.CustomerID,o.OrderDate) as PreviousOrderDate
    FROM OrderDetail o
    JOIN Customer c on o.CustomerID=c.CustomerID
    JOIN Country co on c.CountryID=co.CountryID), 
    ordertemp as(SELECT *,max(julianday(lt.OrderDate) - julianday(lt.PreviousOrderDate)) as MaxDaysWithoutOrder 
    FROM lagt as lt
    GROUP BY CustomerID)
    SELECT * FROM ordertemp ORDER BY MaxDaysWithoutOrder DESC, FirstName DESC """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement