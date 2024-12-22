import sqlite3
import pandas as pd
import streamlit as st

# Function to connect to the first SQLite database
def get_db_connection_1():
    conn = sqlite3.connect('F:/1st_mini_poject/orders.db')  # Path to first database
    return conn

# Function to connect to the second SQLite database
def get_db_connection_2():
    conn = sqlite3.connect('F:/1st_mini_poject/Copy of orders.db')  # Path to second database
    return conn

if __name__ == "__main__":

  # Streamlit app
  st.title("Retail Order Data Analysis")

  st.markdown("""
  ### Overview
  This app analyzes the sales data to extract insights like top-selling products, regions with high profits, and more. The data is pulled from two different databases.
  """)

  # --- First Database (for questions 1-10) ---
  st.header("Questions 1-10")

  # Connect to the first database
  conn_1 = get_db_connection_1()
  cursor_1 = conn_1.cursor()

  queries = {
    "Top 10 Highest Revenue Generating Products": '''
          WITH ranked_products AS (
          SELECT product_id,
                 SUM(sale_price * quantity) AS total_revenue,
                 ROW_NUMBER() OVER (ORDER BY SUM(sale_price * quantity) DESC) AS rank
          FROM orders
          GROUP BY product_id
        )
        SELECT product_id, total_revenue
        FROM ranked_products
        WHERE rank <= 10;
      ''',
    "Cities with the Highest Profit Margins": '''
          SELECT city,
                 SUM(profit) / SUM(sale_price) * 100 AS profit_margin
          FROM orders
          GROUP BY city
          HAVING SUM(sale_price) > 0
          ORDER BY profit_margin DESC
          LIMIT 5;
      ''',
    "Total Discount Given for Each Category": '''
          SELECT category,
                 SUM(discount) AS total_discount
          FROM orders
          GROUP BY category
          ORDER BY total_discount DESC;
      ''',
    "Average Sale Price Per Product Category": '''
          SELECT category,
                 AVG(sale_price) AS average_sale_price
          FROM orders
          GROUP BY category;
      ''',
    "Region with the Highest Average Sale Price": '''
          WITH ranked_regions AS (
          SELECT region,
                 AVG(sale_price) AS average_sale_price,
                 ROW_NUMBER() OVER (ORDER BY AVG(sale_price) DESC) AS rank
          FROM orders
          GROUP BY region
        )
        SELECT region, average_sale_price
        FROM ranked_regions
        WHERE rank = 1;
      ''',
    "Total Profit per Category": '''
          SELECT category,
                 SUM(profit) AS total_profit
          FROM orders
          GROUP BY category
          ORDER BY total_profit DESC;
      ''',
    "Top 3 Highest Quantity of Orders by Segment": '''
          SELECT segment,
                 SUM(quantity) AS total_quantity
          FROM orders
          GROUP BY segment
          ORDER BY total_quantity DESC
          LIMIT 3;
      ''',
    "Average Discount Percentage per Region": '''
          SELECT region,
                 AVG(discount_percent) AS avg_discount_percentage
          FROM orders
          GROUP BY region;
      ''',
    "Product Category with Highest Total Profit": '''
          SELECT category,
                 SUM(profit) AS total_profit
          FROM orders
          GROUP BY category
          ORDER BY total_profit DESC
          LIMIT 1;
      ''',
    "Total Revenue by Year": '''
          SELECT strftime('%Y', order_date) AS year,
                 SUM(sale_price * quantity) AS total_revenue
          FROM orders
          GROUP BY year;
      '''
  }

  # Dropdown menu to select a query
  selected_query = st.selectbox("Select a Query", list(queries.keys()))

  # Execute the selected query and display results
  if selected_query:
    st.subheader(selected_query)
    cursor_1.execute(queries[selected_query])
    results = cursor_1.fetchall()

    # Create column names based on the selected query
    if selected_query == "Top 10 Highest Revenue Generating Products":
      columns = ["Product ID", "Total Revenue"]
    elif selected_query == "Cities with the Highest Profit Margins":
      columns = ["City", "Profit Margin"]
    elif selected_query == "Total Discount Given for Each Category":
      columns = ["Category", "Total Discount"]
    elif selected_query == "Average Sale Price Per Product Category":
      columns = ["Category", "Average Sale Price"]
    elif selected_query == "Region with the Highest Average Sale Price":
      columns = ["Region", "Average Sale Price"]
    elif selected_query == "Total Profit per Category":
      columns = ["Category", "Total Profit"]
    elif selected_query == "Top 3 Highest Quantity of Orders by Segment":
      columns = ["Segment", "Total Quantity"]
    elif selected_query == "Average Discount Percentage per Region":
      columns = ["Region", "Avg Discount Percentage"]
    elif selected_query == "Product Category with Highest Total Profit":
      columns = ["Category", "Total Profit"]
    elif selected_query == "Total Revenue by Year":
      columns = ["Year", "Total Revenue"]
    else:
      columns = []

    # Create and display the DataFrame
    df = pd.DataFrame(results, columns=columns)
    st.dataframe(df)
  # Close the first database connection
  conn_1.close()

  # --- Second Database (for questions 11-20) ---
  st.header("Questions 11-20")

  # Connect to the second database
  conn_2 = get_db_connection_2()
  cursor_2 = conn_2.cursor()

  cursor_2.execute('''
    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER PRIMARY KEY,
        order_date TEXT,
        ship_mode VARCHAR(100),
        segment VARCHAR(100),
        country VARCHAR(100),
        city VARCHAR(100),
        state VARCHAR(100),
        postal_code TEXT,
        region VARCHAR(100),
        category VARCHAR(100),
        sub_category VARCHAR(100),
        product_id VARCHAR(100),
        cost_price REAL,
        list_price REAL,
        quantity INTEGER,
        discount_percent REAL,
        discount REAL,
        sale_price REAL,
        profit REAL
    )
  ''')



  cursor_2.execute('''
  CREATE TABLE IF NOT EXISTS product_details (
    product_id VARCHAR(100) PRIMARY KEY,
    category VARCHAR(100),
    sub_category VARCHAR(100),
    cost_price REAL,
    list_price REAL,
    quantity INTEGER,
    discount_percent REAL,
    discount REAL,
    sale_price REAL,
    profit REAL
  );
  ''')

  cursor_2.execute('''
  CREATE TABLE IF NOT EXISTS customer_details (
    order_id INTEGER PRIMARY KEY,
    order_date TEXT,
    ship_mode VARCHAR(100),
    segment VARCHAR(100),
    country VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code TEXT,
    region VARCHAR(100),
    product_id VARCHAR(100),
    FOREIGN KEY (product_id) REFERENCES product_details (product_id)
  );
  ''')

  cursor_2.execute('''
  INSERT OR REPLACE INTO product_details (product_id, category, sub_category, cost_price, list_price, quantity, discount_percent, discount, sale_price, profit)
  SELECT product_id, category, sub_category, cost_price, list_price, quantity, discount_percent, discount, sale_price, profit
  FROM orders;
  ''')

  cursor_2.execute('''
  INSERT OR REPLACE INTO customer_details (order_id, order_date, ship_mode, segment, country, city, state, postal_code, region, product_id)
  SELECT order_id, order_date, ship_mode, segment, country, city, state, postal_code, region, product_id
  FROM orders;
  ''')


  additional_queries = {
    "Number of Orders per Product Category": '''
          SELECT pd.category, COUNT(cd.order_id) AS total_orders
          FROM product_details pd
          JOIN customer_details cd ON pd.product_id = cd.product_id
          GROUP BY pd.category
          ORDER BY total_orders DESC;
      ''',
    "Average Quantity Sold per Product in Each Region": '''
          SELECT cd.region, pd.product_id, AVG(pd.quantity) AS avg_quantity_sold
          FROM customer_details cd
          JOIN product_details pd ON cd.product_id = pd.product_id
          GROUP BY cd.region, pd.product_id
          ORDER BY avg_quantity_sold DESC;
      ''',
    "Top 5 Products with the Highest Profit, Including their Region": '''
          SELECT pd.product_id, pd.profit, cd.region, cd.order_date
          FROM product_details pd
          JOIN customer_details cd ON pd.product_id = cd.product_id
          ORDER BY pd.profit DESC
          LIMIT 5;
      ''',
    "Total Number of Orders per Product": '''
          SELECT pd.product_id, COUNT(cd.order_id) AS order_count
          FROM product_details pd
          JOIN customer_details cd ON pd.product_id = cd.product_id
          GROUP BY pd.product_id
          ORDER BY order_count DESC;
      ''',
    "Region with the Highest Total Revenue for Each Product": '''
          SELECT cd.region, pd.product_id, SUM(pd.sale_price * pd.quantity) AS total_revenue
          FROM customer_details cd
          JOIN product_details pd ON cd.product_id = pd.product_id
          GROUP BY cd.region, pd.product_id
          ORDER BY total_revenue DESC
          LIMIT 1;
      ''',
    "Top 3 Cities with Highest Discount": '''
          WITH RankedCities AS (
              SELECT 
                  cd.city, 
                  pd.product_id, 
                  pd.discount_percent AS highest_discount_percent,
                  ROW_NUMBER() OVER (PARTITION BY cd.city ORDER BY pd.discount_percent DESC) AS rank_within_city
              FROM 
                  customer_details cd
              JOIN 
                  product_details pd 
                  ON cd.product_id = pd.product_id
          )
          SELECT 
              city, 
              product_id, 
              highest_discount_percent
          FROM 
              RankedCities
          WHERE 
              rank_within_city = 1
          ORDER BY 
              highest_discount_percent DESC
          LIMIT 3;
      ''',
    "Total Cost Price and Total Profit for Each Region": '''
          SELECT cd.region, SUM(pd.cost_price * pd.quantity) AS total_cost_price,
                 SUM(pd.profit * pd.quantity) AS total_profit
          FROM customer_details cd
          JOIN product_details pd ON cd.product_id = pd.product_id
          GROUP BY cd.region
          ORDER BY total_profit DESC;
      ''',
    "Average Profit Margin per Product Category": '''
          SELECT pd.category,
                 AVG((pd.profit / pd.cost_price) * 100) AS avg_profit_margin
          FROM product_details pd
          JOIN customer_details cd ON pd.product_id = cd.product_id
          GROUP BY pd.category
          ORDER BY avg_profit_margin DESC;
      ''',
    "Total Quantity of Orders in Furniture Products": '''
          SELECT SUM(pd.quantity) AS total_quantity
          FROM customer_details cd
          JOIN product_details pd ON cd.product_id = pd.product_id
          WHERE pd.category = 'Furniture';
      ''',
    "Product that Gives Profit More Than 1000": '''
          SELECT cd.region, pd.product_id, pd.sale_price, pd.cost_price, pd.profit
          FROM product_details pd
          JOIN customer_details cd ON pd.product_id = cd.product_id
          WHERE pd.profit > 1000
          ORDER BY cd.region, pd.product_id;
      '''
  }

  # Dropdown menu for selecting queries
  selected_query = st.selectbox(
    "Select a Query",
    list(additional_queries.keys())
  )

  # Execute the selected query and display results
  if selected_query:
    st.subheader(selected_query)
    cursor_2.execute(additional_queries[selected_query])
    results = cursor_2.fetchall()

    # Map selected query to corresponding column names
    columns_mapping = {
      "Number of Orders per Product Category": ["Category", "Total Orders"],
      "Average Quantity Sold per Product in Each Region": ["Region", "Product ID", "Avg Quantity Sold"],
      "Top 5 Products with the Highest Profit, Including their Region": ["Product ID", "Profit", "Region",
                                                                         "Order Date"],
      "Total Number of Orders per Product": ["Product ID", "Order Count"],
      "Region with the Highest Total Revenue for Each Product": ["Region", "Product ID", "Total Revenue"],
      "Top 3 Cities with Highest Discount": ["City", "Product ID", "Highest Discount Percent"],
      "Total Cost Price and Total Profit for Each Region": ["Region", "Total Cost Price", "Total Profit"],
      "Average Profit Margin per Product Category": ["Category", "Avg Profit Margin"],
      "Total Quantity of Orders in Furniture Products": ["Total Quantity"],
      "Product that Gives Profit More Than 1000": ["Region", "Product ID", "Sale Price", "Cost Price", "Profit"]
    }

    # Create and display the DataFrame
    df = pd.DataFrame(results, columns=columns_mapping[selected_query])
    st.dataframe(df)

  # Close the second database connection
  conn_2.close()