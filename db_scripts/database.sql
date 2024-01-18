CREATE DATABASE IF NOT EXISTS online_store;
USE online_store;

CREATE TABLE IF NOT EXISTS Products (
    product_id VARCHAR(32) NOT NULL,
    product_category VARCHAR(50),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT,
    PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS Users (
    user_name VARCHAR(32) NOT NULL,
    customer_zip_code INT,
    customer_city VARCHAR(50),
    customer_state VARCHAR(50),
    PRIMARY KEY (user_name)
);

CREATE TABLE IF NOT EXISTS Sellers (
    seller_id VARCHAR(32) NOT NULL,
    seller_zip_code INT,
    seller_city VARCHAR(50),
    seller_state VARCHAR(50),
    PRIMARY KEY (seller_id)
);

CREATE TABLE IF NOT EXISTS Orders (
    order_id VARCHAR(32) NOT NULL,
    user_name VARCHAR(32) NOT NULL,
    order_status VARCHAR(50),
    order_date DATETIME,
    order_approved_date DATETIME,
    pickup_date DATETIME,
    delivered_date DATETIME,
    estimated_time_delivery DATETIME,
    PRIMARY KEY (order_id),
    FOREIGN KEY (user_name) REFERENCES Users(user_name)
);

CREATE TABLE IF NOT EXISTS Payments (
    order_id VARCHAR(32) NOT NULL,
    payment_sequential TINYINT,
    payment_type VARCHAR(50),
    payment_installments TINYINT,
    payment_value FLOAT,
    FOREIGN KEY (order_id) REFERENCES Orders(order_id)
);

CREATE TABLE IF NOT EXISTS Order_Items (
    order_id VARCHAR(32) NOT NULL,
    order_item_id TINYINT,
    product_id VARCHAR(32) NOT NULL,
    seller_id VARCHAR(32) NOT NULL,
    pickup_limit_date DATETIME,
    price FLOAT,
    shipping_cost FLOAT,
    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id),
    FOREIGN KEY (seller_id) REFERENCES Sellers(seller_id)
);

CREATE TABLE IF NOT EXISTS Feedbacks (
    feedback_id VARCHAR(32) NOT NULL,
    order_id VARCHAR(32) NOT NULL,
    feedback_score TINYINT,
    feedback_form_sent_date DATETIME,
    feedback_answer_date DATETIME,
    PRIMARY KEY (feedback_id),
    FOREIGN KEY (order_id) REFERENCES Orders(order_id)
);

