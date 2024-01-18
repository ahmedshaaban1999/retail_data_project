import pandas as pd
import numpy as np
import sys
import math

from sdv.metadata import SingleTableMetadata
from sdv.single_table import GaussianCopulaSynthesizer
from mysql.connector import connect, Error


def DataSize(message, obj_0):
    size = sys.getsizeof(obj_0)
    unit = ['Byte','KB','MB','GB']
    unit_i = 0
    while True:
        if math.log10(size) > 3:
            size = size / 1024
            unit_i += 1
        else:
            break
    print(message + ' ' + str(size) + ' ' + unit[unit_i])


def RemoveData(connection):
    cursor = connection.cursor()
    cursor.execute('delete from online_store.order_items;')
    cursor.execute('delete from online_store.feedbacks;')
    cursor.execute('delete from online_store.payments;')
    cursor.execute('delete from online_store.orders;')
    cursor.execute('delete from online_store.users;')
    cursor.execute('delete from online_store.sellers;')
    cursor.execute('delete from online_store.products;')
    connection.commit()



def CreateSynthesizers():
    print('Creating Synthesizers ...')
    product_dataset = pd.read_csv('data/product_dataset.csv')
    product_metadata = SingleTableMetadata()
    product_metadata.detect_from_dataframe(product_dataset)
    product_metadata.update_column(column_name='product_id', sdtype='md5')
    product_synthesizer = GaussianCopulaSynthesizer(product_metadata)
    product_synthesizer.fit(product_dataset)
    del product_dataset
    
    user_dataset = pd.read_csv('data/user_dataset.csv')
    user_dataset = user_dataset.groupby('user_name').first().reset_index()
    user_metadata = SingleTableMetadata()
    user_metadata.detect_from_dataframe(user_dataset)
    user_metadata.update_column(column_name='user_name', sdtype='md5')
    user_metadata.set_primary_key(column_name='user_name')
    user_synthesizer = GaussianCopulaSynthesizer(user_metadata)
    user_synthesizer.fit(user_dataset)
    del user_dataset

    seller_dataset = pd.read_csv('data/seller_dataset.csv')
    seller_metadata = SingleTableMetadata()
    seller_metadata.detect_from_dataframe(seller_dataset)
    seller_metadata.update_column(column_name='seller_id', sdtype='md5')
    seller_synthesizer = GaussianCopulaSynthesizer(seller_metadata)
    seller_synthesizer.fit(seller_dataset)
    del seller_dataset

    order_dataset = pd.read_csv('data/order_dataset.csv')
    order_dataset.drop(['user_name'], axis=1, inplace=True)
    order_metadata = SingleTableMetadata()
    order_metadata.detect_from_dataframe(order_dataset)
    order_metadata.update_column(column_name='order_id', sdtype='md5')
    order_synthesizer = GaussianCopulaSynthesizer(order_metadata)
    order_synthesizer.fit(order_dataset)
    del order_dataset

    payment_dataset = pd.read_csv('data/payment_dataset.csv')
    payment_dataset.drop(['order_id'], axis=1, inplace=True)
    payment_metadata = SingleTableMetadata()
    payment_metadata.detect_from_dataframe(payment_dataset)
    payment_synthesizer = GaussianCopulaSynthesizer(payment_metadata)
    # custom constraint to make sure that only credit cards has payment_installments > 1
    payment_synthesizer.load_custom_constraint_classes(filepath='customConstraint.py', class_names=['InstallmentsConstraintClass'])
    my_constraint = {
        'constraint_class': 'InstallmentsConstraintClass',
        'constraint_parameters': {
            'column_names': ['payment_type','payment_installments'],
            'extra_parameter': None
        }
    }
    payment_synthesizer.add_constraints(constraints=[my_constraint])
    payment_synthesizer.fit(payment_dataset)
    del payment_dataset

    order_item_dataset = pd.read_csv('data/order_item_dataset.csv')
    order_item_dataset.drop(['order_id','order_item_id','product_id','seller_id'], axis=1, inplace=True)
    order_item_metadata = SingleTableMetadata()
    order_item_metadata.detect_from_dataframe(order_item_dataset)
    order_item_synthesizer = GaussianCopulaSynthesizer(order_item_metadata)
    order_item_synthesizer.fit(order_item_dataset)
    del order_item_dataset

    feedback_dataset = pd.read_csv('data/feedback_dataset.csv')
    feedback_dataset.drop(['order_id'], axis=1, inplace=True)
    feedback_dataset = feedback_dataset.groupby('feedback_id').first().reset_index()
    feedback_metadata = SingleTableMetadata()
    feedback_metadata.detect_from_dataframe(feedback_dataset)
    feedback_metadata.update_column(column_name='feedback_id', sdtype='md5')
    feedback_metadata.set_primary_key(column_name='feedback_id')
    feedback_synthesizer = GaussianCopulaSynthesizer(feedback_metadata)
    feedback_synthesizer.fit(feedback_dataset)
    del feedback_dataset

    print('Synthesizers created')
    return product_synthesizer, user_synthesizer, seller_synthesizer, order_synthesizer, payment_synthesizer, \
           order_item_synthesizer, feedback_synthesizer



def GenerateProductsUsersSellers(product_synthesizer, user_synthesizer, seller_synthesizer, records_no):
    # Generate the users, sellers and product tables individually
    print('Generating Products, Users and Sellers ...')
    synthetic_product_dataset = product_synthesizer.sample(num_rows=records_no)
    synthetic_user_dataset = user_synthesizer.sample(num_rows=records_no)
    synthetic_seller_dataset = seller_synthesizer.sample(num_rows=records_no)
    print('Products, Users and Sellers are generated')
    return synthetic_product_dataset, synthetic_user_dataset, synthetic_seller_dataset


def GenerateOrders(order_synthesizer,np_username, records_no):
    # Genrate orders table as a single table and get the user_name forigen key from the synthetic_user_dataset directly 
    # instead of generating it
    print('Generating Orders ...')
    synthetic_order_dataset = order_synthesizer.sample(num_rows=records_no)
    np_username = np.random.choice(np_username, records_no, replace=True)
    synthetic_order_dataset['user_name'] = np_username.tolist()
    print('Orders generated')
    return synthetic_order_dataset


def GenerateFeedbacks(feedback_synthesizer, np_order_id, records_no):
    # Genrate payments table as a single table and get the order_id forigen key from the synthetic_order_dataset directly 
    # instead of generating it
    print('Generating Feedbacks ...')
    synthetic_feedback_dataset = feedback_synthesizer.sample(num_rows=records_no)
    synthetic_feedback_dataset['order_id'] = np_order_id.tolist()
    print('Feedbacks generated')
    return synthetic_feedback_dataset


def GeneratePayments(payment_synthesizer, np_order_id, records_no):
    # Genrate payments table as a single table and get the order_id forigen key from the synthetic_order_dataset directly 
    # instead of generating it
    print('Generating Payments ...')
    synthetic_payment_dataset = payment_synthesizer.sample(num_rows=records_no)
    synthetic_payment_dataset['order_id'] = np_order_id.tolist()
    print('Payments generated')
    return synthetic_payment_dataset


def GenerateOrder_Items(order_item_synthesizer, np_order_id, np_seller_id, np_product_id, records_no):
    ''' 
    Generate Order_Item table manually due to the multiple conditions that must be applied:
    1- order_id and seller_id are directly related such that an order can't have more than 1 seller
    2- order_item_id depends on the repeatation of the order_id
    '''
    print('Generating Order Items ...')
    synthetic_order_item_dataset = order_item_synthesizer.sample(num_rows=records_no)

    np_order_id = np.sort(np.random.choice(np_order_id, records_no, replace=True))
    np_seller_id = np.random.choice(np_seller_id, records_no, replace=True)
    np_product_id = np.random.choice(np_product_id, records_no, replace=True)

    rows = []
    prev_order_id = 0
    prev_order_item_id = 0
    prev_seller_id = ''
    for i in range(records_no):
        dict1 = {}
        dict1['order_id'] = order_id = np_order_id[i]
        dict1['product_id'] = np_product_id[i]
        if order_id == prev_order_id :
            dict1['order_item_id'] = prev_order_item_id + 1
            dict1['seller_id'] = prev_seller_id
            prev_order_item_id = prev_order_item_id + 1
        else:
            dict1['order_item_id'] = prev_order_item_id = 1
            dict1['seller_id'] = prev_seller_id = np_seller_id[i]
            prev_order_id = order_id
        rows.append(dict1)
    df = pd.DataFrame(rows)
    synthetic_order_item_dataset = pd.concat([df,synthetic_order_item_dataset],axis=1)
    print('Order Items generated')
    return synthetic_order_item_dataset


def ConnectDB():
    print('Connecting to Database ...')
    try:
        connection = connect(host="db", user='root', password='root')
        print('Connected to Database successfully')
        return connection
    except Error as e:
        print(e)
        return None



def InsertProductsUsersSellers(connection, synthetic_product_dataset, synthetic_user_dataset, synthetic_seller_dataset):
    print('Inserting Products, Users and Sellers ...')
    cursor = connection.cursor()
    product_insert_query = '''
    INSERT INTO online_store.Products (product_id, product_category, product_name_length, 
        product_description_length, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
    '''
    user_insert_query = '''
    INSERT INTO online_store.Users (user_name, customer_zip_code, customer_city, customer_state)
    VALUES (%s,%s,%s,%s)
    '''
    seller_insert_query = '''
    INSERT INTO online_store.Sellers (seller_id, seller_zip_code, seller_city, seller_state)
    VALUES (%s,%s,%s,%s)
    '''

    synthetic_product_dataset = synthetic_product_dataset.replace(np.nan, None)
    cursor.executemany(product_insert_query,list(synthetic_product_dataset.itertuples(index=False)))
    synthetic_user_dataset = synthetic_user_dataset.replace(np.nan, None)
    cursor.executemany(user_insert_query,list(synthetic_user_dataset.itertuples(index=False)))
    synthetic_seller_dataset = synthetic_seller_dataset.replace(np.nan, None)
    cursor.executemany(seller_insert_query,list(synthetic_seller_dataset.itertuples(index=False)))

    connection.commit()
    print('Products, Users and Sellers are inserted')



def InsertOrdersFeedbacks(connection, synthetic_order_dataset, synthetic_feedback_dataset):
    print('Inserting Orders and Feedbacks ...')
    cursor = connection.cursor()
    order_insert_query = '''
    INSERT INTO online_store.Orders (order_id, order_status, order_date, order_approved_date, 
        pickup_date, delivered_date, estimated_time_delivery, user_name)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    '''
    feedback_insert_query = '''
    INSERT INTO online_store.Feedbacks (feedback_id, feedback_score, feedback_form_sent_date, feedback_answer_date, order_id)
    VALUES (%s,%s,%s,%s,%s)
    '''

    synthetic_order_dataset = synthetic_order_dataset.replace(np.nan, None)
    cursor.executemany(order_insert_query,list(synthetic_order_dataset.itertuples(index=False)))
    synthetic_feedback_dataset = synthetic_feedback_dataset.replace(np.nan, None)
    cursor.executemany(feedback_insert_query,list(synthetic_feedback_dataset.itertuples(index=False)))
    connection.commit()
    print('Orders and Feedbacks inserted')

def InsertPayments(connection, synthetic_payment_dataset):
    print('Inserting Payments ...')
    cursor = connection.cursor()
    payment_insert_query = '''
    INSERT INTO online_store.Payments (payment_sequential, payment_type, payment_installments, payment_value, order_id)
    VALUES (%s,%s,%s,%s,%s)
    '''
    synthetic_payment_dataset = synthetic_payment_dataset.replace(np.nan, None)
    cursor.executemany(payment_insert_query,list(synthetic_payment_dataset.itertuples(index=False)))
    connection.commit()
    print('Payments inserted')

def InsertOrder_Items(connection, synthetic_order_item_dataset):
    print('Inserting Order Items ...')
    cursor = connection.cursor()
    order_item_insert_query = '''
    INSERT INTO online_store.Order_Items (order_id, product_id, order_item_id, seller_id, pickup_limit_date, price, shipping_cost)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    '''
    synthetic_order_item_dataset = synthetic_order_item_dataset.replace(np.nan, None)
    cursor.executemany(order_item_insert_query,list(synthetic_order_item_dataset.itertuples(index=False)))
    connection.commit()
    print('Order Items inserted')

def main():

    connection = ConnectDB()
    # Remove data if exists to avoid voilating primary key contraints
    RemoveData(connection)

    if connection:
        product_synthesizer, user_synthesizer, seller_synthesizer, order_synthesizer, payment_synthesizer, \
           order_item_synthesizer, feedback_synthesizer = CreateSynthesizers()
        np_username = np.array([])
        np_order_id = np.array([])
        np_seller_id = np.array([])
        np_product_id = np.array([])
        records_num = 300
        for i in range(20):
            synthetic_product_dataset, synthetic_user_dataset, synthetic_seller_dataset = \
                GenerateProductsUsersSellers(product_synthesizer,user_synthesizer,seller_synthesizer,records_num)
            
            np_username = np.append(np_username, synthetic_user_dataset['user_name'].values)
            np_seller_id = np.append(np_seller_id, synthetic_seller_dataset['seller_id'].values)
            np_product_id = np.append(np_product_id, synthetic_product_dataset['product_id'].values)
            
            InsertProductsUsersSellers(connection,synthetic_product_dataset, synthetic_user_dataset, synthetic_seller_dataset)
            
            synthetic_order_dataset = GenerateOrders(order_synthesizer,np_username,records_num *2)
            np_order_id = np.append(np_order_id, synthetic_order_dataset['order_id'].values)
            synthetic_feedback_dataset = GenerateFeedbacks(feedback_synthesizer,\
                                                           synthetic_order_dataset['order_id'].values,records_num *2)
            InsertOrdersFeedbacks(connection,synthetic_order_dataset,synthetic_feedback_dataset)
            
            synthetic_payment_dataset = GeneratePayments(payment_synthesizer, \
                                                         synthetic_order_dataset['order_id'].values, records_num *2)
            InsertPayments(connection,synthetic_payment_dataset)

            synthetic_order_item_dataset = GenerateOrder_Items(order_item_synthesizer,np_order_id, \
                                                               np_seller_id, np_product_id, records_num *10)
            InsertOrder_Items(connection,synthetic_order_item_dataset)
            DataSize('Size of user_name structure in memory',np_username)
            DataSize('Size of product_id structure in memory',np_product_id)
            DataSize('Size of seller structure in memory',np_seller_id)
            DataSize('Size of order structure in memory',np_order_id)
            DataSize('Size of products data inserted into the database',synthetic_product_dataset)
            DataSize('Size of users data inserted into the database',synthetic_user_dataset)
            DataSize('Size of sellers data inserted into the database',synthetic_seller_dataset)
            DataSize('Size of orders data inserted into the database',synthetic_order_dataset)
            DataSize('Size of feedbacks data inserted into the database',synthetic_feedback_dataset)
            DataSize('Size of payments data inserted into the database',synthetic_payment_dataset)
            DataSize('Size of order items data inserted into the database',synthetic_order_item_dataset)
            print('Loop iteration ' + str(i) + ' finished')

main()