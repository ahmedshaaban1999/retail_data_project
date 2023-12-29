import pandas as pd
import numpy as np
from sdv.metadata import SingleTableMetadata
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.datasets.local import load_csvs
from sdv.metadata import MultiTableMetadata
from sdv.multi_table import HMASynthesizer
#from faker import faker.providers.date_time

print('Generating Synthetic data ...')

# Generate the users, sellers and product tables individually
print('Generating Products Data ...')
product_dataset = pd.read_csv('data/product_dataset.csv')
product_metadata = SingleTableMetadata()
product_metadata.detect_from_dataframe(product_dataset)
product_metadata.update_column(column_name='product_id', sdtype='md5')
product_synthesizer = GaussianCopulaSynthesizer(product_metadata)
product_synthesizer.fit(product_dataset)
synthetic_product_dataset = product_synthesizer.sample(num_rows=300,output_file_path='synthetic_data/product_dataset.csv')
del product_dataset
print('Products Data Generated')

print('Generating Users Data ...')
user_dataset = pd.read_csv('data/user_dataset.csv')
user_metadata = SingleTableMetadata()
user_metadata.detect_from_dataframe(user_dataset)
user_metadata.update_column(column_name='user_name', sdtype='md5')
user_synthesizer = GaussianCopulaSynthesizer(user_metadata)
user_synthesizer.fit(user_dataset)
synthetic_user_dataset = user_synthesizer.sample(num_rows=300,output_file_path='synthetic_data/users_dataset.csv')
del user_dataset
print('Users Data Generated')

print('Generating Sellers Data ...')
seller_dataset = pd.read_csv('data/seller_dataset.csv')
seller_metadata = SingleTableMetadata()
seller_metadata.detect_from_dataframe(seller_dataset)
seller_metadata.update_column(column_name='seller_id', sdtype='md5')
seller_synthesizer = GaussianCopulaSynthesizer(seller_metadata)
seller_synthesizer.fit(seller_dataset)
synthetic_seller_dataset = seller_synthesizer.sample(num_rows=300,output_file_path='synthetic_data/sellers_dataset.csv')
del seller_dataset
print('Sellers Data Generated')

# Genrate orders table as a single table and get the user_name forigen key from the synthetic_user_dataset directly instead of generating it
print('Generating Orders Data ...')
records_no = 300
order_dataset = pd.read_csv('data/order_dataset.csv')
order_dataset.drop(['user_name'], axis=1, inplace=True)
order_metadata = SingleTableMetadata()
order_metadata.detect_from_dataframe(order_dataset)
order_metadata.update_column(column_name='order_id', sdtype='md5')
order_synthesizer = GaussianCopulaSynthesizer(order_metadata)
order_synthesizer.fit(order_dataset)
synthetic_order_dataset = order_synthesizer.sample(num_rows=records_no)

np_username = synthetic_user_dataset['user_name'].values
np_username = np.random.choice(np_username, records_no, replace=True)
synthetic_order_dataset['user_name'] = np_username.tolist()
synthetic_order_dataset.to_csv('synthetic_data/order_dataset.csv', index=False)
print('Orders Data Generated')

# Genrate payments table as a single table and get the order_it forigen key from the synthetic_order_dataset directly instead of generating it
print('Generating Payments Data ...')
records_no = 300
payment_dataset = pd.read_csv('data/payment_dataset.csv')
payment_dataset.drop(['order_id'], axis=1, inplace=True)
payment_metadata = SingleTableMetadata()
payment_metadata.detect_from_dataframe(payment_dataset)
payment_synthesizer = GaussianCopulaSynthesizer(payment_metadata)

#custome constraint to make sure that only credit cards has payment_installments > 1
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
synthetic_payment_dataset = payment_synthesizer.sample(num_rows=records_no)

np_order_id = synthetic_order_dataset['order_id'].values
synthetic_payment_dataset['order_id'] = np_order_id.tolist()
synthetic_payment_dataset.to_csv('synthetic_data/payment_dataset.csv', index=False)
print('Payments Data Generated')

''' 
Generate Order_Item table manually due to the multiple conditions that must be applied:
1- order_id and seller_id are directly related such that an order can't have more than 1 seller
2- order_item_id depends on the repeatation of the order_id
'''
print('Generating Order Items Data ...')
records_no = 500
order_item_dataset = pd.read_csv('data/order_item_dataset.csv')
order_item_dataset.drop(['order_id','order_item_id','product_id','seller_id'], axis=1, inplace=True)
order_item_metadata = SingleTableMetadata()
order_item_metadata.detect_from_dataframe(order_item_dataset)
order_item_synthesizer = GaussianCopulaSynthesizer(order_item_metadata)
order_item_synthesizer.fit(order_item_dataset)
synthetic_order_item_dataset = order_item_synthesizer.sample(num_rows=records_no)

np_order_id = np.sort(np.random.choice(np_order_id, records_no, replace=True))
np_seller_id = synthetic_seller_dataset['seller_id'].values
np_seller_id = np.random.choice(np_seller_id, records_no, replace=True)
np_product_id = synthetic_product_dataset['product_id'].values
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
synthetic_order_item_dataset.to_csv('synthetic_data/order_item_dataset.csv', index=False)
print('Order Items Data Generated')