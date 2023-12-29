from sdv.datasets.local import load_csvs
from sdv.metadata import MultiTableMetadata
from sdv.multi_table import HMASynthesizer

# assume that my_folder contains many CSV files
datasets = load_csvs(folder_name='data/')

metadata = MultiTableMetadata()
metadata.detect_from_dataframes(datasets)

metadata.update_column(table_name='feedback_dataset', column_name='feedback_id', sdtype='id', regex_format='\w{32}')
metadata.update_column(table_name='feedback_dataset', column_name='feedback_score', sdtype='numerical')
metadata.set_primary_key(table_name='feedback_dataset', column_name='feedback_id')

metadata.update_column(table_name='order_dataset', column_name='user_name', sdtype='id', regex_format='\w{32}')

metadata.update_column(table_name='user_dataset', column_name='user_name', sdtype='id', regex_format='\w{32}')
metadata.set_primary_key(table_name='user_dataset', column_name='user_name')

metadata.add_relationship(parent_table_name='user_dataset', child_table_name='order_dataset', parent_primary_key='user_name', child_foreign_key='user_name')


dataset1 = datasets['user_dataset'].groupby('user_name').first().reset_index()
print(datasets['user_dataset'].count())
print('===========')
print(dataset1.count())
datasets['user_dataset'] = dataset1
datasets['feedback_dataset'] = datasets['feedback_dataset'].groupby('feedback_id').first().reset_index()
#print(metadata.to_dict())

synthesizer = HMASynthesizer(metadata)

synthesizer.load_custom_constraint_classes(filepath='customConstraint.py',class_names=['InstallmentsConstraintClass'])
my_constraint = {
    'constraint_class': 'InstallmentsConstraintClass',
    'table_name': 'payment_dataset',
    'constraint_parameters': {
        'column_names': ['payment_type','payment_installments'],
        'extra_parameter': None
    }
}
synthesizer.add_constraints([my_constraint])

synthesizer.fit(datasets)

synthetic_data = synthesizer.sample(num_rows=300,folder_name='synthetic_data2/')
for name,data in synthetic_data.items():
    data.to_csv('synthetic_data2/'+name+'.csv')