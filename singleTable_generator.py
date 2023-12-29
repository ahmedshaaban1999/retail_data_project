import pandas as pd
from sdv.metadata import SingleTableMetadata
from sdv.single_table import GaussianCopulaSynthesizer

dataset = pd.read_csv('products_dataset.csv')

metadata = SingleTableMetadata()
metadata.detect_from_dataframe(dataset)
metadata.update_column(column_name='product_id', sdtype='id', regex_format='\w{32}')

synthesizer = GaussianCopulaSynthesizer(metadata)
synthesizer.fit(dataset)
synthetic_data = synthesizer.sample(num_rows=300,output_file_path='synthetic_data.csv')