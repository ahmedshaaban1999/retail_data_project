import pandas as pd
from sdv.constraints import create_custom_constraint_class


def is_valid(column_names, data, extra_parameter):
    payment_method = column_names[0]
    no_of_installments = column_names[1]

    return (data[payment_method] == 'credit_card') | (data[no_of_installments] == 1)


def transform(column_names, data, extra_parameter):
    transformed_data = data
    return transformed_data


def reverse_transform(column_names, transformed_data, extra_parameter):
    reversed_data = transformed_data
    return reversed_data


InstallmentsConstraintClass = create_custom_constraint_class(is_valid_fn=is_valid)

### USE THE CONSTRAINT IN A SEPARATE FILE

# Step 1. Load the constraint
# synthesizer.load_custom_constraint_class(
#     filepath='custom_constraint_template.py'
#     class_names='MyCustomConstraintClass'
# )
   
# Step 2. Apply it
# my_constraint = {
#     'constraint_class': 'MyCustomConstraintClass',
#     'constraint_parameters': {
#         'column_names': [],
#         'extra_parameter': None
#     }
# }
# synthsizer.add_constraints([my_constraint])   