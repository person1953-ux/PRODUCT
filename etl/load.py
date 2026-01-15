# etl/load.py
from Model import Model  # absolute import

def load_author(data, config):
    """
    Load a single author record into the 'authors' table.
    """
    # call the Model class to insert data
    model = Model(table_name="authors", data=data, **config)
    model.save()

