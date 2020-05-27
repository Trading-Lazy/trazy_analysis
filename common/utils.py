from django.db.models.query import QuerySet
from django.forms import model_to_dict
from pandas import DataFrame


def validate_dataframe_columns(df: DataFrame, required_columns: list):
    sorted_required_columns = sorted(required_columns)
    sorted_columns = sorted(df.columns.tolist())
    if sorted_columns != sorted_required_columns:
        raise Exception(
            "The input dataframe is malformed. It must contain only columns: {} but has instead {}".format(
                required_columns, df.columns
            )
        )
