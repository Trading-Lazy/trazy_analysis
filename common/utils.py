from pandas import DataFrame


def validate_dataframe_columns(df: DataFrame, required_columns: list):
    sorted_required_columns = sorted(required_columns)
    sorted_columns = sorted(list(df.columns))
    if sorted_columns != sorted_required_columns:
        raise Exception(
            'The input dataframe is malformed. It must contain only columns: {}'.format(required_columns))
