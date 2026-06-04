"""
This module contains all the sql utility functions that can be used in sql files loaded by :class:`~TableTransformer`.
"""

from typing import List

from pyspark.sql import Column
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

# The sql_* functions below are utilized dynamically by other classes .build() functions through getattr
#   i.e. getattr(sql_utils, field_transform)(field_name, **field_transform_args)
FLOAT_REGEX = "'([^0-9.-])'"


def sql_str_to_num(column_name: str, cast_type: str, precision: int = None, scale: int = None) -> str:
    """
    Convert a string column to a specified numerical type.

    :param column_name: The column name to convert.
    :param cast_type: The numerical type.
    :param precision: The precision of the number, applicable only to decimal type.
    :param scale: The scale of the number, applicable only to decimal type.
    :return:
    """
    if cast_type == "decimal":
        casted_type = f"DECIMAL({precision}, {scale})"
    else:
        casted_type = cast_type.upper()

    return f"CAST(REGEXP_REPLACE({column_name}, {FLOAT_REGEX}, '') AS {casted_type})"


def sql_rtrim_tab_and_space(column_name: str):
    """
    Removes the all whitespaces to the right of the string column.

    :param column_name: The column name to remove whitespaces from.
    :return:
    """
    return f'REGEXP_REPLACE({column_name}, "[\\t ]+$", "")'


def sql_ltrim_tab_and_space(column_name: str):
    """
    Removes the all whitespaces to the left of the string column.

    :param column_name: The column name to remove whitespaces from.
    :return:
    """
    return f'REGEXP_REPLACE({column_name}, "^[\\t ]+", "")'


def sql_trim_tab_and_space(column_name: str):
    """
    Removes the all whitespaces to the both ends of the string column.

    :param column_name: The column name to remove whitespaces from.
    :return:
    """
    return f'REGEXP_REPLACE(REGEXP_REPLACE({column_name}, "[\\t ]+$", ""), "^[\\t ]+", "")'


def sql_int_to_date(column_name: str):
    """
    Convert an integer column which is an epoch-based timestamp to a date formatted as yyyyMMdd.

    :param column_name: The column name to convert.
    :return:
    """
    return f"TO_DATE(CAST(({column_name} + 19000000) AS STRING),'yyyyMMdd')"


def sql_str_to_date(column_name: str):
    """
    Converts a string column to date formatted as MM/dd/yyyy.

    :param column_name: The column name to convert.
    :return:
    """
    return f"TO_DATE({column_name}, 'MM/dd/yyyy')"


def sql_military_date_int_to_date(column_name: str):
    """
    Convert an integer column to date formatted as MMddyyyy.

    :param column_name: The column name to convert.
    :return:
    """
    return f"""
        CASE
            WHEN {column_name} = 0 THEN NULL
            WHEN LENGTH(CAST({column_name} AS STRING)) < 8
                THEN TO_DATE(CONCAT('0', CAST({column_name} AS STRING)), 'MMddyyyy')
            ELSE TO_DATE(CAST({column_name} AS STRING), 'MMddyyyy')
        END
    """


def sql_timestamp_to_date(column_name: str):
    """
    Converts a timestamp-string column to date formatted as yyyy-MM-dd.

    :param column_name: The column name to convert.
    :return:
    """
    return f"TO_DATE(SUBSTRING(CAST({column_name} AS STRING), 0, 10), 'yyyy-MM-dd')"


def sql_empty_string_to_null(column_name: str):
    """
    Convert empty string values to null.

    :param column_name: The column name to convert.
    :return:
    """
    return f"""
        CASE
            WHEN {column_name} = '' THEN NULL
            ELSE {column_name}
        END
    """


def sql_safe_addition(column_name1: str, column_name2: str):
    """
    Adds to sql columns while taking into account null values.

    :param column_name1: The first column addend.
    :param column_name2: The second column addend.
    :return:
    """
    return f"""
    CASE
        WHEN {column_name1} IS NULL AND {column_name2} IS NULL THEN NULL
        ELSE COALESCE({column_name1}, 0) + COALESCE({column_name2}, 0)
    END
    """


def sql_truncate_field(column_name: str, limit: int):
    """
    Truncates the column to a specified length limit.

    :param column_name: The name of the column.
    :param limit: The maximum length of the string.
    :return:
    """
    return f"SUBSTRING({column_name}, 0, {limit})"


NA_VALUES = ["NAN", "NA", "N/A", "N.A.", "N.A", "NONE"]


def _clean_str_column(column: Column) -> Column:
    column = _trim_column(column)
    return F.when(column.isNull() | F.upper(column).isin(*NA_VALUES), F.lit("")).otherwise(column)


def _trim_column(column: Column) -> Column:
    return F.trim(F.regexp_replace(column, r"^(\u00A0|\s)+|(\u00A0|\s)+$", " "))


def clean_all_str_columns(
    dataframe: DataFrame, except_cols: List[str] = None, replace_na_to_empty_string: bool = True
) -> DataFrame:
    """
    Cleans all the string columns of a dataframe. Replaces null values to empty string by default. When
    `replace_na_to_empty_string` is set to False, it will instead replace multiple whitespaces to a single one.

    :param dataframe: The dataframe to clean.
    :param except_cols: The columns to exclude from cleaning.
    :param replace_na_to_empty_string: Whether to replace null values to an empty string or not.
    :return:
    """
    except_cols = except_cols or []
    except_cols = [column.lower() for column in except_cols]
    func = _clean_str_column if replace_na_to_empty_string else _trim_column

    cols = [
        func(F.col(field.name)).alias(field.name)
        if field.dataType == StringType() and field.name.lower() not in except_cols
        else field.name
        for field in dataframe.schema.fields
    ]

    return dataframe.select(cols)
