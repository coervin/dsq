from typing import List, Union

from pyspark.sql import DataFrame, Column
import pyspark.sql.functions as F


def mobile_col_to_clean(column_name: str) -> Column:

    # Remove non-digit characters if length of digits is between 10 and 12
    digits_only = F.regexp_replace(column_name, r"\D", "")
    cleaned_col = F.when(F.length(digits_only).between(10, 12), digits_only).otherwise(F.lit(None))

    # Remove numbers with 8 or more repeated digits
    cleaned_col = F.when(cleaned_col.rlike(r"(\d)\1{8,}$"), F.lit(None)).otherwise(cleaned_col)

    # Normalize valid Philippine numbers to format starting with '63'
    cleaned_col = (
        F.when(
            (F.length(cleaned_col) == 10) & (F.substring(cleaned_col, 1, 1).isin("8", "9")),
            F.concat(F.lit("63"), cleaned_col),
        )
        .when(
            (F.length(cleaned_col) == 11) & (F.substring(cleaned_col, 1, 2).isin("08", "09")),
            F.concat(F.lit("63"), F.substring(cleaned_col, 2, 10)),
        )
        .when((F.length(cleaned_col) == 12) & (F.substring(cleaned_col, 1, 3).isin("638", "639")), cleaned_col)
        .otherwise(F.lit(None))
    )

    return cleaned_col


def landline_col_to_clean(column_name: str) -> Column:

    # Remove non-digit characters and filter landlines with lengths from 8 to 11
    digits_only = F.regexp_replace(column_name, r"\D", "")
    cleaned_col = F.when(F.length(digits_only).between(8, 11), digits_only).otherwise(F.lit(None))

    # Normalize landline numbers
    cleaned_col = (
        # Prepend "02" if landline is from Metro Manila
        F.when(
            F.length(cleaned_col) == 8,
            F.concat(F.lit("02"), cleaned_col),
        )
        # Prepend "0" if landline is from Metro Manila
        .when(
            (F.length(cleaned_col) == 9) & (F.substring(cleaned_col, 1, 1) == "2"),
            F.concat(F.lit("0"), cleaned_col),
        )
        # Remove "0" if landline is not from Metro Manila
        .when(
            (
                (F.length(cleaned_col) == 10)
                & (F.substring(cleaned_col, 1, 1) == "0")
                & (F.substring(cleaned_col, 2, 1) != "2")
            ),
            F.substring(cleaned_col, 2, 10),
        )
        # Remove "63" and prepend "0" if landline is from Metro Manila
        .when(
            (
                (F.length(cleaned_col) == 11)
                & (F.substring(cleaned_col, 1, 2) == "63")
                & (F.substring(cleaned_col, 3, 1) == "2")
            ),
            F.concat(F.lit("0"), F.substring(cleaned_col, 3, 11)),
        )
        # Remove "63" if landline is not from Metro Manila
        .when(
            (
                (F.length(cleaned_col) == 11)
                & (F.substring(cleaned_col, 1, 2) == "63")
                & (F.substring(cleaned_col, 3, 1) != "2")
            ),
            F.substring(cleaned_col, 3, 11),
        )
    )

    # Filter valid landline numbers
    cleaned_col = (
        # Select Metro Manila landlines
        F.when(
            (F.length(cleaned_col) == 10) & (F.substring(cleaned_col, 1, 3).isin("023", "025", "026", "027", "028")),
            cleaned_col,
        )
        # Select provincial landlines
        .when(
            (
                (F.length(cleaned_col) == 9)
                & (
                    F.substring(cleaned_col, 1, 2).isin(
                        "32",
                        "33",
                        "34",
                        "35",
                        "36",
                        "38",
                        "42",
                        "43",
                        "44",
                        "45",
                        "46",
                        "47",
                        "48",
                        "49",
                        "52",
                        "53",
                        "54",
                        "55",
                        "56",
                        "62",
                        "63",
                        "64",
                        "65",
                        "68",
                        "72",
                        "74",
                        "75",
                        "77",
                        "78",
                        "82",
                        "83",
                        "84",
                        "85",
                        "86",
                        "87",
                        "88",
                    )
                )
            ),
            cleaned_col,
        ).otherwise(F.lit(None))
    )

    return cleaned_col


def clean_mobile_num(dataframe: DataFrame, mobile_column: Union[List[str], str]) -> DataFrame:

    if isinstance(mobile_column, str):
        mobile_column = [mobile_column]

    for column_name in mobile_column:
        dataframe = dataframe.withColumn(column_name, mobile_col_to_clean(column_name))

    return dataframe


def clean_landline_num(dataframe: DataFrame, landline_column: Union[List[str], str]) -> DataFrame:

    if isinstance(landline_column, str):
        landline_column = [landline_column]

    for column_name in landline_column:
        dataframe = dataframe.withColumn(column_name, landline_col_to_clean(column_name))

    return dataframe
