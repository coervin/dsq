from xnl.table_builder import Table
from xnl.table_builder.sql_utils import clean_all_str_columns

base_extract = Table.extractor().add_post_processing(clean_all_str_columns).reuse()
