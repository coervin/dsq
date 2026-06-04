from xflow.task import TaskResultType
from xnl.table_builder import Table

from cdp_automated_offers_dm_etl.operations.utils.rule_loader import load_rules_from_s3
from cdp_automated_offers_dm_etl.operations.utils.rule_engine import generate_sql_rules

def get_dynamic_sql_condition(rules_file: str, s3_path_key: str = "spark_app_rules_path") -> str:
    """
    Fetches JSON from S3 and converts it directly into a SQL WHERE string.
    """
    print(f"[RULE_ENGINE] Fetching and parsing rules file: {rules_file}")
    rules_dict = load_rules_from_s3(s3_path_key, rules_file)
    return generate_sql_rules(rules_dict)

transform_dm_casa_to_hl_cross_sell_ptb_offers = (
    Table.transformer()
    .with_filename("transform_dm_casa_to_hl_cross_sell_ptb_offers")
    .with_result_name("interim_df")
    .with_result_type(TaskResultType.DATAFRAME)
    # Pass the generated SQL string as a parameter to inject into the .sql file
    .with_params(
        dynamic_json_rules=get_dynamic_sql_condition(rules_file="dm_casa_to_hl_cross_sell_ptb_offers.json") 
    )
)