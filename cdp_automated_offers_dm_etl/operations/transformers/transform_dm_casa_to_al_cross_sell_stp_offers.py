from xflow.task import TaskResultType
from xnl.table_builder import Table
from pyspark.sql import DataFrame

from cdp_automated_offers_dm_etl.operations.utils.rule_loader import load_rules_from_s3
from cdp_automated_offers_dm_etl.operations.utils.rule_engine import apply_rules

def create_dynamic_rule_filter(rules_file: str, s3_path_key: str = "spark_app_rules_path"):
    def _apply_rules(df: DataFrame) -> DataFrame:
        print(f"[RULE_ENGINE] Using rules file: {rules_file}")
        rules = load_rules_from_s3(s3_path_key, rules_file)
        return apply_rules(df, rules)
    return _apply_rules

transform_dm_casa_to_al_cross_sell_stp_offers = (
    Table.transformer()
    .with_filename("transform_dm_casa_to_al_cross_sell_stp_offers")
    .with_result_name("interim_df")
    .with_result_type(TaskResultType.DATAFRAME)
    .add_post_processing(
        # ONLY CHANGE THIS STRING FOR EACH PYTHON FILE
        create_dynamic_rule_filter(rules_file="dm_casa_to_al_cross_sell_stp_offers.json") 
    )
)