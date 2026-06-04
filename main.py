import sys

from xflow.job import JobManager

from cdp_automated_offers_dm_etl import jobs
from cdp_automated_offers_dm_etl.config.config import Config

if __name__ == "__main__":
    JobManager(
        additional_allowed_parameters=["pipeline_name"],
        config=Config().set_config(sys.argv[1]),
        job_modules=jobs,
        global_run_parameters=sys.argv[1],
    ).run()
