FROM registry.sbonline.ph/cdp/pyspark_tests:spark-3.0.1-hadoop-3.2-python3.8.7-cdp0.4

USER root
COPY . ./src
WORKDIR ./src
RUN mkdir /tmp/test
RUN chmod -R 775 /tmp/test
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --trusted-host nexus.sbonline.ph -r requirements.txt
RUN python -m pytest -ra -vv --cov=cdp_exponea_dm_etl/ ./cdp_exponea_dm_etl/tests/ --junitxml=report.xml
RUN coverage xml
RUN flake8 ./ --output-file flake8.txt || exit 0
RUN flake8_junit flake8.txt flake8_junit.xml
RUN cat flake8_junit.xml