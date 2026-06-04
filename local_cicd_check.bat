black --line-length=120 --target-version=py38 .
flake8 --max-line-length=120 --max-complexity=10 .
pylint --output-format=colorized --recursive=true --ignore-patterns=.*tests.* .
bandit -r -x */tests/* .
pytest --cov=. --cov-report=term-missing tests/