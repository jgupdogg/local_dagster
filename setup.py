from setuptools import find_packages, setup

setup(
    name="solana_pipeline",
    packages=find_packages(exclude=["solana_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-postgres",
        "pandas",
        "pyyaml",
        # Add other dependencies from your Airflow project
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)