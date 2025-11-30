import pytest
import logging
from pathlib import Path
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    try:
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.ERROR)
        spark_session = SparkSession.builder.config(
            "spark.driver.memory", "2g"
        ).getOrCreate()
        yield spark_session
    except:
        pass
    finally:
        spark_session.stop()


@pytest.fixture(scope="session")
def users():
    path = Path(__file__).parent / "test" / "fixtures" / "users.csv"
    return str(path)