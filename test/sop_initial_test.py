import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, FloatType
from pyspark.sql.functions import col, datediff, current_date, to_date
from src.sop_initial_load_hive import transform_data, save_to_hive


# Fixture to create the SparkSession
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local").appName("UnitTest").getOrCreate()
    yield spark
    spark.stop()


# Create a test class with the setUp method as fixture:
class TestSopInitialLoadHive:

    @pytest.fixture(autouse=True)
    def setUp(self, spark):
        # Create a sample DataFrame with the new schema
        data = [
            (1, "2023-01-01 12:34:56", 1234567890123456, "Merchant A", "shopping", 100.50, "John", "Doe", "M", "1234 Elm St", "Springfield", "IL", "62701", 39.7817, -89.6501, 116250, "Engineer", "1980-05-14", 1000001, 1672571696, 39.7817, -89.6501, 0),
            (2, "2023-01-02 13:45:12", 9876543210987654, "Merchant B", "groceries", 50.75, "Jane", "Smith", "F", "5678 Oak St", "Chicago", "IL", "60601", 41.8781, -87.6298, 271485, "Manager", "1990-08-19", 1000002, 1672574312, 41.8781, -87.6298, 1),
            (3, "2023-06-15 10:00:00", None, "Merchant C", "electronics", 300.25, "Alice", "Brown", "F", "1357 Maple St", "Peoria", "IL", "61602", 40.6936, -89.5887, 113150, "Data Scientist", "1985-12-10", 1000003, 1676310000, 40.6936, -89.5887, 0)
        ]

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("trans_date_trans_time", StringType(), True),
            StructField("cc_num", LongType(), True),
            StructField("merchant", StringType(), True),
            StructField("category", StringType(), True),
            StructField("amt", FloatType(), True),
            StructField("first", StringType(), True),
            StructField("last", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip", StringType(), True),
            StructField("lat", FloatType(), True),
            StructField("long", FloatType(), True),
            StructField("city_pop", IntegerType(), True),
            StructField("job", StringType(), True),
            StructField("dob", StringType(), True),
            StructField("trans_num", IntegerType(), True),
            StructField("unix_time", LongType(), True),
            StructField("merch_lat", FloatType(), True),
            StructField("merch_long", FloatType(), True),
            StructField("is_fraud", IntegerType(), True)
        ])

        self.df = spark.createDataFrame(data, schema)

    def test_transform_fillna_category(self):
        """Test the fillna transformation for the 'category' column."""
        df_transformed = transform_data(self.df)
        
        # Check that the 'category' column is filled with 'travel' where it's None
        result = df_transformed.collect()
        assert result[2]["category"] == "travel"  # Third row had a None value for 'category'
        assert result[0]["category"] == "shopping"
        assert result[1]["category"] == "groceries"

    def test_drop_column(self):
        """Test dropping the 'cc_num' column."""
        df_transformed = transform_data(self.df)

        # Assert that the 'cc_num' column is dropped
        assert "cc_num" not in df_transformed.columns

    def test_rename_columns(self):
        """Test renaming of columns."""
        df_transformed = transform_data(self.df)

        # Assert that the columns are renamed correctly
        assert "first_name" in df_transformed.columns
        assert "last_name" in df_transformed.columns
        assert "population" in df_transformed.columns

    def test_age_calculation(self):
        """Test age calculation based on 'dob'."""
        df_transformed = transform_data(self.df)

        # Collect results and check if the 'Age' column is calculated
        result = df_transformed.collect()
        assert isinstance(result[0]["Age"], int)  # Age should be an integer
        assert result[0]["Age"] >= 0  # Age should be non-negative

    def test_save_to_hive(self, spark):
        """Test saving data to Hive."""
        df_transformed = transform_data(self.df)

        # Here we are testing if the save_to_hive function executes without errors.
        # For testing purposes, we'll check if the function is called (mocked in an actual environment).
        try:
            save_to_hive(df_transformed)
            assert True  # If no error occurs, test passes.
        except Exception as e:
            pytest.fail(f"Save to Hive failed: {str(e)}")

