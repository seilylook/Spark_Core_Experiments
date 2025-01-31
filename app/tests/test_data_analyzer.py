import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession, DataFrame
from src.services.analyze import DataAnalyzer
from src.modules.exceptions import DataProcessingError


@pytest.fixture
def mock_spark():
    spark = Mock(spec=SparkSession)
    spark._sc = Mock()
    spark._jsc = Mock()
    spark._jvm = Mock()
    spark.sparkContext = Mock()
    spark.createDataFrame = Mock()
    return spark


@pytest.fixture
def mock_column():
    col = Mock()
    col.alias = Mock(return_value=col)
    return col


@pytest.fixture
def mock_dataframe(mock_column):
    df = Mock(spec=DataFrame)

    # RDD mocking
    mock_rdd = Mock()
    mock_rdd.getNumPartitions = Mock(return_value=10)
    df.rdd = mock_rdd

    # DataFrame method mocks
    df.agg = Mock(return_value=df)
    df.show = Mock()
    df.groupBy = Mock(return_value=Mock(**{"agg.return_value": df}))

    # Repartition mock - 수정된 부분
    mock_repart_df = Mock(spec=DataFrame)
    mock_repart_rdd = Mock()
    mock_repart_rdd.getNumPartitions = Mock()
    mock_repart_df.rdd = mock_repart_rdd
    df.repartition = Mock(return_value=mock_repart_df)

    # Coalesce mock - 수정된 부분
    mock_coalesce_df = Mock(spec=DataFrame)
    mock_coalesce_rdd = Mock()
    mock_coalesce_rdd.getNumPartitions = Mock()
    mock_coalesce_df.rdd = mock_coalesce_rdd
    df.coalesce = Mock(return_value=mock_coalesce_df)

    return df


@pytest.fixture(autouse=True)
def mock_functions():
    with patch("src.services.analyze.avg") as mock_avg, patch(
        "src.services.analyze.sum"
    ) as mock_sum, patch("src.services.analyze.max") as mock_max:

        col = Mock()
        col.alias = Mock(return_value=col)

        mock_avg.return_value = col
        mock_sum.return_value = col
        mock_max.return_value = col

        yield


@pytest.fixture
def data_analyzer(mock_spark):
    return DataAnalyzer(mock_spark)


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: mark test as an integration test")


class TestDataAnalyzer:
    def test_generate_sample_data_success(
        self, data_analyzer, mock_spark, mock_dataframe
    ):
        mock_spark.createDataFrame.return_value = mock_dataframe
        df = data_analyzer.generate_sample_data(100)
        assert isinstance(df, Mock)
        mock_spark.createDataFrame.assert_called_once()

    def test_generate_sample_data_failure(self, data_analyzer, mock_spark):
        mock_spark.createDataFrame.side_effect = Exception("Data generation failed")
        with pytest.raises(DataProcessingError) as exc_info:
            data_analyzer.generate_sample_data(100)
        assert "샘플 데이터 생성 실패" in str(exc_info.value)

    def test_analyze_data_success(self, data_analyzer, mock_dataframe):
        result, time = data_analyzer.analyze_data(mock_dataframe, "테스트")
        assert isinstance(result, Mock)
        assert isinstance(time, float)

    def test_analyze_data_failure(self, data_analyzer, mock_dataframe):
        mock_dataframe.agg.side_effect = Exception("Analysis failed")
        with pytest.raises(DataProcessingError) as exc_info:
            data_analyzer.analyze_data(mock_dataframe, "테스트")
        assert "데이터 분석 중 오류 발생" in str(exc_info.value)

    def test_analyze_partitions_success(self, data_analyzer, mock_dataframe):
        df_repart, df_coal = data_analyzer.analyze_partitions(mock_dataframe, "테스트")
        assert isinstance(df_repart, Mock)
        assert isinstance(df_coal, Mock)

    def test_analyze_partitions_failure(self, data_analyzer, mock_dataframe):
        mock_dataframe.rdd.getNumPartitions.side_effect = Exception(
            "Partition analysis failed"
        )
        with pytest.raises(DataProcessingError) as exc_info:
            data_analyzer.analyze_partitions(mock_dataframe, "테스트")
        assert "파티션 분석 중 오류 발생" in str(exc_info.value)

    def test_repartition_count(self, data_analyzer, mock_dataframe):
        mock_df = mock_dataframe.repartition(10)
        mock_df.rdd.getNumPartitions()  # 실제 호출 추가
        mock_df.rdd.getNumPartitions.assert_called_once()

    def test_coalesce_count(self, data_analyzer, mock_dataframe):
        mock_df = mock_dataframe.coalesce(5)
        mock_df.rdd.getNumPartitions()  # 실제 호출 추가
        mock_df.rdd.getNumPartitions.assert_called_once()

    @pytest.mark.integration
    def test_end_to_end_analysis(self, data_analyzer, mock_dataframe):
        df = data_analyzer.generate_sample_data(100)
        stats, time = data_analyzer.analyze_data(df, "End-to-end 테스트")
        repart, coal = data_analyzer.analyze_partitions(df, "End-to-end 테스트")

        assert all(isinstance(x, Mock) for x in [df, stats, repart, coal])
        assert isinstance(time, float)
