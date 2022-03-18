from airflow.plugins_manager import AirflowPlugin
from operators.stage_redshift import *
from operators.load_dimension import *
from operators.load_fact import *
from operators.data_quality import *
from helpers.sql_queries import *
from helpers.unit_test import *


class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        StageToRedshiftOperator,
        LoadDimensionOperator,
        LoadFactOperator,
        DataQualityOperator,
    ]
    helpers = [
        SqlQueries,
        UnitTest,
    ]
