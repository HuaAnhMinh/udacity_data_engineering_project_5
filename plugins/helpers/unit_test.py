import logging
from helpers.sql_queries import SqlQueries


class UnitTest:
    @staticmethod
    def check_duplicate_rows(redshift, table=""):
        """
        Check for duplicate rows in a table
        :param redshift: Redshift connection
        :param table: Table need to be checked
        :return: None
        :raises: ValueError when there are duplicate rows
        """
        logging.info("Checking for duplicate rows in {}".format(table))
        result = redshift.get_records(
            SqlQueries.check_duplicate_rows.format(table, table, table)
        )
        if result[0][0] > 0:
            logging.error(f"Duplicate rows found in {table}")
            raise ValueError("Duplicate rows found")
        else:
            logging.info("No duplicate rows found in {}".format(table))
            return True

    @staticmethod
    def check_null_column(redshift, table="", column=""):
        """
        Check for null values in a column
        :param redshift: Redshift connection
        :param table: Table need to be checked
        :param column: Column need to be checked
        :return: None
        :raises: ValueError when there are null values in a column
        """
        logging.info("Checking for null values in {}.{}".format(table, column))
        result = redshift.get_records(
            SqlQueries.check_null_column.format(table, column)
        )
        if result[0][0] > 0:
            logging.error(f"Null values found in {table}.{column}")
            raise ValueError("Null values found")
        else:
            logging.info("No null values found in {}.{}".format(table, column))
            return True

    @staticmethod
    def check_existed_foreign_key(
            redshift,
            primary_table="",
            foreign_table="",
            primary_column="",
            foreign_column=""
    ):
        """
        Check for existed foreign key
        :param redshift: Redshift connection
        :param primary_table: Primary table need to be checked
        :param foreign_table: Foreign table need to be checked
        :param primary_column: Primary column need to be checked
        :param foreign_column: Foreign column need to be checked
        :return: None
        :raises: ValueError when there are foreign key(s) that do not exist in primary table
        """
        logging.info("Checking for existed foreign key")
        result = redshift.get_records(
            SqlQueries.check_existed_foreign_key.format(
                primary_table, foreign_table, primary_column, foreign_column, primary_column, foreign_column
            )
        )
        if result[0][0] > 0:
            logging.error(f"There are key(s) in {foreign_table}.{foreign_column} " +
                          f"not existed in {primary_table}.{primary_column}")
            raise ValueError("Error foreign key not found in primary key")
        else:
            logging.info(f"All keys in {foreign_table}.{foreign_column} " +
                         f"are existed in {primary_table}.{primary_column}")
            return True

    @staticmethod
    def run(redshift):
        tables = [{
            "table": "artists",
            "primary_key": "artistid",
        }, {
            "table": "songs",
            "primary_key": "songid",
        }, {
            "table": "songplays",
            "primary_key": "playid",
        }, {
            "table": "users",
            "primary_key": "userid",
        }, {
            "table": "time",
            "primary_key": "start_time",
        }]

        # Check for duplicate rows in each table
        for table in tables:
            UnitTest.check_duplicate_rows(redshift, table["table"])

        # Check for null values in primary key
        for table in tables:
            UnitTest.check_null_column(redshift, table["table"], table["primary_key"])

        # Check for null artist name
        UnitTest.check_null_column(redshift, "artists", "name")

        # Check for null song title
        UnitTest.check_null_column(redshift, "songs", "title")

        # Check for null user level
        UnitTest.check_null_column(redshift, "users", "level")

        # Check for null user first name
        UnitTest.check_null_column(redshift, "users", "first_name")

        # Check for artist id in songs table exists in artists table
        UnitTest.check_existed_foreign_key(
            redshift,
            "artists",
            "songs",
            "artistid",
            "artistid"
        )

        # Check for song id in songplays table exists in songs table
        UnitTest.check_existed_foreign_key(
            redshift,
            "songs",
            "songplays",
            "songid",
            "songid"
        )

        # Check for user id in songplays table exists in users table
        UnitTest.check_existed_foreign_key(
            redshift,
            "users",
            "songplays",
            "userid",
            "userid"
        )

        # Check for start time in songplays table exists in time table
        UnitTest.check_existed_foreign_key(
            redshift,
            "time",
            "songplays",
            "start_time",
            "start_time"
        )
