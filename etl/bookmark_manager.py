from etl.dynamo_helper import read_parameter_from_dynamodb, write_parameter_to_dynamodb


class BookmarkManager:
    """
    ##########################################
    Raw to Stage
    ##########################################
    """

    def __init__(self, bookmark_table):
        self.bookmark_table = bookmark_table

    def read_bookmark(self, table):
        primary_key = {"table_name": table}

        item = read_parameter_from_dynamodb(self.bookmark_table, primary_key)

        return item["timestamp_bookmark"] if item else None

    def write_bookmark(self, table, bookmark_value):
        item = {"table_name": table, "timestamp_bookmark": bookmark_value}
        write_parameter_to_dynamodb(self.bookmark_table, item)

    def read_bookmark_with_fallback(self, bookmark_value, table):
        # If the parameter store is empty, fallback to the provided parameter
        if bookmark_value and bookmark_value != "None":
            return bookmark_value
        else:
            bookmark = self.read_bookmark(table)
            return bookmark if bookmark else "INITIAL_LOAD"
