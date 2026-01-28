from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp



general_expectations = {"timestamp_valid": "timestamp IS NOT NULL", "client_id_valid": "client_id IS NOT NULL", }
                          
                        #   {"well_id_valid": "well_id IS NOT NULL", "sensor_id_valid": "sensor_id IS NOT NULL"}

@dp.table
@dp.expect_all_or_fail(valid_pages)
def dailyprod_events():
    # Read individual stream endpoints into dataframes
    return spark.readStream.table()


@dp.table
@dp.expect_all_or_fail(valid_pages)
def reservoir_events():
    return spark.readStream.table(src_tables[1])


@dp.table
@dp.expect_all_or_fail(valid_pages)
def equipment_events():
    return spark.readStream.table(src_tables[2])


@dp.table
@dp.expect_all_or_fail(valid_pages)
def wellbore_events():

    return spark.readStream.table(src_tables[3])



@dp.table
@dp.expect_all_or_fail(general_expectations)
def general_validation(source_tables):
    return spark.readStream.table(source_tables)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    description='Validate Stream Data')
    parser.add_argument('input',
                    type=str,
                    help = "The type of event data coming in")

                    
    args = parser.parse_args()
    source = args.input

    sourcezone = "outlake.landing"

    source_tables = f"{sourcezone}.`{source}`"



    general_validation(source_tables)
    







