from encodings import utf_8
import pymysql
import csv
import sys
import json
import os
from datetime import datetime

class Exporter:
    def __init__(self):
        pass

    def export(self, env: str):
        with open("config_exporter.json") as json_config_file:
            full_config = json.load(json_config_file)

        print("Read config.json completed.")

        # Initialize Variables
        config = full_config[env]
        fileconfig = config["fileconfig"]
        dbconfig = config["dbconfig"]
        table_list_filepath = fileconfig["table_list_file"]
        csv_file_path = "../data_" + env + "_" + datetime.now().strftime("%d%m%Y") + "/"
        total_tables = 0
        total_csv = 0

        # Create output directory if it doesn't already exist
        if not os.path.exists(csv_file_path):
            os.makedirs(csv_file_path)

        # Read Table List
        table_list_file = open(table_list_filepath, 'r')
        table_list = table_list_file.readlines()

        print("Read table_list_file {0} completed.".format(table_list_filepath))

        db_opts = dbconfig

        print("Prepare database connection completed.")

        db = pymysql.connect(**db_opts)
        print("Connect database completed.")
        print("exporting from " + full_config["env"])

        for table_name in table_list:
            cur = db.cursor()

            table_name = table_name.strip()

            sql = 'SELECT * from '
            output_file_path = '{0}{1}.csv'.format(csv_file_path, table_name)

            try:
                cur.execute(sql + "%s" % table_name)
                rows = cur.fetchall()

                print("Read table {0} completed.".format(table_name))

                # Continue only if there are rows returned.
                #if rows:
                # New empty list called 'result'. This will be written to a file.
                result = list()

                # The row name is the first entry for each entity in the description tuple.
                column_names = list()
                for i in cur.description:
                    column_names.append(i[0])

                result.append(column_names)
                for row in rows:
                    result.append(row)

                # Write result to file.
                with open(output_file_path, 'w', newline='', encoding = 'utf-8') as csvfile:
                    csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    for row in result:
                        csvwriter.writerow(row)

                    total_csv += 1
                    print("Write csv for table {0} completed.".format(table_name))
                #else:
                #    print("No rows found for table {0}".format(table_name))

            except Exception as error:
                print("Error occurred for table {0}. - {1}".format(table_name, error))

            total_tables += 1
            print(".")

        db.close()

        print("Database connection closed.")

        print("Total Tables: {0}".format(total_tables))
        print("Total CSV Files: {0}".format(total_csv))

        print(">>>>>End Export<<<<<")

        return csv_file_path