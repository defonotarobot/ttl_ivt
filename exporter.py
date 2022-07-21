from encodings import utf_8
import pymysql
import csv
import sys
import json
import os
from datetime import datetime
from color import color

class Exporter:
    def __init__(self):
        pass

    def export(self, env: str, env_naming: str):
        print(color.BOLD + color.CYAN + ">>>>>>>>>>>>>>>>>>>> START EXPORTER <<<<<<<<<<<<<<<<<<<<" + color.END)

        with open("config_exporter.json") as json_config_file:
            full_config = json.load(json_config_file)

        print("Full config " + color.BOLD + color.GREEN + "IMPORTED" + color.END + " from the file " + color.BOLD + "\"config_exporter.json\"" + color.END)

        # Initialize Variables
        config = full_config[env]
        fileconfig = config["fileconfig"]
        dbconfig = config["dbconfig"]
        table_list_filepath = fileconfig["table_list_file"]
        csv_file_path = "../data_" + env_naming + "_" + datetime.now().strftime("%d%m%Y_%H%M_%p") + "/"
        total_tables = 0
        total_csv = 0

        # Create output directory if it doesn't already exist
        if not os.path.exists(csv_file_path):
            os.makedirs(csv_file_path)

        # Read Table List
        table_list_file = open(table_list_filepath, 'r')
        table_list = table_list_file.readlines()

        print("Exported table list from the file \"" + color.BOLD + "{0}".format(table_list_filepath) + "\"" + color.END)

        db_opts = dbconfig

        print("Prepared config before connecting to dB")

        db = pymysql.connect(**db_opts)
        print("dB connection " + color.BOLD + color.GREEN + "ESTABLISHED" + color.END)
        print("Exporting from " + color.BOLD + color.UNDERLINE + full_config["env"].upper() + color.END)

        for table_name in table_list:
            cur = db.cursor()

            table_name = table_name.strip()

            sql = 'SELECT * from '
            output_file_path = '{0}{1}.csv'.format(csv_file_path, table_name)

            print("Attempting to download the table \"" + color.BOLD + "{0}".format(table_name) + "\"" + color.END)
            try:
                cur.execute(sql + "%s" % table_name)
                rows = cur.fetchall()

                print(color.GREEN + color.BOLD + "DOWNLOAD SUCCESSFUL" + color.END)

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
                    print("Finished writing csv file for the table \"" + color.BOLD + "{0}".format(table_name) + "\"" + color.END)
                #else:
                #    print("No rows found for table {0}".format(table_name))

            except Exception as error:
                print(color.BOLD + color.RED + "Error occurred querying for the table {0}. - {1}".format(table_name, error) + color.END)

            total_tables += 1
            print(".")

        db.close()

        print("dB connection " + color.BOLD + color.GREEN + "CLOSED" + color.END)

        print("Total listed tables: {0}".format(total_tables))
        print("Total CSV files exported: {0}".format(total_csv))

        print(color.BOLD + color.CYAN + ">>>>>>>>>>>>>>>>>>>> END EXPORTER <<<<<<<<<<<<<<<<<<<<" + color.END)

        return csv_file_path