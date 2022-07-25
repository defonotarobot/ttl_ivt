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
    
    # Function: export()
    # -------------------
    # Takes in an environment and a filename and exports the tables listed in "table_list.txt".
    # If the user wants to export all tables, set the optional boolean parameter use_full_table_list
    # to True when calling.
    def export(self, env: str, env_naming: str, use_full_table_list: bool):
        print(color.BOLD + color.CYAN + ">>>>>>>>>>>>>>>>>>>> START EXPORTER <<<<<<<<<<<<<<<<<<<<" + color.END)
        
        # If environment is prod, prompt user to switch VPN to prod, then switch back after exporting
        if (env == "prod"):
            while (True):
                answer = str(input(color.BOLD + color.RED + "ACTION REQUIRED: " + color.END + "You're trying to export from prod database. Are you sure you're connected to the right VPN? Type 'y' to move on: "))
                if answer.lower() == "y":
                    break

        with open("config_exporter.json") as json_config_file:
            full_config = json.load(json_config_file)

        print("Full config " + color.BOLD + color.GREEN + "IMPORTED" + color.END + " from the file " + color.BOLD + "\"config_exporter.json\"" + color.END)

        # Initialize Variables
        config = full_config[env]
        fileconfig = config["fileconfig"]
        dbconfig = config["dbconfig"]
        csv_file_path = "../data/data_" + env_naming + "_" + datetime.now().strftime("%d%m%Y_%H%M_%p") + "/"
        total_tables = 0
        total_csv = 0

        # Create output directory if it doesn't already exist
        if not os.path.exists(csv_file_path):
            os.makedirs(csv_file_path)

        # Read Table List
        table_list_filepath = "table_list.txt"
        if (use_full_table_list):
            table_list_filepath = "full_table_list.txt"

        table_list_file = open(table_list_filepath, 'r')
        table_list = table_list_file.readlines()

        print("Exporting table list from the file \"" + color.BOLD + "{0}".format(table_list_filepath) + "\"" + color.END)

        db_opts = dbconfig

        print("Prepared config before connecting to dB")

        db = pymysql.connect(**db_opts)
        print("dB connection " + color.BOLD + color.GREEN + "ESTABLISHED" + color.END)
        print("Exporting from " + color.BOLD + color.UNDERLINE + env.upper() + color.END)

        for table_name in table_list:
            cur = db.cursor()

            table_name = table_name.strip()

            sql = 'SELECT * from '
            output_file_path = '{0}{1}.csv'.format(csv_file_path, table_name)

            print("Attempting to download table: \"" + color.BOLD + "{0}".format(table_name) + "\"" + color.END)
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
                    print("Table saved as CSV: \"" + color.BOLD + "{0}".format(table_name) + "\"" + color.END)
                #else:
                #    print("No rows found for table {0}".format(table_name))

            except Exception as error:
                print(color.BOLD + color.RED + "Error occurred querying for the table {0}. - {1}".format(table_name, error) + color.END + "\n")

            total_tables += 1
            print(color.UNDERLINE + "Summary\n" + color.END)

        db.close()

        print("dB connection " + color.BOLD + color.GREEN + "CLOSED" + color.END)

        print("Total listed tables: {0}".format(total_tables))
        print("Total CSV files exported: {0}".format(total_csv))

        if (env == "prod"):
            while (True):
                answer = str(input(color.BOLD + color.RED + "ACTION REQUIRED: " + color.END + "If you are going to export from a non-prod database next, please switch your VPN to non-prod and then type 'y' to continue: "))
                if answer.lower() == "y":
                    break

        print(color.BOLD + color.CYAN + ">>>>>>>>>>>>>>>>>>>> END EXPORTER <<<<<<<<<<<<<<<<<<<<" + color.END)

        return csv_file_path