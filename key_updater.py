import json
import pymysql

from color import color


class KeyUpdater:
    def __init__(self):
        pass

    def updateKeys(self):
        print(color.BOLD + color.CYAN + ">>>>>>>>>>>>>>>>>>>> START KEY UPDATER <<<<<<<<<<<<<<<<<<<<" + color.END)
        env = "ps2"

        # Open table list file to obtain keys for
        table_list_file = open("full_table_list.txt", 'r')
        table_list = table_list_file.readlines()

        # Get database configurations
        with open("config_exporter.json") as json_config_file:
            full_config = json.load(json_config_file)
        config = full_config[env]
        dbconfig = config["dbconfig"]
        db_opts = dbconfig
        print("Prepared config before connecting to dB")

        # Establish connection to database
        while True:
            print("Attempting to connect to " + color.BOLD + color.UNDERLINE + env.upper() + color.END + " dB...\n")
            try:
                db = pymysql.connect(**db_opts)
                print("dB connection " + color.BOLD + color.GREEN + "ESTABLISHED" + color.END)
                print("Extracting keys from " + color.BOLD + color.UNDERLINE + env.upper() + color.END)
                break
            except Exception as error:
                print(color.RED + "Error connecting to " + color.BOLD + "NON-PROD" + color.END + color.RED + " database" + color.END)
                while True:
                    answer = str(input(color.BOLD + color.RED + "ACTION REQUIRED: " + color.END + "Would you like to try again? [y/n] (Check VPN first maybe?)"))
                    if answer.lower() == "y":
                        break
        
        # For each table in full_table_list.txt, extract keys and concatenate them
        result = {}
        for table in table_list:
            table = table.strip()
            full_table_name = table.split(".")
            schema = full_table_name[0]
            name = full_table_name[1]

            sql = "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA = '" + schema + "' AND TABLE_NAME = '" + name + "';"
            try:
                cur = db.cursor()
                cur.execute(sql)
                rows = cur.fetchall()

                if len(rows) == 0:
                    print(color.RED + "No keys were obtained from table: " + color.BOLD + table + color.END)
                    print(color.RED + "Terminating process without writing to key_table.json. Check table names in full_table_list.txt.")
                    return
                
                # Concatenate all keys together
                keys = []
                for row in rows:
                    keys.append(row[0])
                key = ",".join(keys)

                result[table] = key
                print(color.GREEN + "Successfully gathered keys from: " + color.BOLD + table + color.END)
            except Exception as error:
                print(color.RED + "Error occured while getting keys from table: " + color.BOLD + table + color.END)
                print(color.RED + "Terminating process without writing to key_table.json. Check VPN connection before running again.")
                return
            
        # Serializing json
        json_object = json.dumps(result, indent=4)

        # Write to key_table.json
        with open("key_table.json", "w") as outfile:
            outfile.write(json_object)
        
        print(color.GREEN + "Successfully updated file" + color.BOLD + " 'key_table.json' " + color.END + color.GREEN + "with updated keys" + color.END)

        print(color.BOLD + color.CYAN + ">>>>>>>>>>>>>>>>>>>> END KEY UPDATER <<<<<<<<<<<<<<<<<<<<" + color.END)
        


        

        