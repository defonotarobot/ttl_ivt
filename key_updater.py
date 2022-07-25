import json
import pymysql

from color import color


class KeyUpdater:
    def __init__(self):
        pass

    def updateKeys(self):
        print(color.BOLD + color.CYAN + ">>>>>>>>>>>>>>>>>>>> START KEY UPDATER <<<<<<<<<<<<<<<<<<<<" + color.END)

        # Open table list file to obtain keys for
        table_list_file = open("full_table_list.txt", 'r')
        table_list = table_list_file.readlines()

        # Connect to database
        with open("config_exporter.json") as json_config_file:
            full_config = json.load(json_config_file)
        config = full_config["ps2"]
        dbconfig = config["dbconfig"]
        db_opts = dbconfig
        print("Prepared config before connecting to dB")
        db = pymysql.connect(**db_opts)

        
        result = {}
        for table in table_list:
            table = table.strip()
            full_table_name = table.split(".")
            schema = full_table_name[0]
            name = full_table_name[1]

            sql = "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA = '" + schema + "' AND TABLE_NAME = '" + name + "';"
            cur = db.cursor()
            cur.execute(sql)
            rows = cur.fetchall()

            # For each table, concatenate the keys together to create composite keys
            keys = []
            for row in rows:
                keys.append(row[0])
            key = ",".join(keys)

            result[table] = key
        
        # Serializing json
        json_object = json.dumps(result, indent=4)

        # Write to key_table.json
        with open("key_table.json", "w") as outfile:
            outfile.write(json_object)

        print(color.BOLD + color.CYAN + ">>>>>>>>>>>>>>>>>>>> END KEY UPDATER <<<<<<<<<<<<<<<<<<<<" + color.END)
        


        

        