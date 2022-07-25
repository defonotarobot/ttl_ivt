from tkinter.tix import Tree
import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
import xlsxwriter
from color import color
import time
import threading
import itertools
import sys

class Comparator:
    def __init__(self):
        pass
    
    # Function: compare()
    # -------------------
    # Takes in filepaths to two folders, each containing excel tables and compares the
    # values in each table. Returns a new folder, containing subfolders for tables that 
    # matched/didn't match.
    def compare(self, path1, path2, env1, env2):
        # Initialize variables from given config file
        print(color.BOLD + color.CYAN + ">>>>>>>>>>>>>>>>>>>> START COMPARATOR <<<<<<<<<<<<<<<<<<<<" + color.END)
        with open("config_comparator.json") as json_config_file:
            config = json.load(json_config_file)

        with open("key_table.json") as key_config:
            keys = json.load(key_config)

        # Initialize table options
        pd.options.mode.chained_assignment = None  # default='warn'
        pd.set_option('display.max_columns', 500)

        directory = os.fsencode(path1)
        
        process_done = False
        total_tables = 0
        total_files = 0
        total_matched = 0
        total_unmatched = 0
        
        matchResult = [color.BOLD + color.RED + "UNMATCHED" + color.END, color.BOLD + color.GREEN + "MATCHED" + color.END]

        output_directory = "../compare/compare_" + env1 + "_" + env2 + "_" + datetime.now().strftime("%d%m%Y_%H%M_%p")

        # Create output directory if it doesn't already exist
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
            os.makedirs(output_directory + "/matched/")
            os.makedirs(output_directory + "/unmatched/")

        for table in os.listdir(directory):
            try:
                file_name = os.fsdecode(table)
                table_name, ext = os.path.splitext(file_name)
                file = '\\' + file_name

                print("Comparing table: " + color.BOLD + "\"" + table_name + "\"" + color.END)

                df1 = pd.read_csv(path1 + file, dtype=str).fillna('')
                print("Reading source data from: " + color.CYAN + path1 + file_name + color.END)

                df2 = pd.read_csv(path2 + file, dtype=str).fillna('')
                print("Reading target data from: " + color.CYAN + path2 + file_name + color.END)

                table_key = keys[table_name]

                id_present = table_key in list(df1.columns)

                check_result = self.__check_keys(df1, df1.columns, table_key)
                pri_key = check_result["newKey"]

                if check_result["matchedResult"] == True:
                
                    # concatenate new pri_key in the table for composite pk table
                    if check_result["isComposite"] == True:
                        df1 = self.__insert_composite_pk(df1, check_result["keys"], pri_key)
                        df2 = self.__insert_composite_pk(df2, check_result["keys"], pri_key)
                        # add the check key query in here?

                    dfx = df1.merge(df2, on=pri_key,
                                    how='outer')  # outer join on id
                    tmp = dfx.reindex(sorted(dfx.columns), axis=1).fillna(
                        '')  # sorting to make same field adjacent
                    compare_result = self.__compare(tmp, df1.shape[1], pri_key)  # compare

                    data = compare_result["data"]
                    isMatched = compare_result["isMatched"]

                    print("Matching result for table " + color.BOLD + "\"{0}\"" + color.END + "is {1}".format(table_name, matchResult[isMatched]))
                    # print("Matching result for table {0} is {1}.".format(table_name, isMatched))

                    if not isMatched:
                        print("Unmatched Columns: " + color.YELLOW + "{0}".format(compare_result["unMatchedColumns"]) + color.END + "\n")
                        total_unmatched += 1
                    else:
                        total_matched += 1

                    almost_done = self.__union(data, env1, env2, df1, df2, pri_key)
                    done = self.__prefix(almost_done, env1, env2)
                    
                    output_path = output_directory
                    if isMatched:
                        output_path += "/matched/"
                    else:
                        output_path += "/unmatched/"
                    

                    print("Result location: \"" + output_path + "\"")
                    print("Result file: \"" + env1 + '_' + env2 + '_' + table_name + '.xlsx\"')
                    
                    # t = threading.Thread(gg=self.__animate(process_done))
                    # t.start()

                    #long process here
                    print("Exporting...\n")
                    
                    with pd.ExcelWriter(output_path + env1 + '_' + env2 + '_' + table_name + '.xlsx') as writer:
                        df1.to_excel(writer, sheet_name=env1, index=False)
                        df2.to_excel(writer, sheet_name=env2, index=False)
                        done.to_excel(writer, sheet_name='COM', index=False)
                    total_files += 1
                    
                    # process_done = True
                    
                    print("Exported result file for the table " + color.BOLD + "\"{0}\"".format(table_name) + color.END + "\n")
                else:
                    print("Can't find key for table {0}.".format(table_name))
            except Exception as error:
                print("Error occurred for table {0}. - {1}".format(table_name, error))

            total_tables += 1
            print(color.UNDERLINE + "Summary" + color.END)

        print("Total Tables: {0}".format(total_tables))
        print("Total Output Files: {0}".format(total_files))
        print("Total Unmatched: {0}".format(total_unmatched))
        print("Total Matched: {0}".format(total_matched))

        print(color.BOLD + color.CYAN + ">>>>>>>>>>>>>>>>>>>> END COMPARATOR <<<<<<<<<<<<<<<<<<<<" + color.END)
    
    def __compare(self, merged_table, column_number, table_key):
        dfc = merged_table.drop(table_key, axis=1)
        result = merged_table[[table_key]]
        col = 2  # start from third col

        isMatched = True
        unMatchedColumns = {}

        for i in range(0, column_number-1):
            # first colon selects all rows, second arg selects col-2 to col
            com_col = dfc.iloc[:, col-2:col]

            com_col_ret = com_col.iloc[:, 0] == com_col.iloc[:, 1]
            com_col_name = 'COM_' + com_col.columns[0][:-2]
            com_col[com_col_name] = com_col_ret

            unMatchedRet = com_col_ret.loc[com_col_ret[:] == False]
            cellUnmatched = len(unMatchedRet) >= 1
            isMatched = isMatched and not cellUnmatched

            if cellUnmatched:
                unMatchedCount = len(unMatchedRet)
                unMatchedColumns[com_col.columns[0][:-2]] = unMatchedCount

            # comparison logic
            # com_col column name defined on the left inside outer bracket
            # [0] is the label and [:-2] says to get string from left until
            # position -2 as counting from right increases negatively
            # com_col values on the first column then set to bool comp logic of
            col += 2
            result = pd.concat([result, com_col], axis=1)

        return {"data": result, "isMatched": isMatched, "unMatchedColumns": unMatchedColumns}

    def __prefix(self, tbl, env1, env2):
        result = tbl.copy()
        for col in result.columns:
            new_col = ''
            if '_x' in col:
                new_col = env1 + '_' + col.removesuffix('_x')
            elif '_y' in col:
                new_col = env2 + '_' + col.removesuffix('_y')
            else:
                new_col = col
            result = result.rename(columns={col: new_col})

        return result

    def __union(self, tbl, env1, env2, df1, df2, table_key):
        y = tbl.copy()
        y.insert(1, 'is in ' + env2, y[table_key].isin(df2[table_key]))
        y.insert(1, 'is in ' + env1, y[table_key].isin(df1[table_key]))

        return y

    def __check_keys(self, df1, column_list, table_keys, delimiter=","):

        isComposite = False
        matchedResult = False

        keys = list(map(str.strip, table_keys.split(delimiter)))
        keyCount = len(keys)

        if keyCount > 1:
            isComposite = True
            newKey = "_".join(keys)
            matchedResult = set(keys).issubset(column_list)
        else:
            newKey = table_keys
            matchedResult = newKey in list(df1.columns)

        return {"isComposite": isComposite, "matchedResult": matchedResult, "newKey": newKey, "keys": keys}

    def __insert_composite_pk(self, tbl, table_keys, compositeKey, delimiter="_"):
    
        result = tbl.copy()
        composite_key_col = pd.DataFrame()
        i = 0
    
        for table_key in table_keys:
            key_col = tbl.loc[:][table_key]
    
            if i == 0:
                composite_key_col[compositeKey] = key_col
            else:
                composite_key_col[compositeKey] = composite_key_col[compositeKey].astype(
                    str) + "_" + key_col
    
            i += 1
    
        result = pd.concat([result, composite_key_col], axis=1)
    
        return result

    def __animate(self, process_done):
        for c in itertools.cycle(['|', '/', '-', '\\']):
            if process_done:
                break
            sys.stdout.write('\rExporting...' + c)
            sys.stdout.flush()
            time.sleep(0.1)
        sys.stdout.write('\Exported!     ')