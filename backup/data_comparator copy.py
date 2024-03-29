# %%
from tkinter.tix import Tree
import pandas as pd
import numpy as np
import os
import json
import xlsxwriter
import pymysql


# %%
# Read Config File
with open("config_comparator.json") as json_config_file:
    config = json.load(json_config_file)

# Initialize Variables
config_source = config["source"]
config_target = config["target"]
default_unique_column = config["default_unique_column"]
config_faulty = config["faulty_dir"]
config_clean = config["clean_dir"]
config_output = config["output"]
exception_tables = config["exception_tables"]
dbconfig = config["dbconfig"]

pd.options.mode.chained_assignment = None  # default='warn'


# %%
def compare(merged_table, column_number, table_key):

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


# %%
def prefix(tbl, env1, env2):
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


# %%
def union(tbl, env1, env2, table_key):

    y = tbl.copy()
    y.insert(1, 'is in ' + env2, y[table_key].isin(df2[table_key]))
    y.insert(1, 'is in ' + env1, y[table_key].isin(df1[table_key]))

    return y

# %%


def check_keys(column_list, table_keys, table_name, delimiter=","):
    
    db_opts = dbconfig
    db = pymysql.connect(**db_opts)
    print("Connect database completed.")    

    cur = db.cursor() 
    table_name = table_name.strip()

    sql = 'SELECT CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION FROM information_schema.KEY_COLUMN_USAGE WHERE CONSTRAINT_NAME = \'PRIMARY\' AND TABLE_SCHEMA = \'\' AND TABLE_NAME = \'\''
    output_file_path = '{0}{1}.csv'.format(csv_file_path, table_name)
    try:
        cur.execute(sql + "%s" % table_name)
        # isComposite = False
        # matchedResult = False

        keys = list(map(str.strip, table_keys.split(delimiter)))
        keyCount = len(keys)

        if keyCount > 1:
            isComposite = True
            newKey = "_".join(keys)
            matchedResult = set(keys).issubset(column_list)
        else:
            newKey = table_keys
            matchedResult = newKey in list(df1.columns)
    except Exception as error:
        print("Error occurred for table {0}. - {1}".format(table_name, error))

    return {"isComposite": isComposite, "matchedResult": matchedResult, "newKey": newKey, "keys": keys}

# %%


def insert_composite_pk(tbl, table_keys, compositeKey, delimiter="_"):

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


# %%
pd.set_option('display.max_columns', 500)

directory = os.fsencode(config_source["dir"])
path1 = config_source["dir"]
path2 = config_target["dir"]
env1 = config_source["env"]
env2 = config_target["env"]

total_tables = 0
total_files = 0

for table in os.listdir(directory):
    try:
        file_name = os.fsdecode(table)
        table_name, ext = os.path.splitext(file_name)
        file = '\\' + file_name

        print("Table: " + table_name)

        df1 = pd.read_csv(path1 + file, dtype=str).fillna('')
        print("Loaded data: " + path1 + file)

        df2 = pd.read_csv(path2 + file, dtype=str).fillna('')
        print("Loaded data: " + path2 + file)

        table_key = default_unique_column

        if table_name in exception_tables:
            table_key = exception_tables[table_name]

        id_present = table_key in list(df1.columns)

        check_result = check_keys(df1.columns, table_key, table_name)
        pri_key = check_result["newKey"]

        if check_result["matchedResult"] == True:

            # concatenate new pri_key in the table for composite pk table
            if check_result["isComposite"] == True:
                df1 = insert_composite_pk(df1, check_result["keys"], pri_key)
                df2 = insert_composite_pk(df2, check_result["keys"], pri_key)

            dfx = df1.merge(df2, on=pri_key,
                            how='outer')  # outer join on id
            tmp = dfx.reindex(sorted(dfx.columns), axis=1).fillna(
                '')  # sorting to make same field adjacent
            compare_result = compare(tmp, df1.shape[1], pri_key)  # compare

            data = compare_result["data"]
            isMatched = compare_result["isMatched"]
            print("Matching result for table {0} is {1}.".format(
                table_name, isMatched))

            if not isMatched:
                print("Unmatched Columns: {0}".format(
                    compare_result["unMatchedColumns"]))

            almost_done = union(data, env1, env2, pri_key)
            done = prefix(almost_done, env1, env2)

            output_path = config_output["dir"]

            if isMatched:
                output_path += config_output["matched"]
            else:
                output_path += config_output["unmatched"]

            with pd.ExcelWriter(output_path + env1 + '_' + env2 + '_' + table_name + '.xlsx') as writer:
                df1.to_excel(writer, sheet_name=env1, index=False)
                df2.to_excel(writer, sheet_name=env2, index=False)
                done.to_excel(writer, sheet_name='COM', index=False)
            total_files += 1
            print("Write file for table {0} is completed.".format(table_name))
        else:
            print("Can't find key for table {0}.".format(table_name))
    except Exception as error:
        print("Error occurred for table {0}. - {1}".format(table_name, error))

    total_tables += 1
    print(".")

print("Total Tables: {0}".format(total_tables))
print("Total Output Files: {0}".format(total_files))

print(">>>>>End Program<<<<<")

# %% [markdown]
#

# %%
