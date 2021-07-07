import pandas as pd
import timeit
from collections import defaultdict


# ==============================
# Import data
# ==============================

df_1c = pd.read_csv('./datatocompare/1C_datatocompare.csv', delimiter=";", index_col='Код', decimal=',', low_memory=False)
df_sql = pd.read_csv('./datatocompare/SQL_datatocompare.csv', delimiter=";", index_col='personalaccount')

# Number of lines in 1C dataframe
number_lines = df_1c.shape[0]


# Get all id from both datasets to iterate over it
df_sql['personalaccount'] = df_sql.index
idlist_1c = set(df_1c['Ссылка'])
idlist_sql = set(df_sql['personalaccount'])

idlist_all = list(idlist_1c.union(idlist_sql))

# Declare vars
idnot_1c = list(idlist_1c.difference(idlist_sql))
idnot_sql = list(idlist_sql.difference(idlist_1c))


# ==============================
# Deletion mark analysis and processing
# ==============================
df_delmark = df_1c.loc[(df_1c['ПометкаУдаления'] == 'Да')]


# ==============================
# 1C dataset processing
# ==============================

# Drop extra columns
df_1c_clear = df_1c.drop(['ПометкаУдаления', 'ПроживающихПроверка'], axis = 1)

# Unifying column order
df_1c_clear = df_1c_clear.reindex(columns=['Ссылка', 'ТелефонныйКод', 'Телефон', 'ЕжемесячныйПлатеж', 'СуммаОстаток',
                                           'ПроживающихВТЧ', 'КоличествоПроживающих'])


# Unifying column names
#df_1c_clear.rename(columns={'Ссылка': 'personalaccount', 'ТелефонныйКод': 'phonecode', 'Телефон': 'phone',
                     #       'ЕжемесячныйПлатеж': 'tarrif', 'СуммаОстаток': 'balance',
                     #       'ПроживающихВТЧ': 'dynamic_prescribed', 'КоличествоПроживающих': 'static_prescribed'},
                   #inplace=True)


df_sql.rename(columns={'phonecode': 'ТелефонныйКод', 'phone':'Телефон', 'tarrif': 'ЕжемесячныйПлатеж',
                       'balance': 'СуммаОстаток', 'dynamic_prescribed':'ПроживающихВТЧ', 'static_prescribed':'КоличествоПроживающих',
                       'personalaccount':'Ссылка'}, inplace=True)



# Unifying balance values (advance/debt)
df_1c_clear['СуммаОстаток'] = df_1c_clear['СуммаОстаток'].multiply(-1)


# ==============================
# SQL dataset processing
# ==============================

# Sort by index
df_sql = df_sql.sort_index()

# Unifying column order
df_sql = df_sql.reindex(columns=['Ссылка', 'ТелефонныйКод', 'Телефон', 'ЕжемесячныйПлатеж', 'СуммаОстаток',
                                           'ПроживающихВТЧ', 'КоличествоПроживающих'])

# Data type changing
df_sql['КоличествоПроживающих'] = df_sql['КоличествоПроживающих'].astype(int)
df_sql['Ссылка'] = df_sql['Ссылка'].astype(int)


# ==============================
# Compartment
# ==============================

# Reindex dataset with less amount of accounts to dataset with more accounts (fill blanks in index of dataset)
if len(df_1c_clear) > len(df_sql):
    df_sql = df_sql.reindex(df_1c_clear.index)
else:
    df_1c_clear = df_1c_clear.reindex(df_sql.index)

#===============================
# Compartment cycle
#===============================

# Fill N/A
df_sql = df_sql.fillna(0)
df_1c_clear = df_1c_clear.fillna(0)


# Data type changing
df_sql['Ссылка'] = df_sql['Ссылка'].astype(int)
df_sql['ПроживающихВТЧ'] = df_sql['ПроживающихВТЧ'].astype(int)

# Create lists
list_id_not = []
list_number_ls_sql = []
list_number_ls_1c = []
value_sql = []
value_1c = []
selective_columns = []
summary_dict = defaultdict(int)
for id in idlist_all:
     if id < (len(idlist_all)-1):
    #if pd.notna(id):
        # extract rows from dataset
        row_to_compare_1C = df_1c_clear.iloc[id]
        row_to_compare_sql = df_sql.iloc[id]


        # get column list to iterate over it
        columns = df_1c_clear.columns.tolist()
        for idx, column in enumerate(columns):

            compare_result = (row_to_compare_sql[columns[idx]] == row_to_compare_1C[columns[idx]])
            if compare_result == False and row_to_compare_sql[0]!=0:
                summary_dict[column]+=1
                # save values to lists
                list_id_not.append(id)
                list_number_ls_sql.append(row_to_compare_sql[0])
                list_number_ls_1c.append(row_to_compare_1C[0])
                selective_columns.append(column)
                value_sql.append(row_to_compare_sql[columns[idx]])
                value_1c.append(row_to_compare_1C[columns[idx]])


for key in summary_dict:
        print(f' Не совпадает {key}: {summary_dict[key]}')

print('========================================================')




print(summary_dict)
discrepancy_dict = {'Номер строки': list_id_not,
     'Номер ЛС sql': list_number_ls_sql,
     'Номер ЛС 1С' : list_number_ls_1c,
     'Аттрибут' : selective_columns,
     'Значение SQL' :   value_sql,
     'Значение 1С' : value_1c
}

#Save data in dicts as dataframe and download as csv
print('Загружается подробный отчет о различиях между файлами ...')
df = pd.DataFrame(discrepancy_dict)
print(df.head())
print('Загружается подробный отчет по поиску расхождений в двух файлах ...')
print('==================================================================================')
print('==================================================================================')

df.to_csv('AFK_LK_db_compare_discrepancy_2021.csv', sep=';', encoding='ANSI', decimal=',')
print('Файл загрузился')


print('Загружается сводная таблица по статистике различий между файлами ...')
print('==================================================================================')
print('==================================================================================')


with open('results/AFK_LK_db_compare_summary_2021.txt', 'w') as file:
    file.write(f'Всего записей: {number_lines}\n')
    file.write(f'Помечены на удаление: {df_delmark.shape[0]}\n')
    file.write(f'Не найдено ЛС в 1С: {len(idnot_1c)}\n')
    file.write(f'не найдено ЛС в sql: {len(idnot_sql)}\n')
    for key, value in summary_dict.items():
        file.write(f'Не совпадает {key}: {value}\n')
print('Файл загрузился')