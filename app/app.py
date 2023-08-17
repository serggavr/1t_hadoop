import pandas
from datetime import datetime as dt

groups_quantity = 10

def on_group_el(dataArr, groups_quantity):
    group_len = round(len(dataArr) / groups_quantity)
    counter_el = 0
    counter_group = 0
    group = []
    groups = []
    for index in range(len(dataArr)):
        if counter_el < group_len - 1:
            group.append(index + 1)
            counter_el += 1
        else:
            group.append(index + 1)
            groups.append(group)
            counter_el = 0
            counter_group += 1
            group = []
        if len(group) > 0 and index + 1 == len(dataArr):
            groups.append(group)

    grouped_el = {}
    for group in groups:
        name = f"{group[0]}-{group[-1]}"
        for el in group:
            grouped_el[el] = name

    return grouped_el

def add_age_group_to_csv(csv, to_csv):
    df = pandas.read_csv(csv, sep=';')
    dt_now = dt.now()
    years = []
    for row in df['Date of birth']:
        years.append(round((dt_now - dt.strptime(row, "%Y-%m-%d")).days / 365))

    dataArr = list(set(years))
    grouped_el = on_group_el(dataArr, 3)
    df_age_group = []

    for row in df['Date of birth']:
        df_age_group.append(grouped_el[round((dt_now - dt.strptime(row, "%Y-%m-%d")).days / 365)])

    df['Age_group'] = df_age_group
    df.to_csv(to_csv, sep=';', index=False)

def add_index_group_to_csv(csv, to_csv):
    df = pandas.read_csv(csv)
    dataArr = list(df['Index'])
    grouped_el = on_group_el(dataArr, groups_quantity)
    df_index_group = []

    for row in df['Index']:
        df_index_group.append(grouped_el[row])

    df['Index_group'] = df_index_group
    df.to_csv(to_csv, sep=';', index=False)


add_index_group_to_csv('../for_load/organizations.csv', '../for_load/organizations_processed.csv')
add_index_group_to_csv('../for_load/customers.csv', '../for_load/customers_processed.csv')
add_index_group_to_csv('../for_load/people.csv', '../for_load/people_processed.csv')
add_age_group_to_csv('../for_load/people_processed.csv', '../for_load/people_processed.csv')
