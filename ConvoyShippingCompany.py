import pandas as pd
import csv
import re
import sqlite3


def print_message(n, word, name):
    if n == 1:
        print(f'{n} {word} was imported to {name}')
    else:
        print(f'{n} {word}s were imported to {name}')


def xlsx_to_csv(name):
    data_frame = pd.read_excel(name, sheet_name='Vehicles', dtype=str)
    name = name.rstrip('xlsx') + 'csv'
    data_frame.to_csv(name, index=False, header=True)
    print_message(data_frame.shape[0], 'line', name)
    return name


def checking_line(line, n, x):
    if n == 0:
        n += 1
        checked_line = line
    else:
        checked_line = []
        for cell in line:
            if cell.isdecimal():
                checked_line.append(cell)
            else:
                x += 1
                checked_line.append(re.findall(r'[0-9]+', cell)[0])
    return checked_line, n, x


def check_csv(name):
    checked_name = name[:-4] + '[CHECKED].csv'
    checked_file = open(checked_name, 'w')
    with open(name, encoding='utf8') as file:
        csv_reader = csv.reader(file)
        x = 0
        n = 0
        for line in csv_reader:
            checked_line, n, x = checking_line(line, n, x)
            checked_file.write(','.join(checked_line) + '\n')
    checked_file.close()
    print(f'{x} cells were corrected in {checked_name}')
    return checked_name


def create_database(name, result, n):
    name = name.replace('[CHECKED].csv', '.s3db')
    conn = sqlite3.connect(name)
    cursor_name = conn.cursor()
    for r in result:
        cursor_name.execute(r)
        conn.commit()
    conn.close()
    print_message(n, 'record', name)
    return name


def scoring_function(line):
    scores = 0
    fuel_consumed = 4.5 * int(line[2])
    pitstops = fuel_consumed / int(line[1])
    truck_capacity = int(line[3])
    if pitstops < 1:
        scores += 2
    elif pitstops < 2:
        scores += 1
    if fuel_consumed <= 230:
        scores += 2
    else:
        scores += 1
    if truck_capacity >= 20:
        scores += 2
    line.append(str(scores))


def create_query(line, n):
    if n == 0:
        line.append('score')
        command = ''
        for cell in line:
            if cell == 'vehicle_id':
                command += cell + ' INTEGER PRIMARY KEY, '
            else:
                command += cell + ' INTEGER NOT NULL, '
        return f'CREATE TABLE convoy ({command[:-2]});'
    else:
        scoring_function(line)
        command = ''
        for cell in line:
            command += f' {cell},'
        return f'INSERT INTO convoy VALUES ({command[1:-1]});'


def database(name):
    result = []
    with open(name, encoding='utf8') as file:
        csv_reader = csv.reader(file)
        n = 0
        for line in csv_reader:
            result.append(create_query(line, n))
            n += 1
    name = create_database(name, result, n - 1)
    return name


def dataframe_from_db(name):
    conn = sqlite3.connect(name)
    df = pd.read_sql('SELECT * FROM convoy', conn)
    conn.close()
    return df


def db_to_json(name):
    df = dataframe_from_db(name)
    df = df.loc[df['score'] > 3]
    df = df.filter(items=df.keys()[:-1])
    result = df.to_json(orient='records')
    result = '{"convoy":' + result + '}'
    return result, len(df)


def json_save(name):
    result, n = db_to_json(name)
    name = name.replace('.s3db', '.json')
    with open(name, 'w') as file:
        file.write(result)
    print_message(n, 'vehicle', name)
    return name


def xml_save(name):
    name = name.replace('.json', '.xml')
    df_name = name.replace('.xml', '.s3db')
    df = dataframe_from_db(df_name)
    df = df.loc[df['score'] <= 3]
    df = df.filter(items=df.keys()[:-1])
    result = df.to_xml(root_name='convoy', row_name='vehicle', xml_declaration=False, index=False)
    if result == '<convoy/>':
        result = '<convoy></convoy>'
    with open(name, 'w') as file:
        file.write(result)
    print_message(len(df), 'vehicle', name)


def main():
    name = input('Input file name\n')
    if name.endswith('.xlsx'):
        name = xlsx_to_csv(name)
    if name.endswith('.csv') and not name.count('[CHECKED]'):
        name = check_csv(name)
    if name.endswith('[CHECKED].csv'):
        name = database(name)
    if name.endswith('.s3db'):
        name = json_save(name)
    if name.endswith('.json'):
        xml_save(name)


if __name__ == '__main__':
    main()
