import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'
def create_new_table():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    statement = 'DROP TABLE IF EXISTS Countries;'
    cur.execute(statement)
    conn.commit()

    statement = '''
        CREATE TABLE Countries (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Alpha2' TEXT,
            'Alpha3' TEXT,
            'EnglishName' TEXT NOT NULL,
            'Region' TEXT,
            'Subregion' TEXT,
            'Population'INTEGER,
            'Area' REAL
        );
    '''    
    cur.execute(statement)
    conn.commit()

    statement = 'DROP TABLE IF EXISTS Bars;' 
    cur.execute(statement)
    conn.commit()

    statement = '''
        CREATE TABLE Bars (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Company' TEXT NOT NULL,
            'SpecificBeanBarName' TEXT,
            'REF' TEXT NOT NULL,
            'ReviewDate' TEXT NOT NULL,
            'CocoaPercent' REAL,
            'CompanyLocation' TEXT,
            'CompanyLocationId' INTEGER REFERENCES Countries(Id),
            'Rating' REAL,
            'BeanType' TEXT,
            'BroadBeanOrigin' TEXT,
            'BroadBeanOriginId' INTEGER REFERENCES Countries(Id)
        );
    '''
    cur.execute(statement)
    conn.commit()

    conn.close()


def insert_data():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    with open(COUNTRIESJSON, encoding='utf8') as f:
        data = json.load(f)

    for item in data:
        insertion = (None, item['alpha2Code'], item['alpha3Code'], item['name'],item['region'],item['subregion'],item['population'],item['area'])
        statement = 'INSERT INTO Countries VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
        cur.execute(statement, insertion)
        conn.commit()
    
    with open(BARSCSV, 'r', encoding='utf-8') as f:
        csvReader = csv.reader(f)
        skip = True
        for row in csvReader:
            if skip:
                skip = False
                continue
            else:
                row[4] = row[4].replace('%', '')
                insertion = (None, row[0], row[1], row[2], row[3], row[4], row[5], None, row[6], row[7], row[8], None)
                statement = 'INSERT INTO Bars VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
                cur.execute(statement, insertion)
                conn.commit()
    
    conn.close()

def insert_Id():
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    statement = 'SELECT EnglishName, Id FROM Countries'
    cur.execute(statement)
    countries = cur.fetchall()

    for country in countries:
        statement = 'UPDATE Bars SET CompanyLocationId=' + str(country[1]) + ' WHERE CompanyLocation="' + country[0] + '"'
        cur.execute(statement)
        statement = 'UPDATE Bars SET BroadBeanOriginId=' + str(country[1]) + ' WHERE BroadBeanOrigin="' + country[0] + '"'
        cur.execute(statement) 

    conn.commit()
    conn.close()

create_new_table()
insert_data()
insert_Id()


# Part 2: Implement logic to process user commands
def process_command(command):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    command_list = command.split()
    if 'bars' in command_list:
        statement = 'SELECT SpecificBeanBarName, Company, CompanyLocation, round(Rating,1), CocoaPercent, BroadBeanOrigin FROM Bars JOIN Countries ON Bars.CompanyLocation = Countries.EnglishName'
        order_param = ' ORDER BY Bars.Rating DESC'
        limit_param = ' limit 10'
        sell_param = ''
        for word in command_list:
            if 'cocoa' in word:
                order_param = ' ORDER BY CocoaPercent DESC'
            elif 'top' in word:
                a, b = word.split('=')
                limit_param =' limit '+ str(b)
            elif 'bottom' in word:
                a, b = word.split('=')
                limit_param = ' limit '+ str(b)
                order_param = order_param.replace('DESC',' ')
            elif 'sellcountry' in word:
                a, b = word.split('=')
                statement_name = 'SELECT EnglishName FROM Countries'
                statement_name += ' WHERE alpha2 ="'+ str(b) + '"'
                result = cur.execute(statement_name)
                for i in result:
                    name = i[0]
                sell_param = ' WHERE CompanyLocation = "'+str(name)+'"'
            elif 'sellregion'in word:
                a,b = word.split('=')
                statement_region = '''SELECT EnglishName FROM Countries'''
                statement_region += ' WHERE Region ="'+str(b)+'"'
                region_result = cur.execute(statement_region)
                region_list = []
                for name in region_result:
                    region_list.append(name[0])
                sell_param =' WHERE CompanyLocation in ' + '{}'.format(tuple(region_list))
            elif 'sourcecountry'in word:
                a, b = word.split('=')
                statement_name = '''SELECT EnglishName FROM Countries'''
                statement_name = ' WHERE Alpha2 = "' + str(b) + '"'
                result = cur.execute(statement_name)
                for i in result:
                    name = i[0]
                sell_param =' WHERE BroadBeanOrigin = "' + str(name) + '"'
            elif 'sourceregion' in word:
                a, b = word.split('=')
                statement_region = 'SELECT EnglishName FROM Countries'
                statement_region += ' WHERE Region ="' + str(b) + '"'
                region_result = cur.execute(statement_region)
                region_list = []
                for name in region_result:
                    region_list.append(name[0])
                sell_param =' WHERE BroadBeanOrigin in '+'{}'.format(tuple(region_list))
        statement += sell_param
        statement += order_param
        statement += limit_param
    if 'companies' in command_list:
        statement_init = 'SELECT Bars.Company, Bars.CompanyLocation,round(AVG(bars.Rating),1) FROM Bars JOIN Countries ON Bars.CompanyLocation = Countries.EnglishName'
        order_param = ' ORDER BY AVG(bars.Rating) DESC'
        limit_param = ' limit 10'
        region_param = ''
        country_param = ''
        num_limit = ' having COUNT(Bars.Id) > 4'
        group_param =' GROUP BY Bars.Company'

        for word in command_list:
            if  'country' in word:
                a,b = word.split('=')
                statement_name = '''SELECT EnglishName FROM Countries'''
                statement_name+= ' WHERE alpha2 ="'+str(b)+'"'
                result=cur.execute(statement_name)
                for i in result:
                    name = i[0]
                country_param = ' WHERE CompanyLocation = "'+str(name)+'"'
            elif 'region' in word:
                a,b = word.split('=')
                region_param= ' WHERE Countries.Region ="'+str(b)+'"'

            elif 'cocoa' in word:
                statement_init = '''SELECT Company, CompanyLocation,round(AVG(CocoaPercent),0) FROM Bars JOIN Countries ON bars.CompanyLocation = Countries.EnglishName'''
                order_param = ' ORDER BY AVG(CocoaPercent) DESC'

            elif 'bars_sold' in word:
                statement_init = '''SELECT Company, CompanyLocation,COUNT(Bars.Id) FROM Bars JOIN Countries ON bars.CompanyLocation = Countries.EnglishName'''
                order_param = ' ORDER BY COUNT(Bars.Id) DESC'
            elif 'top' in word:
                a,b = word.split('=')
                limit_param =' limit '+str(b)

            elif 'bottom' in word:
                a,b = word.split('=')
                limit_param = ' limit '+str(b)
                order_param = order_param.replace('DESC',' ')

        statement = statement_init + region_param + country_param + group_param + num_limit+ order_param+ limit_param

    if 'countries' in command_list:
        region_param = ''
        rating_param = ' round(AVG(Bars.Rating),1)'
        order_param = ' ORDER By AVG(Bars.Rating) DESC'
        limit_param = ' LIMIT 10'
        num_limit = ' HAVING COUNT(Bars.Id) > 4'
        group_param =' GROUP BY Countries.EnglishName'
        sources_param = ' Bars.CompanyLocation = Countries.EnglishName'
        for word in command_list:
            if 'region' in word:
                a,b = word.split('=')
                region_param = ' WHERE Countries.Region = "'+str(b)+'"'
            elif 'sources' in word:
                sources_param = ' Bars.BroadBeanOrigin = Countries.EnglishName'
            elif 'cocoa' in word:
                rating_param = ' round(AVG(Bars.CocoaPercent),0)'
                order_param = ' ORDER BY AVG(Bars.CocoaPercent) DESC'
            elif 'bars_sold' in word:
                rating_param = ' COUNT(Bars.Id)'
                order_param = ' ORDER BY COUNT(Bars.Id) DESC'
            elif 'top' in word:
                a,b = word.split('=')
                limit_param = ' limit '+str(b)
            elif 'bottom' in word:
                a,b = word.split('=')
                limit_param = ' limit '+str(b)
                order_param = order_param.replace('DESC',' ')

        statement = "SELECT Countries.EnglishName, Countries.Region, " +rating_param +" FROM Countries JOIN Bars ON"+sources_param
        statement+=region_param + group_param + num_limit + order_param + limit_param

    if 'regions' in command_list:
        for word in command_list:
            source_param = ' Bars.CompanyLocation = Countries.EnglishName'
            rating_param = ' round(AVG(bars.Rating),1)'
            order_param = ' ORDER By AVG(bars.Rating) DESC'
            limit_param = ' limit 10'
            num_limit = ' having COUNT(Bars.Id) >= 4'
            group_param =' GROUP BY Countries.Region'
            for word in command_list:
                if 'sources' in word:
                    source_param = ' Bars.BroadBeanOrigin = Countries.EnglishName'

                elif 'cocoa' in word:
                    rating_param = ' round(AVG(Bars.CocoaPercent),0)'
                    order_param = ' ORDER BY AVG(Bars.CocoaPercent) DESC'
                elif 'bars_sold' in word:
                    rating_param = ' COUNT(Bars.Id)'
                    order_param = ' ORDER BY COUNT(Bars.Id) DESC'
                elif 'top' in word:
                    a,b = word.split('=')
                    limit_param = ' limit '+str(b)
                elif 'bottom' in word:
                    a,b = word.split('=')
                    limit_param = ' limit '+str(b)
                    order_param = order_param.replace('DESC',' ')

        statement = "SELECT Countries.Region," +rating_param +" FROM Countries JOIN Bars ON" + source_param
        statement+= group_param + num_limit + order_param + limit_param
    result_list = []
    search_result = cur.execute(statement)
    for row in search_result:
        result_list.append(row)
    conn.close()
    return result_list


def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!

def interactive_prompt():
    help_text = load_help_text()
    response = ''
    param_list = ['countries','bars','regions','companies','top','bottom','ratings','cocoa','bars_sold','sources','sellers','region','country','sourceregion','sourcecountry','sellregion','sellcountry']
    while response != 'exit':
        response = input('Enter a command: ')
        response_list = response.split()

        for item in response_list:
            i=item.split('=')
            if i[0] in param_list:
                valid = True
            else:
                valid = False
                break

        if response == 'help':
            print(help_text)
            continue
        elif valid:
            if 'countries' in response_list:
                result_show=process_command(response)
                for i in result_show:
                    s = [" %-12s ", '   '," %-12s ", '   '," %-12s ", '   ']
                    count=0
                    for j in i:
                        count+=1
                        if len(str(j))>12:
                            s[2*count-1]='...'
                    if 'cocoa' in response:
                        print(''.join(s) % (i[0][:12],i[1][:12],str(i[2])+'%'))
                    else:
                        print(''.join(s) % (i[0][:12],i[1][:12],i[2]))
                print('\n')

            if 'bars' in response_list:
                result_show=process_command(response)
                for i in result_show:
                    s = [" %-12s ", '   '," %-12s ", '   '," %-12s ", '   '," %-12s ", '   '," %-12s ", '   '," %-12s ", '  ']
                    count=0
                    for j in i:
                        count+=1
                        if len(str(j))>12:
                            s[2*count-1]='...'

                    print(''.join(s) % (i[0][:12],i[1][:12],i[2][:12],i[3],str(i[4])+'%',i[5][:12]))
                print('\n')
            if 'companies' in response_list:
                result_show=process_command(response)
                for i in result_show:
                    s = [" %-12s ", '   '," %-12s ", '   '," %-12s ", '   ']
                    count=0
                    for j in i:
                        count+=1
                        if len(str(j))>12:
                            s[2*count-1]='...'
                    if 'cocoa' in response:
                        print(''.join(s) % (i[0][:12],i[1][:12],str(i[2])+'%'))
                    else:
                        print(''.join(s) % (i[0][:12],i[1][:12],i[2]))
                print('\n')
            if 'regions' in response_list:
                result_show=process_command(response)
                for i in result_show:
                    s = [" %-12s ", '   '," %-12s ", '   ']
                    count=0
                    for j in i:
                        count+=1
                        if len(str(j))>12:
                            s[2*count-1]='...'
                    if 'cocoa' in response:
                        print(''.join(s) % (i[0][:12],str(i[1])+'%'))
                    else:
                        print(''.join(s) % (i[0][:12],i[1]))
                print('\n')
        elif response =='exit':
            print('bye~')

        else:
            print('Command not recognized: ', response)

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
