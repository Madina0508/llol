import pycamunda.externaltask
import pandas
import csv
import json
import sys
import os
import sqlite3
import openpyxl

url = 'http://localhost:8080/engine-rest'
worker_id = 'py'
variables = ['Input_2gb12m1']
id_='012620b8-8a77-11ec-885f-000c29524a92'  # variables of the process instance

fetch_and_lock = pycamunda.externaltask.FetchAndLock(url=url, worker_id=worker_id, max_tasks=10)
fetch_and_lock.add_topic(name='python', lock_duration=10000, variables=variables)
tasks = fetch_and_lock()

var=pycamunda.variable.GetList(url=url, name=variables)
#var=pycamunda.variable.Get(url=url, id_=id_)
#directory=var().value
directory=var()[0].value
print(var()[0].value)


csvFilePath = r'file.csv'
jsonFilePath = r'data.json'
jsonArray = []

for task in tasks:
    complete = pycamunda.externaltask.Complete(url=url, id_=task.id_, worker_id=worker_id)

    with open('11.xlsx',"rb") as f:
    
        excel_data_df = pandas.read_excel(f,header=1,skiprows=1)
        print(excel_data_df)
        excel_data_df.to_csv("file.csv", header=1, index=False, encoding="utf-8")
        df=pandas.read_csv('file.csv',index_col=None)
        #df=df.drop(df.columns[[0]], axis = 1)
        print(df)
        df1=df.iloc[0:19]
            
        if (df1.columns[0]=="Юридические лица") or (df1.columns[0]=="Индивидуальные предприниматели") or (df1.columns[0]=="Крестьянско-фермерские хозяйства"):
            df1.columns=df1.iloc[0]
            df1=df1[1:]
            df2=df.iloc[20:40]
            #print(df1)
        else:
            print('error')
            complete.add_variable(name='ServiceTaskVariable', value='err')# Send this variable to the instance
            complete()
            sys.exit()
        if (df2.columns[0]=="Юридические лица") or (df2.columns[0]=="Индивидуальные предприниматели") or (df2.columns[0]=="Крестьянско-фермерские хозяйства"):
            df2.columns=df2.iloc[1]
            df2=df2[2:]
            #print(df2)
            df3=df.iloc[41:61]
        
        else:
            print('error')
            complete.add_variable(name='ServiceTaskVariable', value='err')# Send this variable to the instance
            complete()
            sys.exit()
        if (df3.columns[0]=="Юридические лица") or (df3.columns[0]=="Индивидуальные предприниматели") or (df3.columns[0]=="Крестьянско-фермерские хозяйства"):
            df3.columns=df3.iloc[1]
            df3=df3[2:]
            #print(df3)
            df=pandas.concat([df1,df2,df3],ignore_index=True)
            df.to_csv("file1.csv", header=1, index=False, encoding="utf-8")
            df=pandas.read_csv('file1.csv',index_col=None)
            #print(df)
            #print(df['всего'])
        else:
            print('error')
            complete.add_variable(name='ServiceTaskVariable', value='err')# Send this variable to the instance
            complete()
            sys.exit()
        
        if (df[df.columns[0]].iloc[0]=="Республика Казахстан" and 
        df[df.columns[0]].iloc[1]=="Акмолинская область" and
        df[df.columns[0]].iloc[2]=="Актюбинская область" and
        df[df.columns[0]].iloc[3]=="Алматинская область" and
        df[df.columns[0]].iloc[4]=="Атырауская область" and
        df[df.columns[0]].iloc[5]=="Западно-Казахстанская область" and
        df[df.columns[0]].iloc[6]=="Жамбылская область" and
        df[df.columns[0]].iloc[7]=="Карагандинская область " and
        df[df.columns[0]].iloc[8]=="Костанайская область" and
        df[df.columns[0]].iloc[9]=="Кызылординская область" and
        df[df.columns[0]].iloc[10]=="Мангистауская область" and
        df[df.columns[0]].iloc[11]=="Павлодасркая область" and
        df[df.columns[0]].iloc[12]=="Северо-Казахстанская область" and
        df[df.columns[0]].iloc[13]=="Туркестанская область" and
        df[df.columns[0]].iloc[14]=="Восточно-Казахстанская область" and
        df[df.columns[0]].iloc[15]=="г.Нур-Султан" and
        df[df.columns[0]].iloc[16]=="г.Алматы" and
        df[df.columns[0]].iloc[17]=="г.Шымкент" and
            df[df.columns[0]].iloc[18]=="Республика Казахстан" and 
            df[df.columns[0]].iloc[19]=="Акмолинская область" and
            df[df.columns[0]].iloc[20]=="Актюбинская область" and
            df[df.columns[0]].iloc[21]=="Алматинская область" and
            df[df.columns[0]].iloc[22]=="Атырауская область" and
            df[df.columns[0]].iloc[23]=="Западно-Казахстанская область" and
            df[df.columns[0]].iloc[24]=="Жамбылская область" and
            df[df.columns[0]].iloc[25]=="Карагандинская область " and
            df[df.columns[0]].iloc[26]=="Костанайская область" and
            df[df.columns[0]].iloc[27]=="Кызылординская область" and
            df[df.columns[0]].iloc[28]=="Мангистауская область" and
            df[df.columns[0]].iloc[29]=="Павлодасркая область" and
            df[df.columns[0]].iloc[30]=="Северо-Казахстанская область" and
            df[df.columns[0]].iloc[31]=="Туркестанская область" and
            df[df.columns[0]].iloc[32]=="Восточно-Казахстанская область" and
            df[df.columns[0]].iloc[33]=="г.Нур-Султан" and
            df[df.columns[0]].iloc[34]=="г.Алматы" and
            df[df.columns[0]].iloc[35]=="г.Шымкент" and
                df[df.columns[0]].iloc[36]=="Республика Казахстан" and 
                df[df.columns[0]].iloc[37]=="Акмолинская область" and
                df[df.columns[0]].iloc[38]=="Актюбинская область" and
                df[df.columns[0]].iloc[39]=="Алматинская область" and
                df[df.columns[0]].iloc[40]=="Атырауская область" and
                df[df.columns[0]].iloc[41]=="Западно-Казахстанская область" and
                df[df.columns[0]].iloc[42]=="Жамбылская область" and
                df[df.columns[0]].iloc[43]=="Карагандинская область " and
                df[df.columns[0]].iloc[44]=="Костанайская область" and
                df[df.columns[0]].iloc[45]=="Кызылординская область" and
                df[df.columns[0]].iloc[46]=="Мангистауская область" and
                df[df.columns[0]].iloc[47]=="Павлодасркая область" and
                df[df.columns[0]].iloc[48]=="Северо-Казахстанская область" and
                df[df.columns[0]].iloc[49]=="Туркестанская область" and
                df[df.columns[0]].iloc[50]=="Восточно-Казахстанская область" and
                df[df.columns[0]].iloc[51]=="г.Нур-Султан" and
                df[df.columns[0]].iloc[52]=="г.Алматы" and
                df[df.columns[0]].iloc[53]=="г.Шымкент" 
                and (df['всего'].dtype.kind=='i' or df['всего'].dtype.kind=='f') 
                and (df['вновь зарегистрированное'].dtype.kind=='i' or df['вновь зарегистрированное'].dtype.kind=='f')
                and (df['активное'].dtype.kind=='i' or df['активное'].dtype.kind=='f')
                and (df['временно приостановившее деятельность'].dtype.kind=='i' or df['временно приостановившее деятельность'].dtype.kind=='f')
                and (df['нет информации или бездействующий'].dtype.kind=='i' or df['нет информации или бездействующий'].dtype.kind=='f')
                and (df['Приостановившее деятельность'].dtype.kind=='i' or df['Приостановившее деятельность'].dtype.kind=='f')
                and (df['в процессе ликвидации'].dtype.kind=='i' or df['в процессе ликвидации'].dtype.kind=='f')):
            print('ok')
            con = sqlite3.connect('test.db')
            cur = con.cursor()
            columns=['Регионы',	'всего',	'вновь зарегистрированное',	'активное',	'временно приостановившее деятельность',	'нет информации или бездействующий',	'Приостановившее деятельность',	'в процессе ликвидации']
            df=pandas.read_csv('file.csv',index_col=None,header = None, names = columns)
            df.to_sql('user', con=con, if_exists='append', index=False)
            con.commit()
            complete.add_variable(name='ServiceTaskVariable', value='ok')# Send this variable to the instance
            complete()
        else:
            print('error')
            complete.add_variable(name='ServiceTaskVariable', value='err')# Send this variable to the instance
            complete()
            sys.exit()
    '''if (df2.columns[0]=="Юридические лица") or (df2.columns[0]=="Индивидуальные предприниматели") or (df2.columns[0]=="Крестьянско-фермерские хозяйства"):
        df2.columns=df2.iloc[0]
        df2=df2[1:]
    else:
            print('error')
            sys.exit()
    if (df3.columns[0]=="Юридические лица") or (df3.columns[0]=="Индивидуальные предприниматели") or (df3.columns[0]=="Крестьянско-фермерские хозяйства"):
        df3.columns=df3.iloc[0]
        df3=df3[1:]
        df=pandas.concat([df1,df2,df3],ignore_index=True)
        df.to_csv("file.csv", header=1, index=False, encoding="utf-8")
    else:
        print('error')
        sys.exit()'''
    
'''def csv_to_json(csvFilePath, jsonFilePath):

    with open(csvFilePath, encoding='utf-8') as csvf: 
        csvReader = csv.DictReader(csvf) 
        
        for row in csvReader: 
            jsonArray.append(row)
  
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf: 
        jsonString = json.dumps(jsonArray, indent=4, ensure_ascii=False)
        jsonf.write(jsonString)
          
csv_to_json(csvFilePath, jsonFilePath)'''

'''
for task in tasks:
        complete = pycamunda.externaltask.Complete(url=url, id_=task.id_, worker_id=worker_id)
        if variables=='hello':
            complete.add_variable(name='ServiceTaskVariable', value='2')# Send this variable to the instance
            complete()
        else:
            print("error")'''