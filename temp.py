import pycamunda.externaltask
import pandas
import csv
import json
import sys
url = 'http://localhost:8080/engine-rest'
worker_id = 'py'
variables = ['Input_2gb12m1']
id_='012620b8-8a77-11ec-885f-000c29524a92'  # variables of the process instance

fetch_and_lock = pycamunda.externaltask.FetchAndLock(url=url, worker_id=worker_id, max_tasks=10)
fetch_and_lock.add_topic(name='python', lock_duration=10000, variables=variables)
tasks = fetch_and_lock()

var=pycamunda.variable.Get(url=url, id_=id_)
directory=var().value
print(var().value)

csvFilePath = r'file.csv'
jsonFilePath = r'data.json'
jsonArray = []

with open(directory+'\\ИВЦ БНС, Данные по МСП, 2021, М12, Декабрь.xlsx',"rb") as f:
    
    excel_data_df = pandas.read_excel(f,header=1,skiprows=1)
    excel_data_df.to_csv("file.csv", header=1, index=False, encoding="utf-8")
    df=pandas.read_csv('file.csv',index_col=None)
    df1=df.iloc[0:19]
    table_name1=df1.columns[0]
    df2=df.iloc[20:40]
    df2.columns=df2.iloc[0]
    df2=df2[1:]
    table_name2=df2.columns[0]
    df3=df.iloc[41:61]
    df3.columns=df3.iloc[0]
    df3=df3[1:]
    table_name3=df3.columns[0]
    print(df1[df1.columns[0]].iloc[1])
    if (df1.columns[0]=="Юридические лица") or (df1.columns[0]=="Индивидуальные предприниматели") or (df1.columns[0]=="Крестьянско-фермерские хозяйства"):
        df1.columns=df1.iloc[0]
        df1=df1[1:]
        print(df1[df1.columns[0]].iloc[0])
    else:
            print('error')
            sys.exit()
    if (df2.columns[0]=="Юридические лица") or (df2.columns[0]=="Индивидуальные предприниматели") or (df2.columns[0]=="Крестьянско-фермерские хозяйства"):
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
        sys.exit()
        
def csv_to_json(csvFilePath, jsonFilePath):

    with open(csvFilePath, encoding='utf-8') as csvf: 
        csvReader = csv.DictReader(csvf) 
        
        for row in csvReader: 
            jsonArray.append(row)
  
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf: 
        jsonString = json.dumps(jsonArray, indent=4, ensure_ascii=False)
        jsonf.write(jsonString)
          
csv_to_json(csvFilePath, jsonFilePath)

'''
for task in tasks:
        complete = pycamunda.externaltask.Complete(url=url, id_=task.id_, worker_id=worker_id)
        if variables=='hello':
            complete.add_variable(name='ServiceTaskVariable', value='2')# Send this variable to the instance
            complete()
        else:
            print("error")'''