#!/usr/bin/env python
# coding: utf-8

# In[128]:


import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.arima.model import ARIMA


# In[136]:


df = pd.read_csv('claims_data.csv')
print(df)


# In[130]:


df.head()


# In[73]:


print(df.describe())


# In[14]:


print(df.isnull())


# In[137]:


df['PAID_AMOUNT'] = df['PAID_AMOUNT'].abs()
print(df['PAID_AMOUNT'])


# In[138]:


# Функция преобразования
def convert_to_date(month_int):
    date_str = str(month_int)
    year = date_str[:4]
    month = date_str[4:6]
    if month not in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
        return None  # возвращает None для невалидных месяцев
    return pd.to_datetime(f'{year}-{month}-01')

# Применение функции к столбцу
df['DATE'] = df['MONTH'].apply(convert_to_date)

# Удаление строк с невалидными месяцами (где значение даты равно None)
df = df.dropna(subset=['DATE'])
#df.set_index('DATE', inplace=True)
print(df)


# In[140]:


monthly_expenses = df.groupby('DATE').sum()['PAID_AMOUNT'].abs()
monthly_expenses.plot()
plt.show()
print(monthly_expenses)


# In[141]:


# Обработка пропущенных значений (если они есть)
df.dropna(inplace=True)

# Проверка стационарности ряда

result = adfuller(df['PAID_AMOUNT'])
print('ADF статистика:', result[0])
print('p-значение:', result[1])
print('Критические значения:')
for key, value in result[4].items():
    print(f'  {key}: {value}')


# In[142]:


train_data = monthly_expenses
model = ARIMA(train_data, order=(1,1,1))
model_fit = model.fit()
forecast = model_fit.forecast(steps=6)
print(forecast)
print(model_fit.summary())


# In[145]:


# Прогноз на будущее (следующие 12 месяцев)
forecast_future = model_fit.forecast(steps=6)

# Создаем новый DataFrame для будущих значений
future_dates = pd.date_range(start='2020-08-01', periods=6, freq='M')
forecast_df = pd.DataFrame({'DATE1': future_dates, 'FUTURE_AMOUNT': forecast_future})
print(forecast_df)
# Присоединяем прогноз к исходному DataFrame
df = df.append(forecast_df, ignore_index=True)
print(df)
# Визуализация исходных данных и прогноза
plt.figure(figsize=(12, 6))
plt.plot(df['DATE'][:-6], df['PAID_AMOUNT'][:-6], label='Исходные данные')
plt.plot(df['DATE1'][-6:], df['FUTURE_AMOUNT'][-6:], label='Прогноз')
plt.title('Прогноз')
plt.xlabel('DATE')
plt.ylabel('PAID_AMOUNT')
plt.legend()
plt.grid(True)
plt.show()


# In[147]:


df.to_csv('claims_data_pred')


# In[ ]:




