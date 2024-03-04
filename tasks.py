from io import StringIO

import pandas as pd
from prefect import task
import requests
import matplotlib.pyplot as plt

from utils import log

@task
def download_data(n_users: int) -> str:

    url = "https://randomuser.me/api/?results={}&format=csv"
    response = requests.get(
        url.format(n_users)
    )
    log("Dados de " + url  + " baixados com sucesso!")
    return response.text

@task
def parse_data(data: str) -> pd.DataFrame:

    df = pd.read_csv(StringIO(data))
    log("Dados convertidos em DataFrame com sucesso!")
    return df

@task
def save_report(dataframe: pd.DataFrame) -> None:
    dataframe.to_csv("report.csv", index=False)
    log("Dados salvos em report.csv com sucesso!")

@task
def phone_number_normalization(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe['phone'] = dataframe['phone'].str.replace('(','')
    dataframe['phone'] = dataframe['phone'].str.replace(')', '')
    dataframe['phone'] = dataframe['phone'].str.replace('-', '')
    log("Número de Telefone normalizado com sucesso!")
    return dataframe

@task
def stats(dataframe: pd.DataFrame) -> None:

    males_perc = (len(dataframe[dataframe['gender'] == 'male']) / len(dataframe)) * 100
    females_perc = (len(dataframe[dataframe['gender'] == 'female']) / len(dataframe)) * 100

    data1 = [['Male', males_perc],['Female',females_perc]]
    df1 = pd.DataFrame(data1,columns=['gender', 'percentage'])
    df1.to_csv('stats.csv')

    countries = dataframe['location.country'].unique()
    data2 = [[] for row in range(len(countries))]
    i = 0

    for country in countries:
        country_perc = (len(dataframe[dataframe['location.country'] == country ]) / len(dataframe)) * 100
        data2[i].append(country)
        data2[i].append(country_perc)
        i += 1

    df2 = pd.DataFrame(data2, columns=['country','percentage'])
    df2.to_csv('stats.csv',mode='a')
    log("Arquivo de estatística criado com sucesso!")

    print(dataframe['dob.age'].to_string())
    dataframe['dob.age'].plot(kind='kde')
    plt.xlabel('Age')
    plt.savefig('age_graph.png')

    #plt.show()

@task
def same_country_state(dataframe: pd.DataFrame) -> pd.DataFrame:

    df = pd.DataFrame(dataframe.groupby(['location.state', 'location.country'])['name.first'].count())

    df.to_csv('data.csv')

    log("Agrupamento feito com sucesso!")