import pandas as pd
from prefect import Flow, Parameter

from tasks import (
    download_data,
    parse_data,
    phone_number_normalization,
    same_country_state,
    save_report,
    stats
)

with Flow("Users report") as flow:

    # Parâmetros
    n_users = Parameter("n_users", default=10)

    #cols = ['gender', 'location.country','dob.age']
    cols1 = ['location.country', 'location.state', 'name.first']
    # Tasks
    data = download_data(n_users)
    dataframe = parse_data(data)
    normalized_df = phone_number_normalization(dataframe)
    save_report(normalized_df)

    df = pd.read_csv("report.csv")
    #same_country_state(df)
    #stats_df = pd.read_csv("report.csv",usecols=cols)
    #stats(stats_df)