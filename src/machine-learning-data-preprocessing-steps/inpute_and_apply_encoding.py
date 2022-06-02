from sklearn.impute import SimpleImputer
from sklearn import preprocessing
import pandas as pd
import numpy as np


def handle_missing_by_mean(df):
    """
    Handles missing values by calculating mean.
    Parameters
    ----------
    df : Pandas Dataframe
        Handle missing values by taking the mean value in the given dataframe.
        Dataframe can contain multiple columns.

    Returns
    -------
    imputed_df : Pandas dataframe
        Imputed and transformed dataframe with handled missing values.

    """

    # get all the rows and only the columns with numerical type (between 1st(inclusive) and 4th(exlusive) columns)
    numerical_cols = df.iloc[:, 1:4].values

    imputer_mean = SimpleImputer(missing_values=np.nan, strategy="mean")

    # imputer learns
    imputer_mean.fit(numerical_cols)
    # replace the null values with the mean values in that column
    numerical_cols_mean_imputed = imputer_mean.transform(numerical_cols)

    return numerical_cols_mean_imputed


def encode_nominal_features_using_label_encoder(imputed_df):
    ulke_column = imputed_df[:, 0:1]

    le = preprocessing.LabelEncoder()

    le.fit_transform()

    pass


def encode_nominat_feature_using_one_hot_encoder(inputed_df):
    pass


def merge_all_into_final_df():
    pass


if __name__ == "__main__":
    df_path = "../data/input/missing_values.csv"

    df_missing_values = pd.read_csv(df_path).sort_values(by=["ulke", "boy", "kilo", "cinsiyet"])
    print(f"Original dataframe {df_missing_values}")

    imputed_df = handle_missing_by_mean(df_missing_values)
    print(f"Imputed dataframe {imputed_df}")

    encode_nominal_features_using_label_encoder(imputed_df)

    merge_all_into_final_df()
