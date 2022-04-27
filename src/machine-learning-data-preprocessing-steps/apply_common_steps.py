from sklearn.impute import SimpleImputer
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

    # get only columns with numerical type (between 1st(inclusive) and 4th(exlusive) columns)
    numerical_cols = df.iloc[:, 1:4]

    imputer_mean = SimpleImputer(missing_values=np.nan, strategy="mean")
    imputed_df = imputer_mean.fit_transform(numerical_cols)

    return imputed_df


if __name__ == "__main__":
    df_path = "../data/input/eksikveriler.csv"

    df_eksikveriler = pd.read_csv(df_path).sort_values(by=["ulke", "boy", "kilo", "cinsiyet"])
    print(f"Original dataframe {df_eksikveriler}")

    imputed_df_eksikveriler = handle_missing_by_mean(df_eksikveriler)
    print(f"Imputed dataframe {imputed_df_eksikveriler}")
