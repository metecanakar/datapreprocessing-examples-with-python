from sklearn.impute import SimpleImputer
from sklearn import preprocessing
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
    imputed_df : Imputed numpy array

    """

    # get all the rows and only the columns with numerical type (between 1st(inclusive) and 4th(exlusive) columns)
    numerical_cols = df.iloc[:, 1:4].values

    # create an imputer instance using strategy mean
    imputer_mean = SimpleImputer(missing_values=np.nan, strategy="mean")

    # imputer learns
    imputer_mean.fit(numerical_cols)
    # imputer replaces the null values with the mean values in that column
    numerical_cols_mean_imputed = imputer_mean.transform(numerical_cols)

    return numerical_cols_mean_imputed


def merge_all_into_final_df():
    pass
