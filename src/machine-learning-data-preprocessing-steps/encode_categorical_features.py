"""Transform categorical (nominal and ordinal) features
 to numerical features using multiple encoders"""
from sklearn import preprocessing


def encode_categorical_features_using_label_encoder(df_missing_values):
    """
    Label encoder is used for ordinal data types (e.g Tshirt size S, M, L or XL)
    Args:
        df_missing_values: Pandas dataframe

    Returns:

    """

    # pd dataframe without .values
    # ulke_column = df_missing_values.iloc[:, 0:1]
    # numpy ndarray with .values
    ulke_column = df_missing_values.iloc[:, 0:1].values

    le = preprocessing.LabelEncoder()

    ulke_column_lbl_encoded = le.fit_transform(ulke_column)

    return ulke_column_lbl_encoded


def encode_categorical_features_using_one_hot_encoder(df_missing_values):
    """
    Apply one hot encoding values converted to columns. Each label contains 1 or 0.
    Args:
        df_missing_values:

    Returns:

    """
    ulke_column = df_missing_values.iloc[:, 0:1].values

    ohe = preprocessing.OneHotEncoder()
    # fit and transform in 2 steps which is equivalent to fit_transform in one step
    # ohe_encoder = ohe.fit(ulke_column)
    # ulke_column_ohe_encoded = ohe_encoder.transform(ulke_column).toarray()

    # fit and transform and finally convert to numpy array
    ulke_column_ohe_encoded_fit_transformed = ohe.fit_transform(ulke_column).toarray()

    return ulke_column_ohe_encoded_fit_transformed
