""" Combine all the datapreprocessing steps """

import pandas as pd
from inpute_missing_values import handle_missing_by_mean
from encode_categorical_features import encode_nominal_features_using_label_encoder, \
    encode_nominat_feature_using_one_hot_encoder
from train_test_split import split_data_into_train_and_test


def merge_dataframes_and_return_multiple_combinations(df_missing_values, imputed_columns, ulke_column_ohe_encoded):
    """
    Merge the inputed and categorical encoded columns to numerical values
     (numpy arrays) into 1 dataframe.
    Returns:
    """

    df_ulke_columns_ohe = pd.DataFrame(data=ulke_column_ohe_encoded, columns=["fr", "tr", "us"])
    print(df_ulke_columns_ohe)

    df_boy_kilo_yas = pd.DataFrame(data=imputed_columns, columns=["boy", "kilo", "yas"])
    print(df_boy_kilo_yas)

    # contains all rows of the last cinsiyet column
    cinsiyet_arr = df_missing_values.iloc[:, -1].values
    df_cinsiyet = pd.DataFrame(data=cinsiyet_arr, columns=["cinsiyet"])
    print(df_cinsiyet)

    # by setting axis=1 the second dataframe is added as a new column while following the order of the first dataframe
    df_ulke_and_numerical_columns = pd.concat([df_ulke_columns_ohe, df_boy_kilo_yas], axis=1)

    df_final = pd.concat([df_ulke_and_numerical_columns, df_cinsiyet], axis=1)
    print(df_final)

    return df_cinsiyet, df_ulke_and_numerical_columns, df_final


if __name__ == "__main__":
    df_path = "../data/input/missing_values.csv"

    df_missing_values = pd.read_csv(df_path).sort_values(by=["ulke", "boy", "kilo", "cinsiyet"])
    print(f"Original dataframe {df_missing_values}")

    imputed_columns = handle_missing_by_mean(df_missing_values)
    print(f"Imputed columns {imputed_columns}")

    ulke_column_lbl_encoded = encode_nominal_features_using_label_encoder(df_missing_values)

    ulke_column_ohe_encoded = encode_nominat_feature_using_one_hot_encoder(df_missing_values)
    print(f"One hot encoded categorical ulke column {ulke_column_ohe_encoded}")

    df_cinsiyet, df_ulke_and_numerical_columns, df_final = merge_dataframes_and_return_multiple_combinations(
        df_missing_values,
        imputed_columns,
        ulke_column_ohe_encoded)

    X_train, X_test, y_train, y_test = split_data_into_train_and_test(df_ulke_and_numerical_columns,
                                                                      df_cinsiyet)
