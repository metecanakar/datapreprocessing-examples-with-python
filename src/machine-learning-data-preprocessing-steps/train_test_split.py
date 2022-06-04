from sklearn.model_selection import train_test_split


def split_data_into_train_and_test(df_ulke_and_numerical_columns, df_cinsiyet):
    split_percentage = 0.33
    # make sure we always get the same split results across different calls
    seed = 0

    # first param: X (independent variables (bagimsiz degiskenler)),
    # second param: y (dependent variable (bagimli degisken) (cinsiyet))
    X_train, X_test, y_train, y_test = train_test_split(df_ulke_and_numerical_columns, df_cinsiyet,
                                                        test_size=split_percentage,
                                                        random_state=seed)

    return X_train, X_test, y_train, y_test
