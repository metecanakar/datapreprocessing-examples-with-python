from sklearn import preprocessing


def apply_standard_scaling(x_train, x_test):
    """
    Apply standard scaling to x_train and x_test.
    The reason why we need scaling is that every feature (column) in x_train (boy, kilo, yas) have very different
    mean, max and min values. In the next steps, if we want to train an ML model a feature with very large max and mean values
    can have much larger effect (e.g. boy) than a feature with very small mean (e.g. yas).
    In order to eliminate that we need to apply scaling before providing these features to our model.
    TL;DR
    Scaling is done to transform the features coming from different worlds into the same world.
    Args:
        x_train:
        x_test:

    Returns:
        Scaled X_train and X_test

    """
    scaler = preprocessing.StandardScaler()
    # scale x_train and assign to X_train
    X_train = scaler.fit_transform(x_train)

    # scale x_test and assign to X_test
    X_test = scaler.fit_transform(x_test)

    return X_train, X_test
