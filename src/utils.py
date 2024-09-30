import os
import sys

import pandas as pd
import numpy as np
import dill

from src.exception import CustomException
import src.logger


from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import cross_val_score




def save_object(file_path, obj):
    try:
        dir_path = os.path.dirname(file_path)
        
        os.makedirs(dir_path, exist_ok=True)
        
        with open(file_path, "wb") as file_Obj:
            dill.dump(obj, file_Obj)
            
            
    except Exception as e:
        raise CustomException(e,sys)
    
    

def calculate_adjusted_r2_score(r2, n, p):
    """
    Calculates the Adjusted R² score.

    Parameters:
        r2 (float): The R² score of the model.
        n (int): The number of samples.
        p (int): The number of features.

    Returns:
        float: The Adjusted R² score.
    """
    return 1 - (1 - r2) * (n - 1) / (n - p - 1)




def evaluate_models(X_train, X_test, y_train, y_test, models):
    """
    Evaluates multiple regression models based on R² score, Adjusted R² score, and cross-validation R² score.

    Parameters:
        X_train (np.ndarray): Training features.
        X_test (np.ndarray): Testing features.
        y_train (np.ndarray): Training target values.
        y_test (np.ndarray): Testing target values.
        models (dict): A dictionary of model names and corresponding model instances.

    Returns:
        tuple: Best model name, the best model instance, and its Adjusted R² score.
    """
    best_model_name = None
    best_model_score = -float('inf')  # Start with a very low score for maximization
    best_model = None

    n = X_train.shape[0]  # Number of samples
    p = X_train.shape[1]  # Number of features

    for name, model in models.items():
        try:
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)
            
            # Calculate evaluation metrics
            r2 = r2_score(y_test, y_pred)
            adjusted_r2 = calculate_adjusted_r2_score(r2, n, p)
            mae = mean_absolute_error(y_test, y_pred)
            mse = mean_squared_error(y_test, y_pred)
            
            # Cross-validation score
            cv_score = cross_val_score(model, X_train, y_train, cv=5, scoring='r2').mean()

            print(f"{name} - MAE: {mae}, MSE: {mse}, R^2: {r2}, Adjusted R^2: {adjusted_r2}, Cross-Validation R^2 Score: {cv_score}")

            # Compare and store the best model based on Adjusted R^2 score
            if adjusted_r2 > best_model_score:
                best_model_name = name
                best_model_score = adjusted_r2
                best_model = model

        except Exception as e:
            print(f"Error evaluating model {name}: {e}")

    return best_model_name, best_model, best_model_score




def load_object(file_path):
    """ to load pkl files"""
    
    try:
        with open(file_path, "rb") as file_obj:
            return dill.load(file_obj)
        
    except Exception as e:
        raise CustomException(e,sys)
    