"""
demand_prediction.py

Machine Learning Models for Supply Chain Predictive Analytics
"""

import pandas as pd
import numpy as np
import logging
import joblib

from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestRegressor, GradientBoostingClassifier
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, accuracy_score
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.arima.model import ARIMA
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from tensorflow.keras.optimizers import Adam

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Generate sample data
def generate_sample_demand_data(n=365):
    logging.info("Generating sample time series demand data.")
    date_rng = pd.date_range(start='1/1/2023', periods=n, freq='D')
    demand = np.random.poisson(lam=200, size=n)
    weather = np.random.randint(60, 100, size=n)
    economy_index = np.random.normal(100, 10, size=n)
    df = pd.DataFrame({'date': date_rng, 'demand': demand, 'weather': weather, 'economy_index': economy_index})
    df.set_index('date', inplace=True)
    return df

# Time Series Forecasting with ARIMA
def arima_forecast(df):
    logging.info("Running ARIMA model for demand forecasting.")
    model = ARIMA(df['demand'], order=(5,1,0))
    model_fit = model.fit()
    forecast = model_fit.forecast(steps=30)
    return forecast

# Neural Network Forecasting with LSTM
def lstm_forecast(df):
    logging.info("Running LSTM model for demand forecasting.")
    data = df['demand'].values.reshape(-1, 1)
    scaler = StandardScaler()
    data_scaled = scaler.fit_transform(data)
    
    X, y = [], []
    for i in range(10, len(data_scaled)):
        X.append(data_scaled[i-10:i])
        y.append(data_scaled[i])
    
    X, y = np.array(X), np.array(y)

    model = Sequential()
    model.add(LSTM(50, activation='relu', input_shape=(X.shape[1], 1)))
    model.add(Dense(1))
    model.compile(optimizer=Adam(), loss='mse')
    model.fit(X, y, epochs=5, verbose=0)

    last_sequence = data_scaled[-10:].reshape((1, 10, 1))
    forecast = model.predict(last_sequence, verbose=0)
    return scaler.inverse_transform(forecast)

# Inventory Optimization: EOQ
def calculate_eoq(demand_rate, setup_cost, holding_cost):
    eoq = np.sqrt((2 * demand_rate * setup_cost) / holding_cost)
    return round(eoq, 2)

# Supplier Risk Scoring
def train_supplier_risk_model():
    logging.info("Training supplier risk classification model.")
    df = pd.DataFrame({
        'financial_score': np.random.rand(100),
        'geo_risk': np.random.rand(100),
        'delivery_delay': np.random.rand(100),
        'label': np.random.choice([0, 1], size=100)
    })
    X = df[['financial_score', 'geo_risk', 'delivery_delay']]
    y = df['label']
    model = GradientBoostingClassifier()
    model.fit(X, y)
    joblib.dump(model, 'supplier_risk_model.pkl')
    return model

# Evaluation
def evaluate_model(model, X, y):
    logging.info("Evaluating model performance.")
    preds = model.predict(X)
    return accuracy_score(y, preds)

# Main Execution
if __name__ == "__main__":
    df = generate_sample_demand_data()
    forecast_arima = arima_forecast(df)
    forecast_lstm = lstm_forecast(df)
    eoq = calculate_eoq(10000, 500, 2)

    logging.info(f"ARIMA Forecast (next 5 days):\\n{forecast_arima.head()}")
    logging.info(f"LSTM Forecast (next day): {forecast_lstm.flatten()[0]:.2f}")
    logging.info(f"Calculated EOQ: {eoq}")

    model = train_supplier_risk_model()
    logging.info("Supplier risk model saved as 'supplier_risk_model.pkl'.")