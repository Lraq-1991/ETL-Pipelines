-- Create table customers

DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    CustomerId INT PRIMARY KEY,
    Surname TEXT,
    CreditScore TEXT,
    Geography TEXT,
    Gender TEXT,
    Age INT,
    Tenure  INT,
    Balance INT,
    NumOfProducts INT,
    HasCrCard INT,
    IsActiveMember INT ,
    EstimatedSalary REAL,
    Exited INT
);