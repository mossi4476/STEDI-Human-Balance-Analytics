# STEDI-Human-Balance-Analytics

## Introduction
STEDI-Human-Balance-Analytics is a project aimed at analyzing and monitoring human balance through sensor data. This project utilizes machine learning algorithms to provide insights into balance capabilities and improve health outcomes.

## Data Processing
The project includes several scripts for data processing using AWS Glue. Below are the key scripts:

1. **Trainer_landing_to_trusted.py**: Loads data from the landing zone, filters it, and writes it to the trusted zone.
2. **Trainer_trusted_to_curated.py**: Joins trusted data with other datasets and writes the curated data for machine learning.
3. **Customer_landing_to_trusted.py**: Loads customer data from the landing zone, applies privacy filters, and writes it to the trusted zone.
4. **Customer_trusted_to_curated.py**: Joins customer trusted data with accelerometer data and writes the curated data.

## SQL Tables
The project also defines external tables in AWS Athena for querying the data:

- **accelerometer_landing**: Table for raw accelerometer data.
- **customer_landing**: Table for raw customer data.

## Contributing
We welcome contributions from the community. Please create a pull request or open an issue to discuss changes.





