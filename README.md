# 🇰🇪 Kenya Real Estate ETL Pipeline

A production-ready Data Engineering pipeline designed to automatically extract, transform, and load (ETL) real estate property listings into a PostgreSQL database.

The pipeline is orchestrated using **Apache Airflow** and utilizes Python for the data manipulation and acquisition layers.

---

## 🏗️ Architecture & Flow

This project implements a three-stage ETL process, orchestrated by an Airflow DAG:

1.  **Extract (E):** Web scraping of property listings (Title, Price, Location, Bedrooms, etc.) from `buyrentkenya.com`.
2.  **Transform (T):** Data cleaning and normalization using **Pandas** (e.g., converting price and size strings to numeric floats, standardizing location names).
3.  **Load (L):** Persistence of the cleaned and structured data into a PostgreSQL database using **SQLAlchemy** for schema definition and insertion.



## 🚀 Getting Started

### 1. Prerequisites

* Python 3.8+
* **PostgreSQL** database instance running (e.g., locally or via Docker).
* **Apache Airflow** environment (for scheduling and orchestration).

### 2. Repository Setup

Clone the repository and install the dependencies:

```bash
git clone [https://github.com/YourUsername/Kenya-Real-Estate-ETL.git](https://github.com/YourUsername/Kenya-Real-Estate-ETL.git)
cd Kenya-Real-Estate-ETL
pip install -r requirements.txt
