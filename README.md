Graph Analysis of Flight Connections and Airports Worldwide

Welcome to the Graph Analysis project repository! This project focuses on performing graph analysis using the GraphFrames Spark library within a Big Data framework. 
By leveraging the GraphFrames API for DataFrames, we analyze a dataset containing information about flight connections and airports worldwide.

Data Overview
The analysis utilizes three main CSV files:
- airports.csv: Contains information about airports worldwide, including unique identifiers, names, locations (latitude and longitude), cities, and countries.
- airlines.csv: Provides details about different airlines, including airline IDs, names, countries, and ICAO codes.
- routes.csv: Enumerates flight routes provided by each airline between two airports, listing airline IDs, source airport IDs, and destination airport IDs.

Analysis Approach
The project encompasses various analyses and techniques:

Top Airlines and Routes:
- Extracting and analyzing top airlines based on flight connections.
- Identifying and examining popular flight routes worldwide.

Scenarios Evaluation with Motifs:
- Evaluating different scenarios to reach a specific airport using motifs.
- Analyzing patterns and sequences of flight connections to understand airport connectivity.

Connected Components:
- Calculating connected components within the graph to identify clusters of connected airports.
- Choosing a small subgraph to visualize and analyze connectivity patterns.

Distance Calculation:
- Using the Haversine function to calculate distances between airports with connected flights.
- Analyzing geographical distances between airports in the dataset.

Repository Structure
├── data/
│   ├── airports.csv          # Dataset containing information about airports worldwide
│   ├── airlines.csv          # Dataset providing details about different airlines
│   └── routes.csv            # Dataset enumerating flight routes between airports
├── analysis/
│   ├── Python.py             # Executable Python script containing the analysis code
│   ├── Jupyter.ipynb         # Jupyter notebook containing the analysis code
│   └── PDF.pdf               # PDF version of the Jupyter notebook
├── LICENSE                   # License information for the project
└── README.md                 # Detailed project overview, setup instructions, and usage guidelines

Getting Started
To replicate the analysis or explore the provided code and notebooks, follow these steps:
1. Clone this repository to your local machine.
2. Navigate to the analysis/ directory containing the Jupyter notebook (Jupyter.ipynb) or Python script (Python.py).
3. Run the provided code to analyze the flight connections and airport data.
4. Ensure that you have the necessary dependencies installed, including PySpark and GraphFrames.

Contribution Guidelines
Contributions to this project are welcome! If you have any ideas for improvements, additional analyses, or bug fixes, feel free to open an issue or submit a pull request.
