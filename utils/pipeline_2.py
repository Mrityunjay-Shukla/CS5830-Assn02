import glob
import logging
import os

import numpy as np  
import pandas as pd 
from shapely.geometry import Point  
from airflow.models import Variable
import apache_beam as beam 
import geopandas as gpd 
import matplotlib.pyplot as plt  


# setup the working directory
working_dir = os.path.expanduser("~/CS5830/Assignment2/")
if not os.path.exists(working_dir + "tmp"):
    os.makedirs(working_dir + "tmp")


def set_task_inputs(**kwargs):
    """
    Sets task inputs based on provided keyword arguments.

    Parameters:
    **kwargs (dict): Keyword arguments containing the DAG run configuration.

    Returns:
    None
    """
    try:
        # Set the archive file path variable
        Variable.set("input_archive_path", kwargs["dag_run"].conf["archive_file_path"])

        # Set the list of required columns variable
        required_cols = kwargs["dag_run"].conf.get("required_cols", [])
        Variable.set("input_required_cols", ",".join(required_cols))

        logging.info("Input variables 'input_archive_path' and 'input_required_cols' updated successfully.")
    except KeyError:
        logging.error("""'archive_file_path' or 'required_cols' not provided correctly. Please pass
                      configuration arguments as --conf '{"archive_file_path": "<archive_file_path>", "required_cols": ["col1", "col2"]}'
                      """)
        exit()

def process_and_write_hourly_data(**kwargs):
    """
    Processes hourly data arrays from CSV files and writes aggregated data to a text file.
    """

    # Check if the previous task succeeded
    status = kwargs["ti"].xcom_pull(key="return_value", task_ids="validate_and_unzip")
    if status == "error":
        logging.error("Failed to unzip the data. Aborting process.")
        return

    logging.info("Data unzip process successful.")

    # Retrieve CSV file paths using glob
    csv_files = glob.glob(f"{working_dir}/tmp/*.csv")

    # Display the list of CSV file paths
    logging.info(f"CSV files retrieved from glob: {csv_files}")

    # Retrieve required columns from Airflow variable
    required_cols = Variable.get("required_cols", default_var="['ELEVATION']")
    logging.info(f"Required columns specified by user: {required_cols}")

    # Convert the string input to a list
    required_cols = eval(required_cols)

    def read_and_filter_csv(file_name):
        """
        Read CSV file and filter columns based on specified required columns.
        """
        # Read CSV file and filter columns
        df = pd.read_csv(file_name, usecols=required_cols + ["LATITUDE", "LONGITUDE"])

        # Check if all specified columns are found in the dataframe
        cols_not_found = [col for col in required_cols if col not in df.columns]
        if cols_not_found:
            logging.error(f"Specified columns not found in CSV file: {cols_not_found}")
            raise ValueError("Missing required columns in CSV file.")

        return df

    def aggregate_hourly_data(df):
        """
        Aggregate hourly data by latitude and longitude into tuples.
        """
        # Group dataframe by latitude and longitude
        grouped = df.groupby(["LATITUDE", "LONGITUDE"])

        # Iterate over groups to create tuples of aggregated hourly data
        aggregated_data = []
        for (lat, lon), group in grouped:
            hourly_data = group[required_cols].values.tolist()
            aggregated_data.append((lat, lon, hourly_data))

        return aggregated_data

    # Define output file path
    output_path = working_dir + "/tmp/values_arrays.txt"

    # Use Apache Beam to process and write aggregated data to text file
    with beam.Pipeline() as p:
        csv_data = (
            p | beam.Create(csv_files) | beam.Map(read_and_filter_csv) | beam.FlatMap(aggregate_hourly_data)
        )
        csv_data | beam.io.WriteToText(output_path, num_shards=1)

    logging.info(f"Aggregated hourly data written to: {output_path}")

    return


def calculate_and_write_monthly_averages(**kwargs):
    """
    Processes monthly average data from CSV files and writes the results to a text file.
    """

    # Check if the previous task succeeded
    status = kwargs["ti"].xcom_pull(key="return_value", task_ids="validate_and_unzip")
    if status == "error":
        logging.error("Failed to unzip the data. Aborting process.")
        return

    logging.info("Data unzip process successful.")

    # Retrieve CSV file paths using glob
    csv_files = glob.glob(f"{working_dir}/tmp/*.csv")
    logging.info(f"CSV files retrieved from glob: {csv_files}")

    # Retrieve required columns from Airflow variable
    required_cols = Variable.get("required_cols", default_var="['HourlyWindSpeed']")
    logging.info(f"Required columns specified by user: {required_cols}")

    # Convert the string input to a list
    required_cols = eval(required_cols)

    def read_and_filter_csv(file_name):
        """
        Read CSV file and filter columns based on specified required columns.
        """
        # Read CSV file and filter columns
        df = pd.read_csv(file_name, usecols=required_cols + ["DATE", "LATITUDE", "LONGITUDE"])

        # Check if all specified columns are found in the dataframe
        cols_not_found = [col for col in required_cols if col not in df.columns]
        if cols_not_found:
            logging.error(f"Specified columns not found in CSV file: {cols_not_found}")
            raise ValueError("Missing required columns in CSV file.")

        return df

    def calculate_monthly_averages(df):
        """
        Calculate monthly average values for specified columns in the given dataframe.
        """
        # Extract latitude and longitude from the dataframe
        lat = df["LATITUDE"].iloc[0]
        lon = df["LONGITUDE"].iloc[0]

        # Convert dataframe to list of lists
        data = df.values.tolist()

        # Calculate monthly averages for each required column
        monthly_averages = []
        for col in required_cols:
            col_idx = df.columns.get_loc(col)
            col_monthly_avg = []

            # Calculate average for each month (1 to 12)
            for month in range(1, 13):
                # Filter data for the current month
                month_data = [x[col_idx] for x in data if int(x[0].split("-")[1]) == month]

                # Convert data to float, ignoring non-numeric values
                month_data = np.array([float(x) for x in month_data if isinstance(x, (int, float))])

                # Calculate monthly average (ignoring NaN values)
                col_avg = np.nanmean(month_data)
                col_monthly_avg.append(col_avg)

            monthly_averages.append(col_monthly_avg)

        # Transpose the list of monthly averages for output format
        monthly_averages = list(map(list, zip(*monthly_averages)))

        logging.info(f"Calculated monthly averages: {monthly_averages}")

        return (lat, lon, monthly_averages)

    # Define output file path
    output_path = working_dir + "/tmp/values_averages.txt"

    # Use Apache Beam to process and write monthly averages to text file
    with beam.Pipeline() as p:
        csv_data = (
            p | beam.Create(csv_files) | beam.Map(read_and_filter_csv) | beam.Map(calculate_monthly_averages)
        )

        csv_data | beam.io.WriteToText(output_path, num_shards=1)

    logging.info(f"Monthly averages written to: {output_path}")

    return


def make_heatmaps(**kwargs):
    """
    Make heatmaps using the given data. Reads the data from a file, processes it, 
    generates the heatmaps for specified fields, and saves the output as image files.
    """


    # open the output from previous file
    reading_path = working_dir + "/tmp" + "/values_averages.txt" + "-00000-of-00001"
    with open(reading_path, "r") as file:
        content = file.readlines()
    logging.info(f"content is \n{content}")

    # Convert nan to python-understandable None
    content = [i.replace("nan", "None") for i in content]

    # convert the string input from the file to tuples using eval
    data = [eval(line.strip()) for line in content]
    logging.info("read the tuple from the previous python operator")

    def process_data(data):
        """
        Process the given data to extract latitude, longitude, and monthly averages.
        """
        latitudes = [entry[0] for entry in data]
        longitudes = [entry[1] for entry in data]
        monthly_averages = [entry[2] for entry in data]

        logging.info("Extracted coordinates and data values from the tuple")
        return latitudes, longitudes, monthly_averages

    def generate_heatmap(latitudes, longitudes, monthly_averages, required_cols):
        """
        Generate heatmap from given latitude, longitude, and monthly average data.
        """

        # Create a GeoDataFrame with latitude, longitude, and field averages
        geometry = [Point(lon, lat) for lon, lat in zip(longitudes, latitudes)]

        df = pd.DataFrame(
            {
                "Latitude": latitudes,
                "Longitude": longitudes,
                "MonthlyAverages": monthly_averages,
            }
        )

        # plot the values for the first month for now
        for i, field_name in enumerate(required_cols):
            df[field_name] = [monthly_avg[0][i] for monthly_avg in monthly_averages]

        # Create a GeoDataFrame
        crs = {"init": "epsg:4326"}
        gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
        logging.info(f"geopandas df is \n{gdf}")

        # Plot heatmaps for each field
        for i, field_name in enumerate(required_cols):
            
            # get world map file 
            world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
            fig, ax = plt.subplots(figsize=(12, 8))
            world.plot(ax=ax, color="white", edgecolor="gray")

            # plot the values only if the field is not null
            if gdf[field_name].notnull().any():
                gdf.plot(
                    column=field_name, cmap="coolwarm", linewidth=6, ax=ax, legend=True
                )
                ax.set_title(
                    f"Heatmap of {field_name}\nfor given stations" ,
                    fontdict={"fontsize": "18", "fontweight": "3"},
                )
            else:
                ax.set_title(
                    f"Heatmap of {field_name} for given {df.shape[0]} stations.\nNo data available for this field.",
                    fontdict={"fontsize": "15", "fontweight": "3"},
                )

            ax.autoscale()
            ax.axis("off")
            fig.tight_layout()

            # Save plot to output_heatmaps directory
            os.makedirs(working_dir + "/output_heatmaps", exist_ok=True)
            plt.savefig(
                working_dir + "/output_heatmaps" + f"/heatmap_geo_{field_name}.png"
            )

    def run_pipeline(data):
        """
        This function runs a pipeline to process data, retrieve required columns, and generate heatmaps.
        """

        latitudes, longitudes, monthly_averages = process_data(data)
        required_cols = Variable.get("required_cols", default_var=["HourlyWindSpeed"])
        required_cols = eval(required_cols)
        logging.info("Starting to generate the heatmaps")
        generate_heatmap(latitudes, longitudes, monthly_averages, required_cols)

    # Run pipeline
    with beam.Pipeline(runner="DirectRunner") as pipeline:
        p = (
            pipeline
            | "Create data" >> beam.Create([data])
            | "Run pipeline" >> beam.Map(run_pipeline)
        )

    return