import azure.functions as func
import datetime
import logging
import os
import requests
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

# Environment variables
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME", "weather-loger1")
CITY_NAME = os.getenv("CITY_NAME", "Lagos")
TIMER_SCHEDULE = os.getenv("TIMER_SCHEDULE", "0 */3 * * *")  # Every 3 hours

@app.timer_trigger(
    schedule=TIMER_SCHEDULE,
    arg_name="wedaTimer",
    run_on_startup=True,
    use_monitor=True
)
def wedattfunc(wedaTimer: func.TimerRequest) -> None:
    """Fetch weather data and store logs in Blob Storage using Managed Identity."""
    
    if wedaTimer.past_due:
        logging.warning("The timer is past due.")

    logging.info("Starting weather logging function...")

    now = datetime.datetime.now()
    timestamp = now.strftime('%Y\t%b\t%d\t%H:%M:%S')
    log_filename = f"weather_{now.strftime('%Y_%m_%d')}.log"

    try:
        # --- Fetch weather data ---
        url = f"http://wttr.in/{CITY_NAME}?format=j1"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Extract current and forecast data safely
        current = data.get("current_condition", [{}])[0]
        forecast = data.get("weather", [{}])

        current_temp = current.get("temp_C", "N/A")
        current_wind = current.get("windspeedKmph", "N/A")

        # Tomorrow forecast (fallback to today's late evening if not available)
        if len(forecast) > 1 and "hourly" in forecast[1]:
            tomorrow_temp = forecast[1]["hourly"][4]["tempC"]
            tomorrow_wind = forecast[1]["hourly"][4]["windspeedKmph"]
        elif forecast and "hourly" in forecast[0]:
            tomorrow_temp = forecast[0]["hourly"][-1]["tempC"]
            tomorrow_wind = forecast[0]["hourly"][-1]["windspeedKmph"]
            logging.warning("Tomorrow's forecast unavailable. Using today's late data.")
        else:
            tomorrow_temp, tomorrow_wind = "N/A", "N/A"

        # --- Format log line ---
        log_line = (
            f"{timestamp}\t{current_temp}°C\t{current_wind}km/h\t"
            f"{tomorrow_temp}°C\t{tomorrow_wind}km/h\n"
        )

        # --- Connect to Blob Storage using Managed Identity ---
        credential = DefaultAzureCredential()
        blob_service = BlobServiceClient(
            account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
            credential=credential
        )
        container_client = blob_service.get_container_client(CONTAINER_NAME)

        if not container_client.exists():
            container_client.create_container()
            logging.info(f"Created container: {CONTAINER_NAME}")

        blob_client = container_client.get_blob_client(log_filename)

        # --- Append or create log ---
        try:
            existing_log = blob_client.download_blob().readall().decode("utf-8")
            updated_log = existing_log + log_line
        except Exception:
            header = (
                "year\tmonth\tday\thour\tcurrent_temp\tcurrent_wind\t"
                "tomorrow_temp\ttomorrow_wind\n"
            )
            updated_log = header + log_line
            logging.info("Created new log file with header.")

        # --- Upload updated log ---
        blob_client.upload_blob(updated_log, overwrite=True)
        logging.info("Weather data appended successfully.")

    except Exception as e:
        logging.error(f"Error during weather logging: {e}")
