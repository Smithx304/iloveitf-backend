import io
import csv
from datetime import datetime
from flask import Flask, request, send_file, abort
from flask_cors import CORS
import pandas as pd

app = Flask(__name__)
CORS(app)  # allow requests from your frontend

def compare_dates(d1, d2):
    try:
        date1 = datetime.strptime(d1, "%m/%d/%Y")
        date2 = datetime.strptime(d2, "%m/%d/%Y")
        return date1 > date2
    except:
        return False

def extract_trip_number(trip_id):
    try:
        return int(''.join(filter(str.isdigit, trip_id)))
    except:
        return 0

def process_csv(stream):
    reader = csv.reader(io.StringIO(stream.read().decode("utf-8")))
    driver_data = {}
    current_driver = None
    latest_seen_truck = None

    for row in reader:
        if not any(row): 
            continue

        # New driver block
        if row[0].startswith("Driver:"):
            current_driver = row[0].split(":",1)[1].split(",")[0].strip()
            latest_seen_truck = None
            driver_data.setdefault(current_driver, {
                "trip_id": "N/A",
                "truck": "N/A",
                "paperwork_date": "N/A"
            })
            continue

        if current_driver and len(row) > 13:
            trip_id = row[1].strip()
            truck = row[4].strip()
            driver_name = row[6].strip()
            paperwork_date = row[13].strip()

            if truck:
                latest_seen_truck = truck
                driver_data[current_driver]["truck"] = truck

            # skip incomplete rows
            if not trip_id or not driver_name or not paperwork_date:
                continue

            existing = driver_data[current_driver]
            should = (
                existing["paperwork_date"] == "N/A" or
                compare_dates(paperwork_date, existing["paperwork_date"]) or
                (paperwork_date == existing["paperwork_date"] and
                 extract_trip_number(trip_id) > extract_trip_number(existing["trip_id"]))
            )
            if should:
                driver_data[current_driver] = {
                    "trip_id": trip_id,
                    "truck": latest_seen_truck or existing["truck"],
                    "paperwork_date": paperwork_date
                }

    # build DataFrame
    df = pd.DataFrame([
        {
            "Driver": d,
            "Truck": info["truck"],
            "Last Paperwork Date": info["paperwork_date"],
            "Trip ID": info["trip_id"]
        }
        for d, info in driver_data.items()
    ])
    return df

@app.route("/api/process", methods=["POST"])
def process():
    if "file" not in request.files:
        return abort(400, "No file part")
    f = request.files["file"]
    if f.filename == "" or not f.filename.lower().endswith(".csv"):
        return abort(400, "Invalid file")
    # process and write to Excel in-memory
    df = process_csv(f.stream)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Summary")
    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name="Driver_Paperwork_Summary.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
