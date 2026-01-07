import os
import io
import time
import json
import logging
import pymssql
import requests
import azure.functions as func
from datetime import datetime, timedelta
from docx import Document
from pathlib import Path

cors_headers = {
    "Access-Control-Allow-Origin": "https://victorious-pond-02e3be310.2.azurestaticapps.net", 
    "Access-Control-Allow-Methods": "POST, OPTIONS, GET",
    "Access-Control-Allow-Headers": "Content-Type, Accept",
    "Access-Control-Max-Age": "86400"
}

TABLE_FIELD_MAP = {
    "Trade Waste": {
        "File","Sample Date","pH Value","Total Dissolved Solids @180°C","Electrical Conductivity @ 25°C","Suspended Solids (SS)","Chemical Oxygen Demand","Arsenic","Iron","Zinc","Nitrite + Nitrate as N","Total Kjeldahl Nitrogen as N","Total Nitrogen as N","Total Phosphorus as P"
        ,"Sulfate as SO4 - Turbidimetric","Oil & Grease","C6 - C9 Fraction","C10 - C14 Fraction","C15 - C28 Fraction","C29 - C36 Fraction","C10 - C36 Fraction (sum)","C6 - C10 Fraction","C6 - C10 Fraction minus BTEX (F1)",">C10 - C16 Fraction",">C10 - C16 Fraction minus Naphthalene (F2)"
        ,">C16 - C34 Fraction",">C34 - C40 Fraction",">C10 - C40 Fraction (sum)","Benzene","Toluene","Ethylbenzene","meta- & para-Xylene","ortho-Xylene","Total Xylenes","Sum of BTEX","Naphthalene"
    },  
    "Fixation 2025": {
        "File","Sample Date","Moisture Content","Arsenic","Cadmium","Chromium","Copper","Lead","Nickel","Zinc","TCLP Arsenic","TCLP Cadmium","TCLP Chromium","TCLP Copper","TCLP Lead","TCLP Nickel","TCLP Zinc","After HCl pH","Extraction Fluid Number","Final pH","Initial pH","ZHE Extraction Fluid Number"
        ,"C10 - C14 Fraction","TCLP C10 - C14 Fraction","C10 - C36 Fraction (sum)","TCLP C10 - C36 Fraction (sum)","C15 - C28 Fraction","TCLP C15 - C28 Fraction","C29 - C36 Fraction","TCLP C29 - C36 Fraction","C6 - C9 Fraction","TCLP C6 - C9 Fraction",">C10 - C16 Fraction","TCLP >C10 - C16 Fraction",">C10 - C16 Fraction minus Naphthalene (F2)"
        ,"TCLP >C10 - C16 Fraction minus Naphthalene (F2)",">C10 - C40 Fraction (sum)","TCLP >C10 - C40 Fraction (sum)",">C16 - C34 Fraction","TCLP >C16 - C34 Fraction",">C34 - C40 Fraction","TCLP >C34 - C40 Fraction","C6 - C10 Fraction  minus BTEX (F1)","TCLP C6 - C10 Fraction  minus BTEX (F1)","C6 - C10 Fraction","TCLP C6 - C10 Fraction"
        ,"Benzene","TCLP Benzene","Ethylbenzene","TCLP Ethylbenzene","meta- & para-Xylene","TCLP meta- & para-Xylene","Naphthalene","TCLP Naphthalene","ortho-Xylene","TCLP ortho-Xylene","Sum of BTEX","TCLP Sum of BTEX","Toluene","TCLP Toluene","Total Xylenes","TCLP Total Xylenes"
    },
    "Stormwater": {
        "File","Sample Date","pH Value","Electrical Conductivity @ 25°C","Suspended Solids (SS)","Total Organic Carbon","Turbidity",">C10 - C16 Fraction",">C10 - C16 Fraction minus Naphthalene (F2)",">C10 - C40 Fraction (sum)",">C16 - C34 Fraction",">C34 - C40 Fraction","C10 - C14 Fraction"
        ,"C10 - C36 Fraction (sum)","C15 - C28 Fraction","C29 - C36 Fraction","Benzene","C6 - C10 Fraction","C6 - C10 Fraction minus BTEX (F1)","C6 - C9 Fraction","Ethylbenzene","meta- & para-Xylene","Naphthalene","ortho-Xylene","Sum of BTEX","Toluene","Total Xylenes"
    }
}
TEST_CODES = {
    "EP071": {
        ">C10 - C16 Fraction",">C10 - C16 Fraction minus Naphthalene (F2)",">C10 - C40 Fraction (sum)",">C16 - C34 Fraction",">C34 - C40 Fraction","C10 - C14 Fraction","C10 - C36 Fraction (sum)","C15 - C28 Fraction","C29 - C36 Fraction"
    },
    "EP080": {
        "Benzene","C6 - C10 Fraction","C6 - C10 Fraction minus BTEX (F1)","C6 - C9 Fraction","Ethylbenzene","meta- & para-Xylene","Naphthalene","ortho-Xylene","Sum of BTEX","Toluene","Total Xylenes"
    },
    "EA005-P": {
        "pH Value"
    },
    "EG020A-T": {
        "Arsenic","Iron","Zinc"
    },
    "EA015H": {
        "Total Dissolved Solids @180°C"
    },
    "EK067G": {
        "Total Phosphorus as P"
    },
    "EK062G": {
        "Total Nitrogen as N"
    },
    "EK061G": {
        "Total Kjeldahl Nitrogen as N"
    },
    "EK059G": {
        "Nitrite + Nitrate as N", "Nitrite + Nitrate as N (Sol.)"   
    },
    "EP005"	: {
        "Total Organic Carbon"
    },
    "EA025H": {
        "Suspended Solids (SS)"
    },
    "EA055": {
        "Moisture Content"
    },
    "EG005(ED093)T": {
        "Arsenic","Cadmium","Chromium","Copper","Lead","Nickel","Zinc"
    },
    "EP026-P": {
        "Chemical Oxygen Demand"
    },
    "EA010-P": {
        "Electrical Conductivity @ 25°C"
    },
    "EP020": {
        "Oil & Grease"
    },
    "ED041G": {
        "Sulfate as SO4 - Turbidimetric"
    },
    "EA045": {
        "Turbidity"
    },
    "EG005(ED093)T": {
        "Arsenic","Cadmium","Chromium","Copper","Lead","Nickel","Zinc"
    },
    "EG020T": {
        "Arsenic","Cadmium","Chromium","Copper","Lead","Nickel","Zinc"
    },
    "EN33": {
        "After HCl pH", "Extraction Fluid Number" ,"Final pH","Initial pH"
    },
    "EN33Z": {
        "ZHE Extraction Fluid Number"
    },
    "EP080/071": {
        "C10 - C14 Fraction","C10 - C36 Fraction (sum)","C15 - C28 Fraction","C29 - C36 Fraction","C6 - C9 Fraction",">C10 - C16 Fraction",">C10 - C16 Fraction minus Naphthalene (F2)",">C10 - C40 Fraction (sum)",">C16 - C34 Fraction",
        ">C34 - C40 Fraction","C6 - C10 Fraction  minus BTEX (F1)","C6 - C10 Fraction"
    }
}

PROJECT_MAP = {
    "Fixation":"Fixation",
    "FIXATION":"Fixation",
    "Stormwater": "Stormwater",
    "STORMWATER": "Stormwater",
    "Trade Waste": "Trade Waste",
    "TRADE WASTE": "Trade Waste",
}

TCLP_UNIT_MAP = {
    "Arsenic": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Arsenic",
        "standard_units": {"mg/kg"},
        "standard_field": "Arsenic",
    },
    "Cadmium": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Cadmium",
        "standard_units": {"mg/kg"},
        "standard_field": "Cadmium",
    },
    "Chromium": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Chromium",
        "standard_units": {"mg/kg"},
        "standard_field": "Chromium",
    },
    "Copper": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Copper",
        "standard_units": {"mg/kg"},
        "standard_field": "Copper",
    },
    "Lead": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Lead",
        "standard_units": {"mg/kg"},
        "standard_field": "Lead",
    },
    "Nickel": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Nickel",
        "standard_units": {"mg/kg"},
        "standard_field": "Nickel",
    },
    "Zinc": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Zinc",
        "standard_units": {"mg/kg"},
        "standard_field": "Zinc",
    },
    "C10 - C14 Fraction": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP C10 - C14 Fraction",
        "standard_units": {"mg/kg"},
        "standard_field": "C10 - C14 Fraction",
    },
    "C10 - C36 Fraction (sum)": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP C10 - C36 Fraction (sum)",
        "standard_units": {"mg/kg"},
        "standard_field": "C10 - C36 Fraction (sum)",
    },
    "C15 - C28 Fraction": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP C15 - C28 Fraction",
        "standard_units": {"mg/kg"},
        "standard_field": "C15 - C28 Fraction",
    },
    "C29 - C36 Fraction": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP C29 - C36 Fraction",
        "standard_units": {"mg/kg"},
        "standard_field": "C29 - C36 Fraction",
    },
    "C6 - C9 Fraction": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP C6 - C9 Fraction",
        "standard_units": {"mg/kg"},
        "standard_field": "C6 - C9 Fraction",
    },
    ">C10 - C16 Fraction": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP >C10 - C16 Fraction",
        "standard_units": {"mg/kg"},
        "standard_field": ">C10 - C16 Fraction",
    },
    ">C10 - C16 Fraction minus Naphthalene (F2)": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP >C10 - C16 Fraction minus Naphthalene (F2)",
        "standard_units": {"mg/kg"},
        "standard_field": ">C10 - C16 Fraction minus Naphthalene (F2)",
    },
    ">C10 - C40 Fraction (sum)": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP >C10 - C40 Fraction (sum)",
        "standard_units": {"mg/kg"},
        "standard_field": ">C10 - C40 Fraction (sum)",
    },
    ">C16 - C34 Fraction": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP >C16 - C34 Fraction",
        "standard_units": {"mg/kg"},
        "standard_field": ">C16 - C34 Fraction",
    },
    ">C34 - C40 Fraction": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP >C34 - C40 Fraction",
        "standard_units": {"mg/kg"},
        "standard_field": ">C34 - C40 Fraction",
    },
    "C6 - C10 Fraction": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP C6 - C10 Fraction",
        "standard_units": {"mg/kg"},
        "standard_field": "C6 - C10 Fraction",
    },
    "C6 - C10 Fraction  minus BTEX (F1)": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP C6 - C10 Fraction  minus BTEX (F1)",
        "standard_units": {"mg/kg"},
        "standard_field": "C6 - C10 Fraction  minus BTEX (F1)",
    },
    "Benzene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Benzene",
        "standard_units": {"mg/kg"},
        "standard_field": "Benzene",
    },
    "Toluene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Toluene",
        "standard_units": {"mg/kg"},
        "standard_field": "Toluene",
    },
    "Ethylbenzene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Ethylbenzene",
        "standard_units": {"mg/kg"},
        "standard_field": "Ethylbenzene",
    },
    "meta- & para-Xylene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP meta- & para-Xylene",
        "standard_units": {"mg/kg"},
        "standard_field": "meta- & para-Xylene",
    },
    "Naphthalene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Naphthalene",
        "standard_units": {"mg/kg"},
        "standard_field": "Naphthalene",
    },
    "ortho-Xylene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP ortho-Xylene",
        "standard_units": {"mg/kg"},
        "standard_field": "ortho-Xylene",
    },
    "Sum of BTEX": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Sum of BTEX",
        "standard_units": {"mg/kg"},
        "standard_field": "Sum of BTEX",
    },
    "Total Xylenes": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Total Xylenes",
        "standard_units": {"mg/kg"},
        "standard_field": "Total Xylenes",
    },
}

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Fetching and filtering lab data to generate SQL...")

    try:
        # === Environment variables ===
        auth_url = os.environ["API_AUTH_URL"]
        data_url = os.environ["API_DATA_URL"]
        username = os.environ["API_USERNAME"]
        password = os.environ["API_PASSWORD"]

        # === Set parameters for timer trigger ===
        from_days_ago = 7  # Fetch data from the last 7 days
        project_no = None
        workorder_code = None
        
        # Default: last 7 days, page=1
        to_dt = datetime.utcnow()
        from_dt = to_dt - timedelta(days=from_days_ago)
        from_param = from_dt.strftime("%Y/%m/%d %H:%M:%S.000Z")
        to_param = to_dt.strftime("%Y/%m/%d %H:%M:%S.000Z")
        
        # TODO: Implement pagination if more than one page of results is expected
        page_param = "16"

        # === Step 1: Authenticate ===
        auth_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
        }
        auth_payload = {
            "Username": username,
            "Password": password,
        }

        auth_resp = requests.post(auth_url, headers=auth_headers, json=auth_payload, timeout=60)
        auth_resp.raise_for_status()
        auth_data = auth_resp.json()

        # Support multiple possible token structures
        token = (
            auth_data.get("Token")
            or auth_data.get("token")
            or (auth_data.get("Data", {}).get("Token"))
            or (auth_data.get("data", {}).get("token"))
        )
        if not token:
            raise ValueError(f"No token found in auth response: {auth_data}")
        
        # === Step 2: Fetch ALL PAGES of data ===
        def extract_records(api_data):
            """Extracts normalized list of records from any supported API structure."""
            if isinstance(api_data, dict):
                # Format A: {"Results": [...]}
                if "Results" in api_data and isinstance(api_data["Results"], list):
                    return api_data["Results"]

                # Format B: {"data": "[{...}, {...}]"} where "data" is a JSON string
                if "data" in api_data and isinstance(api_data["data"], str):
                    try:
                        return json.loads(api_data["data"])
                    except Exception:
                        logging.error("Failed to parse 'data' JSON string.")
                        return []

                # Format C: nested Data.Results
                if "Data" in api_data and "Results" in api_data["Data"]:
                    return api_data["Data"]["Results"]

            # Fallback: assume raw list
            if isinstance(api_data, list):
                return api_data

            logging.warning("Unrecognized API format. Returning empty result set.")
            return []

        all_records = []
        current_page = 1

        # First request (page 1)
        params = {"From": from_param, "To": to_param, "Page": str(current_page)}
        data_headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

        logging.info(f"Fetching page {current_page}...")

        resp = requests.get(data_url, headers=data_headers, params=params, timeout=60)
        if resp.status_code == 401:
            data_headers["Authorization"] = token
            resp = requests.get(data_url, headers=data_headers, params=params, timeout=60)

        resp.raise_for_status()
        data_json = resp.json()

        # Extract page 1's data
        page_records = extract_records(data_json)
        all_records.extend(page_records)

        # Determine total pages
        total_pages = (
            data_json.get("TotalPages")
            or data_json.get("totalPages")
            or data_json.get("Pages")
        )

        # Compute pages if API provides counts instead
        if not total_pages:
            total_count = (
                data_json.get("TotalCount")
                or data_json.get("totalCount")
                or data_json.get("Count")
            )
            page_size = (
                data_json.get("PageSize")
                or data_json.get("pageSize")
                or len(page_records)
            )

            if total_count and page_size:
                total_pages = max(1, (total_count + page_size - 1) // page_size)

        if not total_pages:
            logging.info("API does not provide page counts. Assuming only 1 page.")
            total_pages = 1

        logging.info(f"Total pages detected: {total_pages}")

        # Fetch remaining pages
        for current_page in range(2, int(total_pages) + 1):
            logging.info(f"Fetching page {current_page}...")

            params["Page"] = str(current_page)
            resp = requests.get(data_url, headers=data_headers, params=params, timeout=60)
            if resp.status_code == 401:
                data_headers["Authorization"] = token
                resp = requests.get(data_url, headers=data_headers, params=params, timeout=60)

            resp.raise_for_status()
            page_json = resp.json()

            page_records = extract_records(page_json)
            all_records.extend(page_records)

        logging.info(f"Total combined records fetched: {len(all_records)}")

        # Replace old sample_records with combined data
        sample_records = all_records
        # === Step 3: Process data and generate SQL ===
        # For a timer trigger, we process all fetched records without extra filtering.
        sql_statements = process_lab_json(
            sample_records,
            project_no=project_no,
            workorder_code=workorder_code
        )

        def connect_sql_pymssql(timeout_seconds: int = 60):
            sql_server = os.environ["SQL_SERVER"]
            sql_database = os.environ["SQL_DB_LAB"]
            sql_username = os.environ["SQL_USER"]
            sql_password = os.environ["SQL_PASSWORD"]

            return pymssql.connect(
                server=sql_server,
                user=sql_username,
                password=sql_password,
                database=sql_database,
                login_timeout=timeout_seconds,
                timeout=timeout_seconds,
                as_dict=False
            )

        # === Step 4: Execute SQL statements ===
        conn = None
        cursor = None
        try:
            conn = connect_sql_pymssql(timeout_seconds=60)
            cursor = conn.cursor()

            if not sql_statements:
                logging.info("No SQL statements to execute.")
            else:
                logging.info(f"Executing {len(sql_statements)} SQL statements...")
                for sql in sql_statements:
                    cursor.execute(sql)

                conn.commit()
                logging.info("✅ Successfully executed and committed SQL statements.")

        except Exception as e:
            logging.error(f"SQL execution failed: {e}")
            raise

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


        logging.info(f"Function finished. {len(sql_statements)} records processed.")
    
        return func.HttpResponse(
            body=json.dumps({
                "status": "success",
                "records_processed": len(sql_statements)
            }),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.exception("Unhandled exception")
        return func.HttpResponse(
            body=json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )


def build_sql_insert(sample_records, project_table):
    """
    Build one SQL INSERT per sample group.
    Includes all mapped analytes as columns; NULL where not found.
    """
    logging.info(f"Building SQL for project table: {project_table}")
    logging.info(f"Type of sample_records in build_sql_insert: {type(sample_records)}")
    fields = TABLE_FIELD_MAP.get(project_table, set())
    if not fields:
        logging.warning(f"No field mapping for table {project_table}")
        return None

    first_record = sample_records[0]
    values = {field: "NULL" for field in fields}
    logging.info(f"Type of first_record in build_sql_insert: {type(first_record)}")
    logging.info(f"First record content: {str(first_record)[:500]}")

    # Static fields
    if "File" in fields:
        values["File"] = f"'{first_record.get('Submission', '')}'"
    if "Sample Location" in fields:
        values["Sample Location"] = f"'{first_record.get('SampleID1', '')}'"
    if "Sample Name" in fields:
        values["Sample Name"] = f"'{first_record.get('SampleID1', '')}'"
    if "Sample Date" in fields:
        sample_date = first_record.get("SampleDate", "")
        if sample_date:
            try:
                parsed_date = datetime.strptime(sample_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            except ValueError:
                parsed_date = sample_date
        else:
            parsed_date = ""
        values["Sample Date"] = f"'{parsed_date}'"

    # Fill analytes
    for rec in sample_records:
        compound = rec.get("Compound")
        result = rec.get("Result")
        if compound in fields and result not in [None, ""]:
            # Normalize result value
            clean_result = str(result).replace("~", "").replace("<", "")
            values[compound] = f"{clean_result}"

    # Generate SQL
    field_list = ", ".join([f"[{f}]" for f in fields])
    value_list = ", ".join([values[f] for f in fields])
    sql = f"INSERT INTO [Jackson].[{project_table}] ({field_list}) VALUES ({value_list});"
    return sql

def process_lab_json(data, project_no=None, workorder_code=None):
    """
    Groups JSON lab data by sample and generates SQL inserts.
    """
    logging.info(f"Processing lab JSON. Data type: {type(data)}")
    if isinstance(data, str):
        logging.info("Data is a string, attempting to parse JSON.")
        data = json.loads(data)

    logging.info(f"Data type after initial check: {type(data)}")

    # Optional filtering
    logging.info(f"Filtering with project_no: '{project_no}' and workorder_code: '{workorder_code}'")

    def norm(val):
        """Normalize for reliable matching."""
        if val is None:
            return ""
        return str(val).strip().lower().replace("(", "").replace(")", "").replace("<", "").replace("~", "")

    pn = norm(project_no)
    wo = norm(workorder_code)

    filtered = [
        rec for rec in data
        if (not pn or norm(rec.get("ProjectNo")) == pn)
        and (not wo or norm(rec.get("WorkorderCode")) == wo)
    ]

    logging.info(f"Found {len(filtered)} records after filtering.")
    if filtered:
        logging.info(f"First filtered record: {str(filtered[0])[:500]}")
    if not filtered:
        logging.warning("No matching records found.")
        return []

    # Group by (Submission, SampleID1, SampleDate)
    grouped = {}
    for rec in filtered:
        key = (rec.get("Submission"), rec.get("SampleID1"), rec.get("SampleDate"))
        grouped.setdefault(key, []).append(rec)

    sql_statements = []

    # PATCH A — determine project table **per group**
    for records in grouped.values():

        record_project = records[0].get("ProjectNo") or records[0].get("Site")
        project_table = PROJECT_MAP.get(record_project)

        if not project_table:
            logging.warning(f"No project table found for project: {record_project}")
            continue

        sql = build_sql_insert(records, project_table)
        if sql:
            sql_statements.append(sql)

    return sql_statements

def write_sql_to_file(sql_statements, output_path="output_inserts.sql"):
    """
    Write all generated SQL statements to a file for review.
    """
    path = Path(output_path)
    path.write_text("\n".join(sql_statements))
    logging.info(f"✅ Wrote {len(sql_statements)} SQL statements to {path.resolve()}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Example usage
    with open("sample_lab_data.json", "r") as f:
        lab_data = json.load(f)

    sqls = process_lab_json(lab_data, project_no="88798", workorder_code="EB2537666")

    if sqls:
        write_sql_to_file(sqls)
        print(f"Generated {len(sqls)} SQL insert statements.")
    else:
        print("No SQL statements generated.")