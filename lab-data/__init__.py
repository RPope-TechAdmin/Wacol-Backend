import azure.functions as func
from requests_toolbelt.multipart import decoder
from io import BytesIO
import pdfplumber
import os
import json
import re
import logging
import pymssql
import time
from sqlalchemy import create_engine, text

# === CONFIGURATION ===
cors_headers = {
    "Access-Control-Allow-Origin": "https://delightful-tree-0888c340f.1.azurestaticapps.net", 
    "Access-Control-Allow-Methods": "POST, OPTIONS, GET",
    "Access-Control-Allow-Headers": "Content-Type, Accept",
    "Access-Control-Max-Age": "86400"
}

FIELD_MAP = {
    "fixation": [
        "File Name",
        "Sample Location",
        "Sampling Date/Time",
        "Moisture Content---", "Antimony", "Arsenic", "Barium", "Beryllium", "Boron", "Cadmium", "Chromium", "Cobalt", "Copper", "Lead", "Manganese", "Molybdenum", "Nickel",
        "Selenium", "Tin", "Zinc", "Mercury", "Initial pH", "After HCl pH", "Extraction Fluid Number", "Final pH",
        "alpha-BHC", "Hexachlorobenzene (HCB)", "beta-BHC", "gamma-BHC - (Lindane)", "delta-BHC", "Heptachlor", "Aldrin", "Heptachlor epoxide", "Chlordane (sum)", "trans-Chlordane", 
        "alpha-Endosulfan", "cis-Chlordane", "Dieldrin", "4.4`-DDE", "Endrin", "Endosulfan (sum)", "beta-Endosulfan", "4.4`-DDD", "Endrin aldehyde", "Endosulfan sulfate", "4.4`-DDT",
        "Endrin ketone", "Methoxychlor", "Sum of DDD + DDE + DDT", "Sum of Aldrin + Dieldrin", "Dichlorvos", "Demeton-S-methyl", "Monocrotophos", "Dimethoate", "Diazinon", "Chlorpyrifos-methyl", 
        "Parathion-methyl", "Malathion", "Fenthion", "Chlorpyrifos", "Parathion", "Pirimphos-ethyl", "Chlorfenvinphos", "Bromophos-ethyl", "Fenamiphos", "Prothiofos", "Ethion", "Carbophenothion",
        "Azinphos Methyl", "Phenol", "2-Chlorophenol", "2-Methylphenol", "3- & 4-Methylphenol", "2-Nitrophenol", "2.4-Dimethylphenol", "2.4-Dichlorophenol", "2.6-Dichlorophenol", 
        "4-Chloro-3-methylphenol", "2.4.6-Trichlorophenol", "2.4.5-Trichlorophenol", "Pentachlorophenol", "Naphthalene", "Acenaphthylene", "Acenaphthene", "Fluorene", "Phenanthrene",
        "Anthracene", "Fluoranthene", "Pyrene", "Benz(a)anthracene", "Chrysene", "Benzo(b+j)fluoranthene", "Benzo(k)fluoranthene", "Benzo(a)pyrene", "Indeno(1.2.3.cd)pyrene", "Dibenz(a.h)anthracene",
        "Benzo(g.h.i)perylene", "Sum of polycyclic aromatic hydrocarbons", "Benzo(a)pyrene TEQ (zero)", "Benzo(a)pyrene TEQ (half LOR)", "Benzo(a)pyrene TEQ (LOR)", "Styrene", "Isopropylbenzene",
        "n-Propylbenzene", "1.3.5-Trimethylbenzene", "sec-Butylbenzene", "1.2.4-Trimethylbenzene", "tert-Butylbenzene", "p-Isopropyltoluene", "n-Butylbenzene", "C6 - C9 Fraction", "C10 - C14 Fraction",
        "C15 - C28 Fraction", "C29 - C36 Fraction", "C10 - C36 Fraction (sum)", "C6 - C10 Fraction", "C6 - C10 Fraction  minus BTEX (F1)", ">C10 - C16 Fraction", ">C16 - C34 Fraction",
        ">C34 - C40 Fraction", ">C10 - C40 Fraction (sum)", ">C10 - C16 Fraction minus Naphthalene (F2)", "Benzene", "Toluene", "Ethylbenzene", "meta- & para-Xylene", "ortho-Xylene", 
        "Xylenes", "Sum of BTEX", "Naphthalene", "Dibromo-DDE", "DEF", "Phenol-d6", "2-Chlorophenol-D4", "2.4.6-Tribromophenol", "2-Fluorobiphenyl", "Anthracene-d10",
        "4-Terphenyl-d14", "1.2-Dichloroethane-D4", "Toluene-D8", "4-Bromofluorobenzene"
    ]
}

SUBMATRIX_MAP = {
    "FIXATION":FIELD_MAP["fixation"],
    "SOIL": ["File Name","Sample Location","Sampling Date/Time","Moisture Content ----","Antimony","Arsenic","Barium","Beryllium","Boron","Cadmium","Chromium","Cobalt","Copper","Lead","Manganese","Molybdenum","Nickel","Selenium","Tin","Zinc","Mercury","Initial pH ----",
            "After HCl pH ----","Extraction Fluid Number ----","Final pH ----","Extraction Fluid Number ----","alpha-BHC","Hexachlorobenzene (HCB)","beta-BHC","gamma-BHC - (Lindane)","delta-BHC","Heptachlor","Aldrin",
            "Heptachlor epoxide","Total Chlordane (sum) ----","trans-Chlordane","alpha-Endosulfan","cis-Chlordane","Dieldrin","4.4`-DDE","Endrin","Endosulfan (sum)","beta-Endosulfan","4.4`-DDD","Endrin aldehyde","Endosulfan sulfate","4.4`-DDT",
            "Endrin ketone","Methoxychlor","Sum of DDD + DDE + DDT","Sum of Aldrin + Dieldrin","Dichlorvos","Demeton-S-methyl","Monocrotophos","Dimethoate","Diazinon","Chlorpyrifos-methyl","Parathion-methyl","Malathion",
            "Fenthion","Chlorpyrifos","Parathion","Pirimphos-ethyl","Chlorfenvinphos","Bromophos-ethyl","Fenamiphos","Prothiofos","Ethion","Carbophenothion","Azinphos Methyl","Phenol","2-Chlorophenol","2-Methylphenol","3- & 4-Methylphenol",
            "2-Nitrophenol","2.4-Dimethylphenol","2.4-Dichlorophenol","2.6-Dichlorophenol","4-Chloro-3-methylphenol","2.4.6-Trichlorophenol","2.4.5-Trichlorophenol","Pentachlorophenol","Naphthalene","Acenaphthylene","Acenaphthene","Fluorene","Phenanthrene","Anthracene","Fluoranthene","Pyrene","Benz(a)anthracene",
            "Chrysene","Benzo(b+j)fluoranthene","Benzo(k)fluoranthene","Benzo(a)pyrene","Indeno(1.2.3.cd)pyrene","Dibenz(a.h)anthracene","Benzo(g.h.i)perylene","Sum of polycyclic aromatic hydrocarbons ----","Benzo(a)pyrene TEQ (zero) ----","Benzo(a)pyrene TEQ (half LOR) ----","Benzo(a)pyrene TEQ (LOR) ----","C6 - C9 Fraction ----","C10 - C14 Fraction ----",
            "C15 - C28 Fraction ----","C29 - C36 Fraction ----","C10 - C36 Fraction (sum) ----","C6 - C10 Fraction","C6 - C10 Fraction  minus BTEX (F1)",">C10 - C16 Fraction ----",">C16 - C34 Fraction ----",">C34 - C40 Fraction ----",">C10 - C40 Fraction (sum) ----",">C10 - C16 Fraction minus Naphthalene (F2) ----","Benzene","Toluene","Ethylbenzene","meta- & para-Xylene",
            "ortho-Xylene","Total Xylenes ----","Sum of BTEX ----","Naphthalene","Dibromo-DDE","DEF","Phenol-d6","2-Chlorophenol-D4","2.4.6-Tribromophenol","2-Fluorobiphenyl","Anthracene-d10","4-Terphenyl-d14","1.2-Dichloroethane-D4","Toluene-D8","4-Bromofluorobenzene"
            ],
    "TCLP LEACHATE": ["File Name","Sample Location","Sampling Date/Time","Moisture Content","Antimony","Arsenic","Barium","Beryllium","Boron","Cadmium","Chromium","Cobalt","Copper","Lead","Manganese","Molybdenum","Nickel","Selenium","Tin","Zinc","Mercury","alpha-BHC","Hexachlorobenzene (HCB)","beta-BHC","gamma-BHC - (Lindane)","delta-BHC","Heptachlor","Aldrin",
            "Heptachlor epoxide","Total Chlordane (sum) ----","trans-Chlordane","alpha-Endosulfan","cis-Chlordane","Dieldrin","4.4`-DDE","Endrin","Endosulfan (sum)","beta-Endosulfan","4.4`-DDD","Endrin aldehyde","Endosulfan sulfate","4.4`-DDT",
            "Endrin ketone","Methoxychlor","Sum of DDD + DDE + DDT","Sum of Aldrin + Dieldrin","Dichlorvos","Demeton-S-methyl","Monocrotophos","Dimethoate","Diazinon","Chlorpyrifos-methyl","Parathion-methyl","Malathion",
            "Fenthion","Chlorpyrifos","Parathion","Pirimphos-ethyl","Chlorfenvinphos","Bromophos-ethyl","Fenamiphos","Prothiofos","Ethion","Carbophenothion","Azinphos Methyl","Phenol","2-Chlorophenol","2-Methylphenol","3- & 4-Methylphenol",
            "2-Nitrophenol","2.4-Dimethylphenol","2.4-Dichlorophenol","2.6-Dichlorophenol","4-Chloro-3-methylphenol","2.4.6-Trichlorophenol","2.4.5-Trichlorophenol","Pentachlorophenol","Naphthalene","Acenaphthylene","Acenaphthene","Fluorene","Phenanthrene","Anthracene","Fluoranthene","Pyrene","Benz(a)anthracene",
            "Chrysene","Benzo(b+j)fluoranthene","Benzo(k)fluoranthene","Benzo(a)pyrene","Indeno(1.2.3.cd)pyrene","Dibenz(a.h)anthracene","Benzo(g.h.i)perylene","Sum of polycyclic aromatic hydrocarbons ----","Benzo(a)pyrene TEQ (zero) ----","C10 - C14 Fraction ----",
            "C15 - C28 Fraction ----","C29 - C36 Fraction ----","C10 - C36 Fraction (sum) ----",">C10 - C16 Fraction ----",">C16 - C34 Fraction ----",">C34 - C40 Fraction ----",">C10 - C40 Fraction (sum) ----",">C10 - C16 Fraction minus Naphthalene (F2) ----","Dibromo-DDE","DEF",
            "Phenol-d6","2-Chlorophenol-D4","2.4.6-Tribromophenol","2-Fluorobiphenyl","Anthracene-d10","4-Terphenyl-d14"
            ],
    "ZHE LEACHATE": ["File Name","Sample Location","Sampling Date/Time","Benzene","Toluene","Ethylbenzene","meta- & para-Xylene","Styrene","ortho-Xylene","Isopropylbenzene","n-Propylbenzene","1.3.5-Trimethylbenzene","sec-Butylbenzene","1.2.4-Trimethylbenzene","tert-Butylbenzene","p-Isopropyltoluene","n-Butylbenzene",
                     "C6 - C9 Fraction ----","C6 - C10 Fraction","C6 - C10 Fraction  minus BTEX (F1)","Total Xylenes ----","Sum of BTEX ----","Naphthalene","1.2-Dichloroethane-D4","Toluene-D8","4-Bromofluorobenzene","1.2-Dichloroethane-D4","Toluene-D8","4-Bromofluorobenzene"
            ],
}

ABBREV_TO_FULL = {
    "mefosa": "N-Methyl perfluorooctane sulfonamide",
    "etfosa": "N-Ethyl perfluorooctane sulfonamide",
    "mefose": "N-Methyl perfluorooctane sulfonamidoethanol",
    "etfose": "N-Ethyl perfluorooctane sulfonamidoethanol",
    "mefosaa": "N-Methyl perfluorooctane sulfonamidoacetic acid",
    "etfosaa": "N-Ethyl perfluorooctane sulfonamidoacetic acid"
}

CAS_TO_FULL = {
    "14808-79-8": "Sulfate",
    "63705-05-5": "Sulfur",
    "2355-31-9": "N-Methyl perfluorooctane sulfonamidoacetic acid",  # MeFOSAA
    "2991-50-6": "N-Ethyl perfluorooctane sulfonamidoacetic acid",   # EtFOSAA
    "31506-32-8": "N-Methyl perfluorooctane sulfonamide",             # MeFOSA
    "4151-50-2": "N-Ethyl perfluorooctane sulfonamide",              # EtFOSA
    "24448-09-7": "N-Methyl perfluorooctane sulfonamidoethanol",     # MeFOSE
    "1691-99-2": "N-Ethyl perfluorooctane sulfonamidoethanol",        # EtFOSE
    "7440-36-0": "Antimony",
    "7440-38-2": "Arsenic",
    "7440-41-7": "Beryllium",
    "7440-43-9": "Cadmium",
    "7440-47-3": "Chromium",
    "7440-50-8": "Copper",
    "7440-48-4": "Cobalt",
    "7440-02-0": "Nickel",
    "7439-92-1": "Lead",
    "7440-66-6": "Zinc",
    "7439-96-5": "Manganese",
    "7439-98-7": "Molybdenum",
    "7782-49-2": "Selenium",
    "7440-22-4": "Silver",
    "7440-31-5": "Tin",
    "7440-62-2": "Vanadium",
    "7440-42-8": "Boron",
    "7439-97-6": "Mercury",
    "319-84-6": "alpha-BHC",
    "118-74-1": "Hexachlorobenzene (HCB)",
    "319-85-7": "beta-BHC",
    "58-89-9": "gamma-BHC - (Lindane)",
    "319-86-8": "delta-BHC",
    "76-44-8": "Heptachlor",
    "309-00-2": "Aldrin",
    "1024-57-3": "Heptachlor epoxide",
    "5103-74-2": "trans-Chlordane",
    "959-98-8": "alpha-Endosulfan",
    "5103-71-9": "cis-Chlordane",
    "60-57-1": "Dieldrin",
    "72-55-9": "4.4`-DDE",
    "72-20-8": "Endrin",
    "33213-65-9": "beta-Endosulfan",
    "115-29-7": "^ Endosulfan (sum)",
    "72-54-8": "4.4`-DDD",
    "7421-93-4": "Endrin aldehyde",
    "1031-07-8": "Endosulfan sulfate",
    "50-29-3": "4.4`-DDT",
    "53494-70-5": "Endrin ketone",
    "72-43-5": "Methoxychlor",
    "309-00-2/60-57-1": "^ Sum of Aldrin + Dieldrin",
    "72-54-8/72-55-9/50-2": "^ Sum of DDD + DDE + DDT",
    "62-73-7":"Dichlorvos",
    "919-86-8":"Demeton-S-methyl",
    "6923-22-4":"Monocrotophos", 
    "60-51-5":"Dimethoate", 
    "333-41-5":"Diazinon", 
    "5598-13-0":"Chlorpyrifos-methyl", 
    "298-00-0":"Parathion-methyl", 
    "121-75-5":"Malathion", 
    "55-38-9":"Fenthion", 
    "2921-88-2":"Chlorpyrifos", 
    "56-38-2":"Parathion", 
    "23505-41-1":"Pirimphos-ethyl", 
    "470-90-6":"Chlorfenvinphos", 
    "4824-78-6":"Bromophos-ethyl", 
    "22224-92-6":"Fenamiphos", 
    "34643-46-4":"Prothiofos", 
    "563-12-2":"Ethion", 
    "786-19-6":"Carbophenothion",
    "86-50-0":"Azinphos Methyl",
    "108-95-2": "Phenol",
    "95-57-8": "2-Chlorophenol",
    "95-48-7": "2-Methylphenol",
    "1319-77-3": "3- & 4-Methylphenol",
    "88-75-5": "2-Nitrophenol",
    "105-67-9": "2,4-Dimethylphenol",
    "120-83-2": "2,4-Dichlorophenol",
    "87-65-0": "2,6-Dichlorophenol",
    "59-50-7": "4-Chloro-3-methylphenol",
    "88-06-2": "2,4,6-Trichlorophenol",
    "95-95-4": "2,4,5-Trichlorophenol",
    "87-86-5": "Pentachlorophenol",
    "C6_C10": "TRH NEPMC6 - C10 Fraction C6_C10",
    "71-43-2": "Benzene",
    "108-88-3": "Toluene",
    "100-41-4": "Ethylbenzene",
    "108-38-3 106-42-3": "meta- & para-Xylene",
    "95-47-6": "ortho-Xylene",
    "91-20-3": "Naphthalene",
    "21655-73-2": "Dibromo-DDE",
    "78-48-8": "DEF",
    "13127-88-3": "Phenol-d6",
    "93951-73-6": "2-Chlorophenol-D4",
    "118-79-6": "2,4,6-Tribromophenol",
    "321-60-8": "2-Fluorobiphenyl",
    "1719-06-8": "Anthracene-d10",
    "1718-51-0": "4-Terphenyl-d14",
    "17060-07-0": "1,2-Dichloroethane-D4",
    "2037-26-5": "Toluene-D8",
    "460-00-4": "4-Bromofluorobenzene",
}

NON_ANALYTE_LABELS = [
    "results", "result", "cas number", "parameter", "compound", "unit",
    "sampling date", "sample id", "sub-matrix", "matrix",
    "ep075", "ep080", "eg020t", "phenolic compounds", "btexn",
    "surrogate", "notes", "qc", "page", "work order", "client", "project",
    "EA055: Moisture Content (Dried @ 105-110°C)","EG020T:  Metals by ICP-MS","EG035T:  Recoverable Mercury by FIMS","EP005:  Organic Carbon (TOC)",
    "EP071 SG:  Petroleum Hydrocarbons - Silica gel cleanup","EP071 SG:  Recoverable Hydrocarbons - NEPM 2013 Fractions - Silica gel cleanup",
    "EP071 SG:  Recoverable Hydrocarbons - NEPM 2013 Fractions - Silica gel cleanup - Continued",
    "EP075(SIM)A: Phenolic Compounds","EP080/071:  Petroleum Hydrocarbons","EP080/071:  Recoverable Hydrocarbons - NEPM 2013 Fractions",
    "EP080: BTEXN","MW006: Thermotolerant Coliforms & E.coli by MF","EP075(SIM)S: Phenolic Compound Surrogates",
    "EP075(SIM)T: PAH Surrogates","EP080S: TPH(V)/BTEX Surrogates","EN33: TCLP Leach - Inorganics/Non-Volatile Organics (Glass Vessel)","EN33Z: ZHE TCLP Leach",
    "EP068A: Organochlorine Pesticides (OC)","EP068B: Organophosphorus Pesticides (OP)","EP068B: Organophosphorus Pesticides (OP) - Continued",
    "EP068S: Organochlorine Pesticide Surrogate","EP068T: Organophosphorus Pesticide Surrogate","EG005(ED093)C: Leachable Metals by ICPAES",
    "EG035C: Leachable Mercury by FIMS"
]

QUERY_TYPE_TO_TABLE = {
    "ds-pfas": "[Jackson].[DSPFAS]",
    "ds-int": "[Jackson].[DSInt]",
    "ds-ext": "[Jackson].[DSExt]"
}

def normalize(text):
    if not text:
        return ''
    # Replace long dash sequences with space, remove punctuation, and collapse spaces
    text = re.sub(r'[-–—]+', ' ', text)
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.lower().strip()

PARTIAL_MATCH_MAP = {
      normalize("Sum of TOP C4 - C14 Carboxylates and C4"): "Sum of TOP C4 - C14 Carboxylates and C4-C8 Sulfonates",
      normalize("^ C6 - C10 Fraction minus BTEX C6_C10-BTEX(F1)"): "TRH NEPMC6 - C10 Fraction minus BTEX",
      normalize("C10 - C14 Fraction"): "TPH Silica C10 - C14 Fraction",
      normalize("C15 - C28 Fraction"): "TPH Silica C15 - C28 Fraction",
      normalize("C29 - C36 Fraction"): "TPH Silica C29 - C36 Fraction",
      normalize("^ C10 - C36 Fraction (sum)"): "TPH Silica C10 - C36 Fraction (sum)",
      normalize(">C10 - C16 Fraction"): "TRH C10 - C16 Fraction",
      normalize(">C16 - C34 Fraction"): "TRH C16 - C34 Fraction",
      normalize(">C34 - C40 Fraction"): "TRH C34 - C40 Fraction",
      normalize("^ >C10 - C40 Fraction (sum)"): "TRH C10 - C40 Fraction (sum)",
      normalize(">C10 - C16 Fraction minus Naphthalene (F2)"): "TRH C10 - C16 Fraction minus Naphthalene",
      normalize("^ C6 - C10 Fraction minus BTEX C6_C10-BTEX (F1)"): "TRH NEPMC6 - C10 Fraction minus BTEX"
}
# === IDENTIFY SUB-MATRIX FROM TEXT ===
def extract_submatrix(page_text):
    match = re.search(r"Sub-?Matrix:\s*(.+)", page_text, re.IGNORECASE)
    return match.group(1).strip().upper() if match else None

# === CLEAN VALUE ===
def clean_value(value):
    return "NULL" if value in ("", None, "----") else f"'{value}'" if not re.match(r"^[\d\.<>=-]+$", value) else value

# === CONVERT PDF TO SQL INSERTS ===
def generate_sql_queries_from_pdf(file_bytes, filename):
    queries = []
    pdf = pdfplumber.open(BytesIO(file_bytes))
    for i, page in enumerate(pdf.pages):
        if i < 2:
            continue  # Skip cover/index pages

        page_text = page.extract_text()
        submatrix = extract_submatrix(page_text)
        if not submatrix:
            logging.info(f"Page {i+1}: No Sub-Matrix found.")
            continue

        field_map = FIELD_MAPS.get(submatrix)
        table_name = TABLE_MAP.get(submatrix)
        if not field_map or not table_name:
            logging.info(f"Page {i+1}: No field/table mapping for sub-matrix '{submatrix}'.")
            continue

        tables = page.extract_tables()
        if not tables:
            logging.info(f"Page {i+1}: No tables found.")
            continue

        for table in tables:
            if len(table) < 3:
                continue

            headers = table[0]
            if headers[3] == "----":
                continue  # Skip invalid table

            sample_id = table[0][2]
            location_row = table[0]
            datetime_row = table[1]

            for col_idx in range(3, len(location_row)):
                sample_location = location_row[col_idx]
                sample_datetime = datetime_row[col_idx]

                row_data = {
                    "File Name": filename,
                    "Sample Location": sample_location,
                    "Sampling Date/Time": sample_datetime
                }

                for row in table[2:]:
                    analyte = row[0]
                    value = row[col_idx]
                    row_data[analyte] = value

                fields = []
                values = []

                for field in field_map:
                    fields.append(f"[{field}]")
                    values.append(clean_value(row_data.get(field, "NULL")))

                query = f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({', '.join(values)});"
                queries.append(query)

    pdf.close()
    return queries

# === AZURE FUNCTION MAIN ===
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        content_type = req.headers.get('Content-Type')
        if not content_type or not content_type.startswith("multipart/form-data"):
            return func.HttpResponse("Invalid content type", status_code=400)

        body = req.get_body()
        multipart_data = decoder.MultipartDecoder(body, content_type)

        responses = []

        for part in multipart_data.parts:
            disposition = part.headers.get(b'Content-Disposition', b'').decode()
            filename_match = re.search(r'filename="(.+?)"', disposition)
            filename = filename_match.group(1) if filename_match else "Unknown"

            queries = generate_sql_queries_from_pdf(part.content, filename)

            responses.append({
                "file": filename,
                "query_count": len(queries),
                "queries": queries
            })

        return func.HttpResponse(
            body=json.dumps(responses, indent=2),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.exception("Failed to process PDF")
        return func.HttpResponse(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )