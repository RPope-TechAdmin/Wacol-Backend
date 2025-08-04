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
    "SOIL SAMPLE ID FIXATION ---- ---- ---- ----": ["File Name","Sample Location","Sampling Date/Time","Moisture Content ----","Antimony","Arsenic","Barium","Beryllium","Boron","Cadmium","Chromium","Cobalt","Copper","Lead","Manganese","Molybdenum","Nickel","Selenium","Tin","Zinc","Mercury","Initial pH ----",
            "After HCl pH ----","Extraction Fluid Number ----","Final pH ----","Extraction Fluid Number ----","alpha-BHC","Hexachlorobenzene (HCB)","beta-BHC","gamma-BHC - (Lindane)","delta-BHC","Heptachlor","Aldrin",
            "Heptachlor epoxide","Total Chlordane (sum) ----","trans-Chlordane","alpha-Endosulfan","cis-Chlordane","Dieldrin","4.4`-DDE","Endrin","Endosulfan (sum)","beta-Endosulfan","4.4`-DDD","Endrin aldehyde","Endosulfan sulfate","4.4`-DDT",
            "Endrin ketone","Methoxychlor","Sum of DDD + DDE + DDT","Sum of Aldrin + Dieldrin","Dichlorvos","Demeton-S-methyl","Monocrotophos","Dimethoate","Diazinon","Chlorpyrifos-methyl","Parathion-methyl","Malathion",
            "Fenthion","Chlorpyrifos","Parathion","Pirimphos-ethyl","Chlorfenvinphos","Bromophos-ethyl","Fenamiphos","Prothiofos","Ethion","Carbophenothion","Azinphos Methyl","Phenol","2-Chlorophenol","2-Methylphenol","3- & 4-Methylphenol",
            "2-Nitrophenol","2.4-Dimethylphenol","2.4-Dichlorophenol","2.6-Dichlorophenol","4-Chloro-3-methylphenol","2.4.6-Trichlorophenol","2.4.5-Trichlorophenol","Pentachlorophenol","Naphthalene","Acenaphthylene","Acenaphthene","Fluorene","Phenanthrene","Anthracene","Fluoranthene","Pyrene","Benz(a)anthracene",
            "Chrysene","Benzo(b+j)fluoranthene","Benzo(k)fluoranthene","Benzo(a)pyrene","Indeno(1.2.3.cd)pyrene","Dibenz(a.h)anthracene","Benzo(g.h.i)perylene","Sum of polycyclic aromatic hydrocarbons ----","Benzo(a)pyrene TEQ (zero) ----","Benzo(a)pyrene TEQ (half LOR) ----","Benzo(a)pyrene TEQ (LOR) ----","C6 - C9 Fraction ----","C10 - C14 Fraction ----",
            "C15 - C28 Fraction ----","C29 - C36 Fraction ----","C10 - C36 Fraction (sum) ----","C6 - C10 Fraction","C6 - C10 Fraction  minus BTEX (F1)",">C10 - C16 Fraction ----",">C16 - C34 Fraction ----",">C34 - C40 Fraction ----",">C10 - C40 Fraction (sum) ----",">C10 - C16 Fraction minus Naphthalene (F2) ----","Benzene","Toluene","Ethylbenzene","meta- & para-Xylene",
            "ortho-Xylene","Total Xylenes ----","Sum of BTEX ----","Naphthalene","Dibromo-DDE","DEF","Phenol-d6","2-Chlorophenol-D4","2.4.6-Tribromophenol","2-Fluorobiphenyl","Anthracene-d10","4-Terphenyl-d14","1.2-Dichloroethane-D4","Toluene-D8","4-Bromofluorobenzene"
            ],
    "TCLP LEACHATE SAMPLE ID FIXATION ---- ---- ---- ----": ["File Name","Sample Location","Sampling Date/Time","Moisture Content","Antimony","Arsenic","Barium","Beryllium","Boron","Cadmium","Chromium","Cobalt","Copper","Lead","Manganese","Molybdenum","Nickel","Selenium","Tin","Zinc","Mercury","alpha-BHC","Hexachlorobenzene (HCB)","beta-BHC","gamma-BHC - (Lindane)","delta-BHC","Heptachlor","Aldrin",
            "Heptachlor epoxide","Total Chlordane (sum) ----","trans-Chlordane","alpha-Endosulfan","cis-Chlordane","Dieldrin","4.4`-DDE","Endrin","Endosulfan (sum)","beta-Endosulfan","4.4`-DDD","Endrin aldehyde","Endosulfan sulfate","4.4`-DDT",
            "Endrin ketone","Methoxychlor","Sum of DDD + DDE + DDT","Sum of Aldrin + Dieldrin","Dichlorvos","Demeton-S-methyl","Monocrotophos","Dimethoate","Diazinon","Chlorpyrifos-methyl","Parathion-methyl","Malathion",
            "Fenthion","Chlorpyrifos","Parathion","Pirimphos-ethyl","Chlorfenvinphos","Bromophos-ethyl","Fenamiphos","Prothiofos","Ethion","Carbophenothion","Azinphos Methyl","Phenol","2-Chlorophenol","2-Methylphenol","3- & 4-Methylphenol",
            "2-Nitrophenol","2.4-Dimethylphenol","2.4-Dichlorophenol","2.6-Dichlorophenol","4-Chloro-3-methylphenol","2.4.6-Trichlorophenol","2.4.5-Trichlorophenol","Pentachlorophenol","Naphthalene","Acenaphthylene","Acenaphthene","Fluorene","Phenanthrene","Anthracene","Fluoranthene","Pyrene","Benz(a)anthracene",
            "Chrysene","Benzo(b+j)fluoranthene","Benzo(k)fluoranthene","Benzo(a)pyrene","Indeno(1.2.3.cd)pyrene","Dibenz(a.h)anthracene","Benzo(g.h.i)perylene","Sum of polycyclic aromatic hydrocarbons ----","Benzo(a)pyrene TEQ (zero) ----","C10 - C14 Fraction ----",
            "C15 - C28 Fraction ----","C29 - C36 Fraction ----","C10 - C36 Fraction (sum) ----",">C10 - C16 Fraction ----",">C16 - C34 Fraction ----",">C34 - C40 Fraction ----",">C10 - C40 Fraction (sum) ----",">C10 - C16 Fraction minus Naphthalene (F2) ----","Dibromo-DDE","DEF",
            "Phenol-d6","2-Chlorophenol-D4","2.4.6-Tribromophenol","2-Fluorobiphenyl","Anthracene-d10","4-Terphenyl-d14"
            ],
    "ZHE LEACHATE SAMPLE ID FIXATION ---- ---- ---- ----": ["File Name","Sample Location","Sampling Date/Time","Benzene","Toluene","Ethylbenzene","meta- & para-Xylene","Styrene","ortho-Xylene","Isopropylbenzene","n-Propylbenzene","1.3.5-Trimethylbenzene","sec-Butylbenzene","1.2.4-Trimethylbenzene","tert-Butylbenzene","p-Isopropyltoluene","n-Butylbenzene",
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

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info("Parsing multipart form data...")
        content_type = req.headers.get("Content-Type", "")
        if "multipart/form-data" not in content_type:
            return func.HttpResponse(json.dumps({"error": "Expected multipart/form-data"}), status_code=400)

        multipart_data = decoder.MultipartDecoder(req.get_body(), content_type)

        file_name, file_content = None, None
        for part in multipart_data.parts:
            content_disp = part.headers.get(b"Content-Disposition", b"").decode()
            if 'filename="' in content_disp and content_disp.endswith('.pdf"'):
                file_content = part.content
                match = re.search(r'filename="(.+?)"', content_disp)
                if match:
                    file_name = match.group(1)

        if not file_content:
            return func.HttpResponse(json.dumps({"error": "No PDF uploaded"}), status_code=400)

        combined_by_submatrix = {}

        with pdfplumber.open(BytesIO(file_content)) as pdf:
            current_submatrix = None
            for page_number, page in enumerate(pdf.pages):
                if page_number < 2:
                    continue

                text = page.extract_text() or ""
                match = re.search(r"Sub[- ]?Matrix[:\s]+([A-Za-z0-9\-/ ]+)", text, re.IGNORECASE)
                if match:
                    current_submatrix = match.group(1).strip().lower().replace(" ", "-")

                if not current_submatrix or current_submatrix not in FIELD_MAP:
                    continue

                tables = page.extract_tables()
                if not tables:
                    continue

                target_fields = FIELD_MAP[current_submatrix]
                analyte_fields = target_fields[2:]
                submatrix_rows = combined_by_submatrix.setdefault(current_submatrix, {})

                for table in tables:
                    if not table or len(table) < 3:
                        continue

                    sample_locations = table[0][3:]
                    sample_datetimes = table[1][3:]

                    for col_index, sample_location in enumerate(sample_locations):
                        if not sample_location or sample_location.strip() == '----':
                            continue

                        date_val = sample_datetimes[col_index] if col_index < len(sample_datetimes) else "NULL"
                        key = (sample_location.strip(), date_val.strip())
                        row_dict = submatrix_rows.setdefault(key, {
                            "File Name": f"'{file_name}'" if file_name else "NULL",
                            "Sample Location": f"'{sample_location.strip()}'",
                            "Sampling Date/Time": f"'{date_val.strip()}'" if date_val.strip() else "NULL"
                        })

                        i = 3
                        while i < len(table):
                            row = table[i]
                            if not row or len(row) < (col_index + 4):
                                i += 1
                                continue

                            analyte = row[0].strip()
                            normalized_analyte = normalize(analyte)

                            if not analyte or normalized_analyte in NON_ANALYTE_LABELS:
                                i += 1
                                continue

                            match = next((f for f in analyte_fields if normalize(f) == normalized_analyte), None)

                            if not match:
                                cas_hits = re.findall(r'\b\d{2,7}-\d{2}-\d\b', analyte)
                                for cas in cas_hits:
                                    if cas in CAS_TO_FULL:
                                        full_name = CAS_TO_FULL[cas]
                                        if full_name in analyte_fields:
                                            match = full_name
                                            break

                            if not match and normalized_analyte in PARTIAL_MATCH_MAP:
                                match = PARTIAL_MATCH_MAP[normalized_analyte]

                            if not match:
                                abbrev_found = re.findall(r'\b[a-z]{2,6}\b', normalized_analyte)
                                for abbrev in abbrev_found:
                                    if abbrev in ABBREV_TO_FULL:
                                        full_name = ABBREV_TO_FULL[abbrev]
                                        if full_name in analyte_fields:
                                            match = full_name
                                            break

                            if not match and len(normalized_analyte) > 10:
                                match = next((f for f in analyte_fields if normalize(f) in normalized_analyte or normalized_analyte in normalize(f)), None)

                            val = row[col_index + 3] if col_index + 3 < len(row) else None
                            if match and val:
                                val = val.strip().replace("<", "").replace("~", "")
                                row_dict[match] = val if re.match(r'^-?\d+(\.\d+)?$', val) else "NULL"

                            i += 1

        # SQL build per submatrix
        queries = []
        for submatrix_type, rows_dict in combined_by_submatrix.items():
            table_name = QUERY_TYPE_TO_TABLE.get(submatrix_type)
            if not table_name:
                continue

            target_fields = FIELD_MAP[submatrix_type]
            columns_sql = ", ".join([f"[{f}]" for f in target_fields])
            row_sqls = []

            for row_dict in rows_dict.values():
                row_values = []
                for i, field in enumerate(target_fields):
                    val = row_dict.get(field, "NULL")
                    if i < 3:
                        val = val.strip("'").replace("'", "''") if val != "NULL" else "NULL"
                        row_values.append(f"'{val}'" if val != "NULL" else "NULL")
                    else:
                        row_values.append(val if re.match(r'^-?\d+(\.\d+)?$', val) else "NULL")
                row_sqls.append(f"           ({', '.join(row_values)})")

            sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES\n" + ",\n".join(row_sqls) + ";"
            queries.append(sql)

        # Run all queries
        try:
            username = os.environ["SQL_USER"]
            password = os.environ["SQL_PASSWORD"]
            server = os.environ["SQL_SERVER"]
            db = os.environ["SQL_DB_LAB"]

            with pymssql.connect(server, username, password, db) as conn:
                with conn.cursor() as cursor:
                    for query in queries:
                        logging.info("Executing query:")
                        logging.info(query[:500])
                        cursor.execute(query)
                conn.commit()

            return func.HttpResponse(json.dumps({"status": "success", "executed_queries": len(queries)}), status_code=200)

        except Exception as e:
            logging.exception("Database insert failed.")
            return func.HttpResponse(json.dumps({"error": "DB insert failed", "details": str(e)}), status_code=500)

    except Exception as e:
        logging.exception("Unhandled exception")
        return func.HttpResponse(json.dumps({"error": "Unhandled exception", "details": str(e)}), status_code=500)
