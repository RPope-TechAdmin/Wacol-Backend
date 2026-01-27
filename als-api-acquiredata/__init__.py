import os
import io
import time
import json
import logging
import pymssql
import pyodbc
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
        "File","Sample Date","Sample Name","4.4`-DDD","4.4`-DDE","4.4`-DDT","Aldrin","alpha-BHC","alpha-Endosulfan","Azinphos Methyl","beta-BHC","beta-Endosulfan","Bromophos-ethyl","Carbophenothion","Chlorfenvinphos","Chlorpyrifos","Chlorpyrifos-methyl","cis-Chlordane","delta-BHC","Demeton-S-methyl","Diazinon","Dichlorvos","Dieldrin"
        ,"Dimethoate","Endosulfan sulfate","Endrin","Endrin aldehyde","Endrin ketone","Ethion","Fenamiphos","Fenthion","gamma-BHC - (Lindane)","Heptachlor","Heptachlor epoxide","Hexachlorobenzene (HCB)","Malathion","Methoxychlor","Monocrotophos","Parathion","Parathion-methyl","Pirimphos-ethyl","Prothiofos","Sum of Aldrin + Dieldrin"
        ,"Sum of DDD + DDE + DDT","Total Chlordane (sum)","trans-Chlordane","Mercury","Arsenic","Cadmium","Chromium","Copper","Lead","Nickel","Zinc",">C10 - C16 Fraction",">C10 - C16 Fraction minus Naphthalene (F2)",">C10 - C40 Fraction (sum)",">C16 - C34 Fraction",">C34 - C40 Fraction","C10 - C14 Fraction","C10 - C36 Fraction (sum)"
        ,"C15 - C28 Fraction","C29 - C36 Fraction","Benzene","C6 - C10 Fraction","C6 - C10 Fraction minus BTEX (F1)","C6 - C9 Fraction","Ethylbenzene","meta- & para-Xylene","Naphthalene","ortho-Xylene","Sum of BTEX","Toluene","Total Xylenes","AMPA","Glyphosate","2.4.5-T","2.4.6-T","2.4-D","2.4-DB","2.4-DP","2.6-D","4-Chlorophenoxy acetic acid"
        ,"Clopyralid","Dicamba","Fluroxypyr","MCPA","MCPB","Mecoprop","Picloram","Silvex (2.4.5-TP/Fenoprop)","Triclopyr","pH Value","Total Nitrogen as N","Total Kjeldahl Nitrogen as N","Nitrite + Nitrate as N","Nitrate as N","Nitrite as N","Ammonia as N","Total Phosphorus as P","Biochemical Oxygen Demand","Chemical Oxygen Demand"
        ,"Suspended Solids (SS)","Total Organic Carbon","Dilution Factor"
    },  
    "Fixation": {
        "File","Sample Date","Sample Name","Moisture Content","4.4`-DDD","4.4`-DDE","4.4`-DDT","Aldrin","alpha-BHC","alpha-Endosulfan","Azinphos Methyl","beta-BHC","beta-Endosulfan","Bromophos-ethyl","Carbophenothion","Chlorfenvinphos","Chlorpyrifos","Chlorpyrifos-methyl","cis-Chlordane","delta-BHC","Demeton-S-methyl","Diazinon","Dichlorvos","Dieldrin"
        ,"Dimethoate","Endosulfan (sum)","Endosulfan sulfate","Endrin","Endrin aldehyde","Endrin ketone","Ethion","Fenamiphos","Fenthion","gamma-BHC - (Lindane)","Heptachlor","Heptachlor epoxide","Hexachlorobenzene (HCB)","Malathion","Methoxychlor","Monocrotophos","Parathion","Parathion-methyl","Pirimphos-ethyl","Prothiofos","Sum of Aldrin + Dieldrin"
        ,"Sum of DDD + DDE + DDT","Total Chlordane (sum)","trans-Chlordane",">C10 - C16 Fraction",">C10 - C16 Fraction minus Naphthalene (F2)",">C10 - C40 Fraction (sum)",">C16 - C34 Fraction",">C34 - C40 Fraction","C10 - C14 Fraction","C10 - C36 Fraction (sum)","C15 - C28 Fraction","C29 - C36 Fraction","Benzene","C6 - C10 Fraction","C6 - C10 Fraction minus BTEX (F1)"
        ,"C6 - C9 Fraction","Ethylbenzene","meta- & para-Xylene","Naphthalene","ortho-Xylene","Sum of BTEX","Toluene","Total Xylenes","2.4.5-Trichlorophenol","2.4.6-Trichlorophenol","2.4-Dichlorophenol","2.4-Dimethylphenol","2.6-Dichlorophenol","2-Chlorophenol","2-Methylphenol","2-Nitrophenol","3- & 4-Methylphenol","4-Chloro-3-methylphenol"
        ,"Acenaphthene","Acenaphthylene","Anthracene","Benz(a)anthracene","Benzo(a)pyrene","Benzo(a)pyrene TEQ (half LOR)","Benzo(a)pyrene TEQ (LOR)","Benzo(a)pyrene TEQ (zero)","Benzo(b+j)fluoranthene","Benzo(g.h.i)perylene","Benzo(k)fluoranthene","Chrysene","Dibenz(a.h)anthracene","Fluoranthene","Fluorene","Indeno(1.2.3.cd)pyrene","PAH Naphthalene"
        ,"Pentachlorophenol","Phenanthrene","Phenol","Pyrene","Sum of polycyclic aromatic hydrocarbons","Antimony","Arsenic","Barium","Beryllium","Boron","Cadmium","Chromium","Cobalt","Copper","Lead","Manganese","Molybdenum","Nickel","Selenium","Tin","Zinc","Mercury","After HCl pH","Extraction Fluid Number","Final pH","Initial pH","ZHE Extraction Fluid Number"
        ,"TCLP 4.4`-DDD","TCLP 4.4`-DDE","TCLP 4.4`-DDT","TCLP Aldrin","TCLP alpha-BHC","TCLP alpha-Endosulfan","TCLP Azinphos Methyl","TCLP beta-BHC","TCLP beta-Endosulfan","TCLP Bromophos-ethyl","TCLP Carbophenothion","TCLP Chlorfenvinphos","TCLP Chlorpyrifos","TCLP Chlorpyrifos-methyl","TCLP cis-Chlordane","TCLP delta-BHC","TCLP Demeton-S-methyl"
        ,"TCLP Diazinon","TCLP Dichlorvos","TCLP Dieldrin","TCLP Dimethoate","TCLP Endosulfan sulfate","TCLP Endrin","TCLP Endrin aldehyde","TCLP Endrin ketone","TCLP Ethion","TCLP Fenamiphos","TCLP Fenthion","TCLP gamma-BHC - (Lindane)","TCLP Heptachlor","TCLP Heptachlor epoxide","TCLP Hexachlorobenzene (HCB)","TCLP Malathion","TCLP Methoxychlor"
        ,"TCLP Monocrotophos","TCLP Parathion","TCLP Parathion-methyl","TCLP Pirimphos-ethyl","TCLP Prothiofos","TCLP Sum of Aldrin + Dieldrin","TCLP Sum of DDD + DDE + DDT","TCLP Total Chlordane (sum)","TCLP trans-Chlordane","TCLP >C10 - C16 Fraction","TCLP >C10 - C16 Fraction minus Naphthalene (F2)","TCLP >C10 - C40 Fraction (sum)","TCLP >C16 - C34 Fraction"
        ,"TCLP >C34 - C40 Fraction","TCLP C10 - C14 Fraction","TCLP C10 - C36 Fraction (sum)","TCLP C15 - C28 Fraction","TCLP C29 - C36 Fraction","TCLP Benzene","TCLP C6 - C10 Fraction","TCLP C6 - C10 Fraction minus BTEX (F1)","TCLP C6 - C9 Fraction","TCLP Ethylbenzene","TCLP meta- & para-Xylene","TCLP Naphthalene","TCLP ortho-Xylene","TCLP Sum of BTEX"
        ,"TCLP Toluene","TCLP Total Xylenes","TCLP 2.4.5-Trichlorophenol","TCLP 2.4.6-Trichlorophenol","TCLP 2.4-Dichlorophenol","TCLP 2.4-Dimethylphenol","TCLP 2.6-Dichlorophenol","TCLP 2-Chlorophenol","TCLP 2-Methylphenol","TCLP 2-Nitrophenol","TCLP 3- & 4-Methylphenol","TCLP 4-Chloro-3-methylphenol","TCLP Acenaphthene","TCLP Acenaphthylene","TCLP Anthracene"
        ,"TCLP Benz(a)anthracene","TCLP Benzo(a)pyrene","TCLP Benzo(a)pyrene TEQ (zero)","TCLP Benzo(b+j)fluoranthene","TCLP Benzo(g.h.i)perylene","TCLP Benzo(k)fluoranthene","TCLP Chrysene","TCLP Dibenz(a.h)anthracene","TCLP Fluoranthene","TCLP Fluorene","TCLP Indeno(1.2.3.cd)pyrene","TCLP PAH Naphthalene","TCLP Pentachlorophenol","TCLP Phenanthrene","TCLP Phenol"
        ,"TCLP Pyrene","TCLP Sum of polycyclic aromatic hydrocarbons","TCLP Antimony","TCLP Arsenic","TCLP Barium","TCLP Beryllium","TCLP Boron","TCLP Cadmium","TCLP Chromium","TCLP Cobalt","TCLP Copper","TCLP Lead","TCLP Manganese","TCLP Molybdenum","TCLP Nickel","TCLP Selenium","TCLP Tin","TCLP Zinc","TCLP Mercury"
    },
    "Stormwater": {
        "File","Sample Date","Sample Name",">C10 - C16 Fraction",">C10 - C16 Fraction minus Naphthalene (F2)",">C10 - C40 Fraction (sum)",">C16 - C34 Fraction",">C34 - C40 Fraction","C10 - C14 Fraction","C10 - C36 Fraction (sum)","C15 - C28 Fraction","C29 - C36 Fraction","Benzene","C6 - C10 Fraction","C6 - C10 Fraction minus BTEX (F1)"
        ,"C6 - C9 Fraction","Ethylbenzene","meta- & para-Xylene","Naphthalene","ortho-Xylene","Sum of BTEX","Toluene","Total Xylenes","pH Value","Electrical Conductivity @ 25°C","Suspended Solids (SS)","Total Organic Carbon","Turbidity"
    },
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
    "4.4`-DDD": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP 4.4`-DDD",
        "standard_units":{"mg/kg"},
        "standard_field":"4.4`-DDD",
    },
    "4.4`-DDE": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP 4.4`-DDE",
        "standard_units":{"mg/kg"},
        "standard_field":"4.4`-DDE",
    },
    "4.4`-DDT": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP 4.4`-DDT",
        "standard_units":{"mg/kg"},
        "standard_field":"4.4`-DDT",
    },
    "Aldrin": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Aldrin",
        "standard_units":{"mg/kg"},
        "standard_field":"Aldrin",
    },
    "alpha-BHC": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP alpha-BHC",
        "standard_units":{"mg/kg"},
        "standard_field":"alpha-BHC",
    },
    "alpha-Endosulfan": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP alpha-Endosulfa",
        "standard_units":{"mg/kg"},
        "standard_field":"alpha-Endosulfa",
    },
    "Azinphos Methyl": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Azinphos Methyl",
        "standard_units":{"mg/kg"},
        "standard_field":"Azinphos Methyl",
    },
    "beta-BHC": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP beta-BHC",
        "standard_units":{"mg/kg"},
        "standard_field":"beta-BHC",
    },
    "beta-Endosulfan": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP beta-Endosulfan",
        "standard_units":{"mg/kg"},
        "standard_field":"beta-Endosulfan",
    },
    "Bromophos-ethyl": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Bromophos-ethyl",
        "standard_units":{"mg/kg"},
        "standard_field":"Bromophos-ethyl",
    },
    "Carbophenothion": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Carbophenothion",
        "standard_units":{"mg/kg"},
        "standard_field":"Carbophenothion",
    },
    "Chlorfenvinphos": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Chlorfenvinphos",
        "standard_units":{"mg/kg"},
        "standard_field":"Chlorfenvinphos",
    },
    "Chlorpyrifos": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Chlorpyrifos",
        "standard_units":{"mg/kg"},
        "standard_field":"Chlorpyrifos",
    },
    "Chlorpyrifos-methyl": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Chlorpyrifos-methyl",
        "standard_units":{"mg/kg"},
        "standard_field":"Chlorpyrifos-methyl",
    },
    "cis-Chlordane": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "cis-Chlordane",
        "standard_units":{"mg/kg"},
        "standard_field":"cis-Chlordane",
    },
    "delta-BHC": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP delta-BHC",
        "standard_units":{"mg/kg"},
        "standard_field":"delta-BHC",
    },
    "Demeton-S-methyl": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Demeton-S-methyl",
        "standard_units":{"mg/kg"},
        "standard_field":"Demeton-S-methyl",
    },
    "Diazinon": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Diazinon",
        "standard_units":{"mg/kg"},
        "standard_field":"Diazinon",
    },
    "Dichlorvos": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Dichlorvos",
        "standard_units":{"mg/kg"},
        "standard_field":"Dichlorvos",
    },
    "Dieldrin": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Dieldrin",
        "standard_units":{"mg/kg"},
        "standard_field":"Dieldrin",
    },
    "Dimethoate": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Dimethoate",
        "standard_units":{"mg/kg"},
        "standard_field":"Dimethoate",
    },
    "Endosulfan (sum)": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Endosulfan (sum)",
        "standard_units":{"mg/kg"},
        "standard_field":"Endosulfan (sum)",
    },
    "Endosulfan sulfate": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Endosulfan sulfate",
        "standard_units":{"mg/kg"},
        "standard_field":"Endosulfan sulfate",
    },
    "Endrin": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Endrin",
        "standard_units":{"mg/kg"},
        "standard_field":"Endrin",
    },
    "Endrin aldehyde": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Endrin aldehyde",
        "standard_units":{"mg/kg"},
        "standard_field":"Endrin aldehyde",
    },
    "Endrin ketone": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Endrin ketone",
        "standard_units":{"mg/kg"},
        "standard_field":"Endrin ketone",
    },
    "Ethion": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Ethion",
        "standard_units":{"mg/kg"},
        "standard_field":"Ethion",
    },
    "Fenamiphos": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Fenamiphos",
        "standard_units":{"mg/kg"},
        "standard_field":"Fenamiphos",
    },
    "Fenthion": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Fenthion",
        "standard_units":{"mg/kg"},
        "standard_field":"Fenthion",
    },
    "gamma-BHC - (Lindane)": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP gamma-BHC - (Lindane)",
        "standard_units":{"mg/kg"},
        "standard_field":"gamma-BHC - (Lindane)",
    },
    "Heptachlor": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Heptachlor",
        "standard_units":{"mg/kg"},
        "standard_field":"Heptachlor",
    },
    "Heptachlor epoxide": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Heptachlor epoxide",
        "standard_units":{"mg/kg"},
        "standard_field":"Heptachlor epoxide",
    },
    "Hexachlorobenzene (HCB)": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Hexachlorobenzene (HCB)",
        "standard_units":{"mg/kg"},
        "standard_field":"Hexachlorobenzene (HCB)",
    },
    "Malathion": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Malathion",
        "standard_units":{"mg/kg"},
        "standard_field":"Malathion",
    },
    "Methoxychlor": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Methoxychlor",
        "standard_units":{"mg/kg"},
        "standard_field":"Methoxychlor",
    },
    "Monocrotophos": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Monocrotophos",
        "standard_units":{"mg/kg"},
        "standard_field":"Monocrotophos",
    },
    "Parathion": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Parathion",
        "standard_units":{"mg/kg"},
        "standard_field":"Parathion",
    },
    "Parathion-methyl": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Parathion-methyl",
        "standard_units":{"mg/kg"},
        "standard_field":"Parathion-methyl",
    },
    "Pirimphos-ethyl": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Pirimphos-ethyl",
        "standard_units":{"mg/kg"},
        "standard_field":"Pirimphos-ethyl",
    },
    "Prothiofos": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Prothiofos",
        "standard_units":{"mg/kg"},
        "standard_field":"Prothiofos",
    },
    "Sum of Aldrin + Dieldrin": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Sum of Aldrin + Dieldrin",
        "standard_units":{"mg/kg"},
        "standard_field":"Sum of Aldrin + Dieldrin",
    },
    "Sum of DDD + DDE + DDT": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Sum of DDD + DDE + DDT",
        "standard_units":{"mg/kg"},
        "standard_field":"Sum of DDD + DDE + DDT",
    },
    "Total Chlordane (sum)": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Total Chlordane (sum)",
        "standard_units":{"mg/kg"},
        "standard_field":"Total Chlordane (sum)",
    },
    "trans-Chlordane": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP trans-Chlordane",
        "standard_units":{"mg/kg"},
        "standard_field":"trans-Chlordane",
    },
    "2.4.5-Trichlorophenol": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP 2.4.5-Trichlorophenol",
        "standard_units":{"mg/kg"},
        "standard_field":"2.4.5-Trichlorophenol",
    },
    "2.4.6-Trichlorophenol": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP 2.4.6-Trichlorophenol",
        "standard_units":{"mg/kg"},
        "standard_field":"2.4.6-Trichlorophenol",
    },
    "2.4-Dichlorophenol": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP 2.4-Dichlorophenol",
        "standard_units":{"mg/kg"},
        "standard_field":"2.4-Dichlorophenol",
    },
    "2.4-Dimethylphenol": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP 2.4-Dimethylphenol",
        "standard_units":{"mg/kg"},
        "standard_field":"2.4-Dimethylphenol",
    },
    "2.6-Dichlorophenol": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP 2.6-Dichlorophenol",
        "standard_units":{"mg/kg"},
        "standard_field":"2.6-Dichlorophenol",
    },
    "2-Chlorophenol": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP 2-Chlorophenol",
        "standard_units":{"mg/kg"},
        "standard_field":"2-Chlorophenol",
    },
    "2-Methylphenol": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP 2-Methylphenol",
        "standard_units":{"mg/kg"},
        "standard_field":"2-Methylphenol",
    },
    "2-Nitrophenol": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP 2-Nitrophenol",
        "standard_units":{"mg/kg"},
        "standard_field":"2-Nitrophenol",
    },
    "3- & 4-Methylphenol": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP 3- & 4-Methylphenol",
        "standard_units":{"mg/kg"},
        "standard_field":"3- & 4-Methylphenol",
    },
    "4-Chloro-3-methylphenol": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP 4-Chloro-3-methylphenol",
        "standard_units":{"mg/kg"},
        "standard_field":"4-Chloro-3-methylphenol",
    },
    "Acenaphthene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Acenaphthene",
        "standard_units":{"mg/kg"},
        "standard_field":"Acenaphthene",
    },
    "Acenaphthylene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Acenaphthylene",
        "standard_units":{"mg/kg"},
        "standard_field":"Acenaphthylene",
    },
    "Anthracene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Anthracene",
        "standard_units":{"mg/kg"},
        "standard_field":"Anthracene",
    },
    "Benz(a)anthracene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Benz(a)anthracene",
        "standard_units":{"mg/kg"},
        "standard_field":"Benz(a)anthracene",
    },
    "Benzo(a)pyrene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Benzo(a)pyrene",
        "standard_units":{"mg/kg"},
        "standard_field":"Benzo(a)pyrene",
    },
    "Benzo(a)pyrene TEQ (half LOR)": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Benzo(a)pyrene TEQ (half LOR)",
        "standard_units":{"mg/kg"},
        "standard_field":"Benzo(a)pyrene TEQ (half LOR)",
    },
    "Benzo(a)pyrene TEQ (LOR)": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Benzo(a)pyrene TEQ (LOR)",
        "standard_units":{"mg/kg"},
        "standard_field":"Benzo(a)pyrene TEQ (LOR)",
    },
    "Benzo(a)pyrene TEQ (zero)": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Benzo(a)pyrene TEQ (zero)",
        "standard_units":{"mg/kg"},
        "standard_field":"Benzo(a)pyrene TEQ (zero)",
    },
    "Benzo(b+j)fluoranthene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Benzo(b+j)fluoranthene",
        "standard_units":{"mg/kg"},
        "standard_field":"Benzo(b+j)fluoranthene",
    },
    "Benzo(g.h.i)perylene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Benzo(g.h.i)perylene",
        "standard_units":{"mg/kg"},
        "standard_field":"Benzo(g.h.i)perylene",
    },
    "Benzo(k)fluoranthene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Benzo(k)fluoranthene",
        "standard_units":{"mg/kg"},
        "standard_field":"Benzo(k)fluoranthene",
    },
    "Chrysene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Chrysene",
        "standard_units":{"mg/kg"},
        "standard_field":"Chrysene",
    },
    "Dibenz(a.h)anthracene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Dibenz(a.h)anthracene",
        "standard_units":{"mg/kg"},
        "standard_field":"Dibenz(a.h)anthracene",
    },
    "Fluoranthene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Fluoranthene",
        "standard_units":{"mg/kg"},
        "standard_field":"Fluoranthene",
    },
    "Fluorene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Fluorene",
        "standard_units":{"mg/kg"},
        "standard_field":"Fluorene",
    },
    "Indeno(1.2.3.cd)pyrene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Indeno(1.2.3.cd)pyrene",
        "standard_units":{"mg/kg"},
        "standard_field":"Indeno(1.2.3.cd)pyrene",
    },
    "PAH Naphthalene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP PAH Naphthalene",
        "standard_units":{"mg/kg"},
        "standard_field":"PAH Naphthalene",
    },
    "Pentachlorophenol": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Pentachlorophenol",
        "standard_units":{"mg/kg"},
        "standard_field":"Pentachlorophenol",
    },
    "Phenanthrene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Phenanthrene",
        "standard_units":{"mg/kg"},
        "standard_field":"Phenanthrene",
    },
    "Phenol": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Phenol",
        "standard_units":{"mg/kg"},
        "standard_field":"Phenol",
    },
    "Pyrene": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Pyrene",
        "standard_units":{"mg/kg"},
        "standard_field":"Pyrene",
    },
    "Sum of polycyclic aromatic hydrocarbons": {
        "tclp_units": {"mg/l", "µg/l"},
        "tclp_field": "TCLP Sum of polycyclic aromatic hydrocarbons",
        "standard_units":{"mg/kg"},
        "standard_field":"Sum of polycyclic aromatic hydrocarbons",
    },
}

def main(timer: func.TimerRequest) -> None:
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
        
        def connect_with_fallback(timeout_seconds: int = 60) -> pyodbc.Connection:
            sql_server= os.environ["SQL_SERVER"]
            sql_database= os.environ["SQL_DB_LAB"]
            sql_username= os.environ["SQL_USER"]
            sql_password= os.environ["SQL_PASSWORD"]
            """
            Try ODBC Driver 18 then 17. Increase Connection Timeout and retry a few times
            (useful if Azure SQL Serverless is resuming).
            """
            drivers = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]
            last_exc = None

            for driver in drivers:
                conn_str = (
                    f"Driver={{{driver}}};"
                    f"Server=tcp:{sql_server},1433;"
                    f"Database={sql_database};"
                    f"Uid={sql_username};"
                    f"Pwd={sql_password};"
                    "Encrypt=yes;"
                    "TrustServerCertificate=no;"
                    f"Connection Timeout={timeout_seconds};"
                )
                for attempt in range(3):
                    try:
                        return pyodbc.connect(conn_str)
                    except Exception as e:
                        last_exc = e
                        logging.warning(f"Connect attempt {attempt+1}/3 with {driver} failed: {e}")
                        time.sleep(3)
            # If we get here, all attempts failed
            raise last_exc


        # === Step 4: Return SQL file ===
        conn = None
        cursor = None
        try:
            conn = connect_with_fallback(timeout_seconds=60)
            cursor = conn.cursor()
            
            if not sql_statements:
                logging.info("No SQL statements to execute.")
            else:
                logging.info(f"Executing {len(sql_statements)} SQL statements...")
                for sql in sql_statements:
                    cursor.execute(sql)
                conn.commit()
                logging.info("✅ Successfully executed and committed SQL statements.")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        logging.info(f"Function finished. {len(sql_statements)} records processed.")
    except Exception as e:
        logging.error(f"Error: {e}")

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
    sql = f"INSERT INTO [Wacol].[{project_table}] ({field_list}) VALUES ({value_list});"
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