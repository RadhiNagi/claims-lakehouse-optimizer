"""
============================================================
Healthcare Claims Data Generator
============================================================
WHAT: Creates realistic Medicare-style claims data
WHY:  We need data to demonstrate Delta Lake optimization
      (OPTIMIZE, VACUUM, ANALYZE)

Based on CMS SynPUF (Synthetic Public Use Files) structure:
https://www.cms.gov/data-research/statistics-trends-and-reports/
medicare-claims-synthetic-public-use-files

This is the SAME type of data that hospitals and insurance
companies process every day. The column names match real
healthcare industry standards.
============================================================
"""

import csv
import random
import os
from datetime import datetime, timedelta


# ============================================================
# SECTION 1: Reference Data (Real medical codes!)
# ============================================================
# WHY: Using REAL diagnosis and procedure codes makes this
#      portfolio project look professional and industry-aware.
#      These are actual ICD-10 and HCPCS codes used in hospitals.

# ICD-10 Diagnosis Codes (what's wrong with the patient)
# In real life, there are 70,000+ codes. We use common ones.
DIAGNOSIS_CODES = [
    ("J06.9", "Acute upper respiratory infection"),
    ("M54.5", "Low back pain"),
    ("E11.9", "Type 2 diabetes without complications"),
    ("I10",   "Essential hypertension"),
    ("J18.9", "Pneumonia"),
    ("K21.0", "Gastroesophageal reflux disease"),
    ("F41.1", "Generalized anxiety disorder"),
    ("M79.3", "Panniculitis"),
    ("R10.9", "Abdominal pain, unspecified"),
    ("N39.0", "Urinary tract infection"),
    ("J45.909", "Asthma, unspecified"),
    ("E78.5", "Hyperlipidemia"),
    ("G47.00", "Insomnia"),
    ("M25.511", "Pain in right shoulder"),
    ("K58.9", "Irritable bowel syndrome"),
    ("R05.9", "Cough"),
    ("H66.90", "Otitis media, unspecified"),
    ("L30.9", "Dermatitis, unspecified"),
    ("R51.9", "Headache"),
    ("Z23",   "Encounter for immunization"),
]

# HCPCS/CPT Procedure Codes (what the doctor did)
PROCEDURE_CODES = [
    ("99213", "Office visit, established patient, low complexity"),
    ("99214", "Office visit, established patient, moderate complexity"),
    ("99203", "Office visit, new patient, low complexity"),
    ("99385", "Preventive visit, 18-39 years"),
    ("99395", "Preventive visit, 40-64 years"),
    ("71046", "Chest X-ray, 2 views"),
    ("80053", "Comprehensive metabolic panel"),
    ("85025", "Complete blood count (CBC)"),
    ("93000", "Electrocardiogram (ECG)"),
    ("90471", "Immunization administration"),
    ("99232", "Subsequent hospital care, moderate"),
    ("36415", "Venipuncture (blood draw)"),
    ("81001", "Urinalysis"),
    ("87081", "Culture, bacterial"),
    ("90834", "Psychotherapy, 45 minutes"),
]

# Place of Service Codes (where the service happened)
PLACE_OF_SERVICE = [
    ("11", "Office"),
    ("21", "Inpatient Hospital"),
    ("22", "Outpatient Hospital"),
    ("23", "Emergency Room"),
    ("81", "Independent Laboratory"),
]

# US States (for provider locations)
STATES = [
    "NY", "CA", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI",
    "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
]

# Provider Specialties
SPECIALTIES = [
    "Internal Medicine", "Family Practice", "Cardiology",
    "Orthopedics", "Dermatology", "Psychiatry",
    "Emergency Medicine", "Pediatrics", "Radiology",
    "General Surgery", "Pulmonology", "Gastroenterology",
]


# ============================================================
# SECTION 2: Generate Beneficiaries (Patients)
# ============================================================
# WHY: In healthcare, patients are called "beneficiaries"
#      because they are beneficiaries of insurance coverage.
#      This matches CMS SynPUF naming conventions.

def generate_beneficiaries(num_patients=2000):
    """
    Creates synthetic patient records.
    
    Columns match CMS SynPUF structure:
    - DESYNPUF_ID: Patient unique identifier
    - BENE_BIRTH_DT: Date of birth
    - BENE_SEX_IDENT_CD: 1=Male, 2=Female
    - BENE_RACE_CD: Race code (1-5)
    - SP_STATE_CODE: State where patient lives
    - BENE_COUNTY_CD: County code
    - BENE_ESRD_IND: End-stage renal disease indicator
    - SP_ALZHDMTA: Alzheimer's indicator (1=Yes, 2=No)
    - SP_CHF: Congestive heart failure (1=Yes, 2=No)
    - SP_DIABETES: Diabetes indicator (1=Yes, 2=No)
    """
    print(f"Generating {num_patients} beneficiaries (patients)...")
    
    beneficiaries = []
    for i in range(num_patients):
        # Generate realistic age distribution (18-95)
        birth_year = random.randint(1930, 2006)
        birth_month = random.randint(1, 12)
        birth_day = random.randint(1, 28)  # Safe for all months
        
        beneficiary = {
            "DESYNPUF_ID": f"P{str(i+1).zfill(6)}",  # P000001, P000002, etc.
            "BENE_BIRTH_DT": f"{birth_year}-{birth_month:02d}-{birth_day:02d}",
            "BENE_SEX_IDENT_CD": random.choice([1, 2]),
            "BENE_RACE_CD": random.choices([1, 2, 3, 4, 5], weights=[60, 20, 10, 5, 5])[0],
            "SP_STATE_CODE": random.choice(STATES),
            "BENE_COUNTY_CD": random.randint(1, 50),
            "BENE_ESRD_IND": random.choices(["Y", "N"], weights=[3, 97])[0],
            "SP_ALZHDMTA": random.choices([1, 2], weights=[8, 92])[0],
            "SP_CHF": random.choices([1, 2], weights=[12, 88])[0],
            "SP_DIABETES": random.choices([1, 2], weights=[25, 75])[0],
        }
        beneficiaries.append(beneficiary)
    
    return beneficiaries


# ============================================================
# SECTION 3: Generate Providers (Doctors/Hospitals)
# ============================================================
# WHY: Every claim has a provider (who provided the service).
#      NPI (National Provider Identifier) is a real 10-digit
#      number assigned to every US healthcare provider.

def generate_providers(num_providers=500):
    """
    Creates synthetic provider records.
    
    In real life, you'd download from NPPES:
    https://npiregistry.cms.hhs.gov/
    """
    print(f"Generating {num_providers} providers...")
    
    providers = []
    for i in range(num_providers):
        provider = {
            "NPI": f"{random.randint(1000000000, 1999999999)}",  # 10-digit NPI
            "PROVIDER_TYPE": random.choice(SPECIALTIES),
            "PROVIDER_STATE": random.choice(STATES),
            "PROVIDER_ZIP": f"{random.randint(10000, 99999)}",
            "PROVIDER_ORGANIZATION": f"Healthcare Group {random.randint(1, 100)}",
            "IS_FACILITY": random.choices([0, 1], weights=[70, 30])[0],
        }
        providers.append(provider)
    
    return providers


# ============================================================
# SECTION 4: Generate Claims (The Main Table!)
# ============================================================
# WHY: This is the CORE table that we'll store in Delta Lake
#      and run OPTIMIZE/VACUUM/ANALYZE on.
#
#      Real healthcare companies process MILLIONS of claims daily.
#      We generate 10,000 to demonstrate the concepts.

def generate_claims(beneficiaries, providers, num_claims=10000):
    """
    Creates synthetic claim line items.
    
    Each claim represents one healthcare service:
    - A patient visited a doctor
    - The doctor performed a procedure
    - The insurance was billed an amount
    
    KEY CONCEPT: "Allowed Amount" vs "Paid Amount"
    - Allowed = what insurance agrees the service is worth
    - Paid = what insurance actually pays (usually less)
    - Patient pays the difference (copay/deductible)
    """
    print(f"Generating {num_claims} claim records...")
    
    # Date range: 2 years of claims (for time-based partitioning)
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 12, 31)
    date_range_days = (end_date - start_date).days
    
    claims = []
    for i in range(num_claims):
        # Pick random patient and provider
        patient = random.choice(beneficiaries)
        provider = random.choice(providers)
        
        # Pick random diagnosis and procedure
        diagnosis = random.choice(DIAGNOSIS_CODES)
        procedure = random.choice(PROCEDURE_CODES)
        place = random.choice(PLACE_OF_SERVICE)
        
        # Generate realistic dollar amounts
        # Higher amounts for hospital visits, lower for office visits
        if place[0] in ("21", "23"):  # Hospital or ER
            base_amount = random.uniform(500, 15000)
        else:
            base_amount = random.uniform(50, 500)
        
        allowed_amount = round(base_amount, 2)
        # Insurance pays 70-90% of allowed amount
        paid_amount = round(allowed_amount * random.uniform(0.70, 0.90), 2)
        # Patient pays the rest
        patient_pay = round(allowed_amount - paid_amount, 2)
        
        # Random service date within our range
        service_date = start_date + timedelta(days=random.randint(0, date_range_days))
        # Payment usually comes 15-90 days after service
        payment_date = service_date + timedelta(days=random.randint(15, 90))
        
        claim = {
            "CLM_ID": f"CLM{str(i+1).zfill(8)}",          # CLM00000001
            "DESYNPUF_ID": patient["DESYNPUF_ID"],          # Links to patient
            "PROVIDER_NPI": provider["NPI"],                 # Links to provider
            "CLM_FROM_DT": service_date.strftime("%Y-%m-%d"),# Service start date
            "CLM_THRU_DT": service_date.strftime("%Y-%m-%d"),# Service end date
            "CLM_PMT_DT": payment_date.strftime("%Y-%m-%d"), # When insurance paid
            "ICD10_DGNS_CD_1": diagnosis[0],                 # Diagnosis code
            "DGNS_DESC": diagnosis[1],                       # Diagnosis description
            "HCPCS_CD": procedure[0],                        # Procedure code
            "PROC_DESC": procedure[1],                       # Procedure description
            "CLM_PLACE_OF_SRVC_CD": place[0],               # Where (office/hospital)
            "PLACE_OF_SRVC_DESC": place[1],                  # Place description
            "CLM_ALLOWED_AMT": allowed_amount,               # Insurance-approved amount
            "CLM_PMT_AMT": paid_amount,                      # What insurance paid
            "CLM_BENE_PD_AMT": patient_pay,                  # What patient paid
            "SERVICE_MONTH": service_date.strftime("%Y-%m"), # For partitioning!
            "PROVIDER_STATE": provider["PROVIDER_STATE"],    # For analytics
        }
        claims.append(claim)
    
    return claims


# ============================================================
# SECTION 5: Save to CSV Files
# ============================================================

def save_to_csv(data, filename, output_dir):
    """Save list of dictionaries to CSV file"""
    filepath = os.path.join(output_dir, filename)
    
    if not data:
        print(f"WARNING: No data to save for {filename}")
        return
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    
    print(f"  Saved {len(data)} records to {filepath}")
    
    # Show first 3 rows as preview
    print(f"  Preview (first 3 rows):")
    for row in data[:3]:
        # Show only key columns to keep it readable
        keys = list(row.keys())[:5]
        preview = {k: row[k] for k in keys}
        print(f"    {preview}")
    print()


# ============================================================
# SECTION 6: Main Function (Run Everything)
# ============================================================

def main():
    """
    Generate all healthcare data files.
    
    This creates:
    - beneficiaries.csv  (2,000 patients)
    - providers.csv      (500 providers)
    - claims.csv         (10,000 claims)
    
    Total: ~12,500 records across 3 files
    """
    print("=" * 60)
    print("HEALTHCARE CLAIMS DATA GENERATOR")
    print("Based on CMS SynPUF structure")
    print("=" * 60)
    print()
    
    # Determine output directory
    # Works both locally AND inside Docker!
    output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
    
    # If running from scripts folder directly
    if not os.path.exists(output_dir):
        output_dir = os.path.join("data", "raw")
    
    # Create directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Output directory: {output_dir}")
    print()
    
    # Set random seed for reproducibility
    # WHY: Same seed = same data every time = reproducible results
    # This is important for testing and demos!
    random.seed(42)
    
    # Generate data
    beneficiaries = generate_beneficiaries(num_patients=2000)
    providers = generate_providers(num_providers=500)
    claims = generate_claims(beneficiaries, providers, num_claims=10000)
    
    # Save to CSV
    print("-" * 60)
    print("SAVING FILES:")
    print("-" * 60)
    save_to_csv(beneficiaries, "beneficiaries.csv", output_dir)
    save_to_csv(providers, "providers.csv", output_dir)
    save_to_csv(claims, "claims.csv", output_dir)
    
    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Beneficiaries (patients): {len(beneficiaries)}")
    print(f"  Providers (doctors):      {len(providers)}")
    print(f"  Claims (transactions):    {len(claims)}")
    print()
    print("  Files saved to: " + output_dir)
    print()
    print("  Next step: Load these into Delta Lake tables!")
    print("=" * 60)


if __name__ == "__main__":
    main()