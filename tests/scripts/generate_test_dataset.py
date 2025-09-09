#!/usr/bin/env python3
"""
Generate a test dataset with realistic data quality issues for BDQ testing.
Creates 400 records with various types of errors across different IE Classes.
"""

import csv
import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any

def generate_uuid() -> str:
    """Generate a UUID string."""
    return f"urn:uuid:{uuid.uuid4()}"

def generate_date_with_issues() -> Dict[str, str]:
    """Generate dates with various issues."""
    base_year = random.randint(1950, 2023)
    base_month = random.randint(1, 12)
    base_day = random.randint(1, 28)
    
    # Different date issue types
    issue_type = random.choice([
        'valid', 'invalid_format', 'inconsistent', 'future_date', 'missing_month', 'missing_day'
    ])
    
    if issue_type == 'valid':
        event_date = f"{base_year}-{base_month:02d}-{base_day:02d}"
        year, month, day = str(base_year), str(base_month), str(base_day)
    elif issue_type == 'invalid_format':
        # Invalid format like "May 15, 2020" or "15/05/2020"
        formats = [f"May {base_day}, {base_year}", f"{base_day}/{base_month}/{base_year}", 
                  f"{base_year}-{base_month}-{base_day}T12:00:00Z"]
        event_date = random.choice(formats)
        year, month, day = str(base_year), str(base_month), str(base_day)
    elif issue_type == 'inconsistent':
        # Inconsistent year/month/day
        event_date = f"{base_year}-{base_month:02d}-{base_day:02d}"
        year, month, day = str(base_year + 1), str(base_month), str(base_day)  # Wrong year
    elif issue_type == 'future_date':
        # Future date
        future_year = 2030
        event_date = f"{future_year}-{base_month:02d}-{base_day:02d}"
        year, month, day = str(future_year), str(base_month), str(base_day)
    elif issue_type == 'missing_month':
        event_date = str(base_year)
        year, month, day = str(base_year), "", str(base_day)
    else:  # missing_day
        event_date = f"{base_year}-{base_month:02d}"
        year, month, day = str(base_year), str(base_month), ""
    
    return {
        'eventDate': event_date,
        'year': year,
        'month': month,
        'day': day,
        'verbatimEventDate': f"Collected on {event_date}" if random.random() > 0.5 else ""
    }

def generate_coordinates_with_issues() -> Dict[str, str]:
    """Generate coordinates with various issues."""
    issue_type = random.choice([
        'valid', 'out_of_range', 'transposed', 'zero_coordinates', 'missing', 'invalid_format'
    ])
    
    if issue_type == 'valid':
        lat = round(random.uniform(-90, 90), 6)
        lon = round(random.uniform(-180, 180), 6)
        return {
            'decimalLatitude': str(lat),
            'decimalLongitude': str(lon),
            'geodeticDatum': 'EPSG:4326',
            'coordinateUncertaintyInMeters': str(random.randint(10, 1000))
        }
    elif issue_type == 'out_of_range':
        # Latitude > 90 or longitude > 180
        if random.random() > 0.5:
            lat = round(random.uniform(90.1, 95), 6)  # Invalid latitude
            lon = round(random.uniform(-180, 180), 6)
        else:
            lat = round(random.uniform(-90, 90), 6)
            lon = round(random.uniform(180.1, 185), 6)  # Invalid longitude
        return {
            'decimalLatitude': str(lat),
            'decimalLongitude': str(lon),
            'geodeticDatum': 'EPSG:4326',
            'coordinateUncertaintyInMeters': str(random.randint(10, 1000))
        }
    elif issue_type == 'transposed':
        # Swapped lat/lon
        lat = round(random.uniform(-90, 90), 6)
        lon = round(random.uniform(-180, 180), 6)
        return {
            'decimalLatitude': str(lon),  # Swapped!
            'decimalLongitude': str(lat),  # Swapped!
            'geodeticDatum': 'EPSG:4326',
            'coordinateUncertaintyInMeters': str(random.randint(10, 1000))
        }
    elif issue_type == 'zero_coordinates':
        return {
            'decimalLatitude': '0',
            'decimalLongitude': '0',
            'geodeticDatum': 'EPSG:4326',
            'coordinateUncertaintyInMeters': str(random.randint(10, 1000))
        }
    elif issue_type == 'missing':
        return {
            'decimalLatitude': '',
            'decimalLongitude': '',
            'geodeticDatum': '',
            'coordinateUncertaintyInMeters': ''
        }
    else:  # invalid_format
        return {
            'decimalLatitude': 'N 40° 42.5\'',
            'decimalLongitude': 'W 74° 0.7\'',
            'geodeticDatum': 'EPSG:4326',
            'coordinateUncertaintyInMeters': str(random.randint(10, 1000))
        }

def generate_taxonomy_with_issues() -> Dict[str, str]:
    """Generate taxonomic data with various issues."""
    # Real taxonomic names with some issues
    taxa = [
        {'scientificName': 'Homo sapiens', 'kingdom': 'Animalia', 'phylum': 'Chordata', 
         'class': 'Mammalia', 'order': 'Primates', 'family': 'Hominidae', 'genus': 'Homo', 
         'specificEpithet': 'sapiens', 'taxonRank': 'species'},
        {'scientificName': 'Quercus alba', 'kingdom': 'Plantae', 'phylum': 'Magnoliophyta', 
         'class': 'Magnoliopsida', 'order': 'Fagales', 'family': 'Fagaceae', 'genus': 'Quercus', 
         'specificEpithet': 'alba', 'taxonRank': 'species'},
        {'scientificName': 'Canis lupus', 'kingdom': 'Animalia', 'phylum': 'Chordata', 
         'class': 'Mammalia', 'order': 'Carnivora', 'family': 'Canidae', 'genus': 'Canis', 
         'specificEpithet': 'lupus', 'taxonRank': 'species'},
        {'scientificName': 'Pinus strobus', 'kingdom': 'Plantae', 'phylum': 'Pinophyta', 
         'class': 'Pinopsida', 'order': 'Pinales', 'family': 'Pinaceae', 'genus': 'Pinus', 
         'specificEpithet': 'strobus', 'taxonRank': 'species'},
        {'scientificName': 'Falco peregrinus', 'kingdom': 'Animalia', 'phylum': 'Chordata', 
         'class': 'Aves', 'order': 'Falconiformes', 'family': 'Falconidae', 'genus': 'Falco', 
         'specificEpithet': 'peregrinus', 'taxonRank': 'species'},
    ]
    
    issue_type = random.choice(['valid', 'invalid_name', 'missing_rank', 'inconsistent', 'non_standard_rank'])
    base_taxon = random.choice(taxa)
    
    if issue_type == 'valid':
        return base_taxon
    elif issue_type == 'invalid_name':
        # Invalid scientific name
        return {
            **base_taxon,
            'scientificName': 'Invalidus speciesname',
            'genus': 'Invalidus',
            'specificEpithet': 'speciesname'
        }
    elif issue_type == 'missing_rank':
        # Missing required taxonomic rank
        result = base_taxon.copy()
        missing_field = random.choice(['family', 'genus', 'specificEpithet'])
        result[missing_field] = ''
        return result
    elif issue_type == 'inconsistent':
        # Inconsistent taxonomy (e.g., wrong family for genus)
        return {
            **base_taxon,
            'family': 'WrongFamily'  # Wrong family for the genus
        }
    else:  # non_standard_rank
        return {
            **base_taxon,
            'taxonRank': 'variety'  # Non-standard rank
        }

def generate_metadata_with_issues() -> Dict[str, str]:
    """Generate metadata with various issues."""
    issue_type = random.choice(['valid', 'missing_required', 'invalid_basis', 'invalid_country', 'missing_license'])
    
    countries = ['United States', 'Canada', 'Mexico', 'Brazil', 'Argentina', 'United Kingdom', 'France', 'Germany']
    country_codes = ['US', 'CA', 'MX', 'BR', 'AR', 'GB', 'FR', 'DE']
    basis_of_record = ['HumanObservation', 'MachineObservation', 'PreservedSpecimen', 'MaterialSample', 'LivingSpecimen']
    
    if issue_type == 'valid':
        country_idx = random.randint(0, len(countries) - 1)
        return {
            'basisOfRecord': random.choice(basis_of_record),
            'country': countries[country_idx],
            'countryCode': country_codes[country_idx],
            'license': 'http://creativecommons.org/licenses/by/4.0/legalcode',
            'occurrenceStatus': 'present',
            'recordedBy': f"Collector_{random.randint(1, 100)}",
            'recordNumber': str(random.randint(1, 10000))
        }
    elif issue_type == 'missing_required':
        country_idx = random.randint(0, len(countries) - 1)
        result = {
            'basisOfRecord': random.choice(basis_of_record),
            'country': countries[country_idx],
            'countryCode': country_codes[country_idx],
            'license': 'http://creativecommons.org/licenses/by/4.0/legalcode',
            'occurrenceStatus': 'present',
            'recordedBy': f"Collector_{random.randint(1, 100)}",
            'recordNumber': str(random.randint(1, 10000))
        }
        # Remove a required field
        missing_field = random.choice(['basisOfRecord', 'country', 'recordedBy'])
        result[missing_field] = ''
        return result
    elif issue_type == 'invalid_basis':
        country_idx = random.randint(0, len(countries) - 1)
        return {
            'basisOfRecord': 'InvalidBasis',  # Invalid basis of record
            'country': countries[country_idx],
            'countryCode': country_codes[country_idx],
            'license': 'http://creativecommons.org/licenses/by/4.0/legalcode',
            'occurrenceStatus': 'present',
            'recordedBy': f"Collector_{random.randint(1, 100)}",
            'recordNumber': str(random.randint(1, 10000))
        }
    elif issue_type == 'invalid_country':
        return {
            'basisOfRecord': random.choice(basis_of_record),
            'country': 'InvalidCountry',  # Invalid country name
            'countryCode': 'XX',  # Invalid country code
            'license': 'http://creativecommons.org/licenses/by/4.0/legalcode',
            'occurrenceStatus': 'present',
            'recordedBy': f"Collector_{random.randint(1, 100)}",
            'recordNumber': str(random.randint(1, 10000))
        }
    else:  # missing_license
        country_idx = random.randint(0, len(countries) - 1)
        return {
            'basisOfRecord': random.choice(basis_of_record),
            'country': countries[country_idx],
            'countryCode': country_codes[country_idx],
            'license': '',  # Missing license
            'occurrenceStatus': 'present',
            'recordedBy': f"Collector_{random.randint(1, 100)}",
            'recordNumber': str(random.randint(1, 10000))
        }

def generate_elevation_with_issues() -> Dict[str, str]:
    """Generate elevation data with issues."""
    issue_type = random.choice(['valid', 'inconsistent', 'missing', 'invalid_range'])
    
    if issue_type == 'valid':
        min_elev = random.randint(0, 2000)
        max_elev = min_elev + random.randint(0, 100)
        return {
            'minimumElevationInMeters': str(min_elev),
            'maximumElevationInMeters': str(max_elev),
            'verbatimElevation': f"{min_elev}-{max_elev} m"
        }
    elif issue_type == 'inconsistent':
        # Min elevation > max elevation
        min_elev = random.randint(1000, 2000)
        max_elev = min_elev - random.randint(50, 200)
        return {
            'minimumElevationInMeters': str(min_elev),
            'maximumElevationInMeters': str(max_elev),
            'verbatimElevation': f"{min_elev}-{max_elev} m"
        }
    elif issue_type == 'missing':
        return {
            'minimumElevationInMeters': '',
            'maximumElevationInMeters': '',
            'verbatimElevation': ''
        }
    else:  # invalid_range
        return {
            'minimumElevationInMeters': '-100',  # Negative elevation
            'maximumElevationInMeters': '50000',  # Unrealistic high elevation
            'verbatimElevation': 'sea level to mountain top'
        }

def generate_record() -> Dict[str, str]:
    """Generate a single occurrence record with potential data quality issues."""
    record_id = generate_uuid()
    modified_date = datetime.now().isoformat() + '+0000'
    
    # Generate data with issues
    date_data = generate_date_with_issues()
    coord_data = generate_coordinates_with_issues()
    taxon_data = generate_taxonomy_with_issues()
    metadata = generate_metadata_with_issues()
    elevation_data = generate_elevation_with_issues()
    
    # Combine all data
    record = {
        'id': record_id,
        'modified': modified_date,
        'license': metadata['license'],
        'institutionID': 'https://ror.org/01xtthb56',
        'institutionCode': 'TEST',
        'datasetName': 'Test Dataset with Data Quality Issues',
        'basisOfRecord': metadata['basisOfRecord'],
        'dynamicProperties': '{"test": true}',
        'occurrenceID': record_id,
        'recordedBy': metadata['recordedBy'],
        'associatedReferences': 'Test reference for data quality demonstration',
        'organismID': generate_uuid(),
        'eventID': generate_uuid(),
        'parentEventID': '',
        'year': date_data['year'],
        'month': date_data['month'],
        'day': date_data['day'],
        'samplingProtocol': 'Standard sampling protocol',
        'eventRemarks': 'Test collection for BDQ validation',
        'country': metadata['country'],
        'countryCode': metadata['countryCode'],
        'stateProvince': f"State_{random.randint(1, 50)}",
        'locality': f"Test locality {random.randint(1, 1000)}",
        'minimumElevationInMeters': elevation_data['minimumElevationInMeters'],
        'maximumElevationInMeters': elevation_data['maximumElevationInMeters'],
        'verbatimElevation': elevation_data['verbatimElevation'],
        'decimalLatitude': coord_data['decimalLatitude'],
        'decimalLongitude': coord_data['decimalLongitude'],
        'geodeticDatum': coord_data['geodeticDatum'],
        'coordinateUncertaintyInMeters': coord_data['coordinateUncertaintyInMeters'],
        'verbatimCoordinates': f"{coord_data['decimalLatitude']}, {coord_data['decimalLongitude']}",
        'verbatimLatitude': coord_data['decimalLatitude'],
        'verbatimLongitude': coord_data['decimalLongitude'],
        'verbatimCoordinateSystem': 'decimal degrees',
        'verbatimSRS': 'EPSG:4326',
        'georeferencedBy': f"Georeferencer_{random.randint(1, 20)}",
        'scientificName': taxon_data['scientificName'],
        'kingdom': taxon_data['kingdom'],
        'phylum': taxon_data['phylum'],
        'class': taxon_data['class'],
        'order': taxon_data['order'],
        'family': taxon_data['family'],
        'genus': taxon_data['genus'],
        'specificEpithet': taxon_data['specificEpithet'],
        'infraspecificEpithet': '',
        'taxonRank': taxon_data['taxonRank'],
        'verbatimTaxonRank': taxon_data['taxonRank'],
        'scientificNameAuthorship': f"Author_{random.randint(1, 100)}",
        'vernacularName': f"Common name {random.randint(1, 100)}",
        'eventDate': date_data['eventDate'],
        'verbatimEventDate': date_data['verbatimEventDate'],
        'occurrenceStatus': metadata['occurrenceStatus'],
        'recordNumber': metadata['recordNumber']
    }
    
    return record

def main():
    """Generate the test dataset."""
    print("Generating test dataset with data quality issues...")
    
    # Generate 200 records
    records = []
    for i in range(200):
        if (i + 1) % 50 == 0:
            print(f"Generated {i + 1} records...")
        records.append(generate_record())
    
    # Write to CSV file with comma separation
    output_file = '/app/output/test_dataset_with_issues.csv'
    fieldnames = list(records[0].keys())
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=',')
        writer.writeheader()
        writer.writerows(records)
    
    print(f"Generated {len(records)} records with data quality issues")
    print(f"Output saved to: {output_file}")
    
    # Print summary of issues included
    print("\nData quality issues included:")
    print("- Date issues: invalid formats, inconsistent dates, future dates, missing components")
    print("- Coordinate issues: out of range, transposed lat/lon, zero coordinates, missing data")
    print("- Taxonomy issues: invalid names, missing ranks, inconsistent classification")
    print("- Metadata issues: missing required fields, invalid basis of record, invalid countries")
    print("- Elevation issues: inconsistent min/max, missing data, invalid ranges")

if __name__ == "__main__":
    main()
