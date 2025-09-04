#!/usr/bin/env python3

import pandas as pd
import json
import time
import subprocess
import tempfile
import os

def test_cli_local():
    print("=== Local CLI Performance Test ===")
    
    # Read the occurrence.txt file
    df = pd.read_csv('occurrence.txt', sep='\t')
    print(f"Loaded CSV: {len(df)} rows, {len(df.columns)} columns")
    
    csv_columns = list(df.columns)
    csv_columns_lower = [col.lower() for col in csv_columns]
    
    # Simulate the exact filtering logic from production
    dwc_mapping = {
        'dwc:countrycode': ['countrycode', 'country_code'],
        'dwc:country': ['country'],
        'dwc:phylum': ['phylum'],
        'dwc:decimallatitude': ['decimallatitude', 'latitude', 'lat'],
        'dwc:decimallongitude': ['decimallongitude', 'longitude', 'lon'],
        'dwc:geodeticdatum': ['geodeticdatum', 'datum'],
        'dwc:scientificname': ['scientificname', 'scientific_name', 'sciname'],
        'dwc:year': ['year'],
        'dwc:month': ['month'],
        'dwc:basisofrecord': ['basisofrecord', 'basis_of_record', 'basis'],
        'dwc:occurrenceid': ['occurrenceid', 'occurrence_id', 'id'],
        'dwc:minimumdepthinmeters': ['minimumdepthinmeters', 'min_depth', 'mindepth'],
        'dwc:maximumdepthinmeters': ['maximumdepthinmeters', 'max_depth', 'maxdepth'],
        'dwc:verbatimcoordinates': ['verbatimcoordinates', 'coordinates', 'coords'],
    }
    
    # Sample tests that should be applicable
    sample_tests = [
        {
            'testId': 'VALIDATION_COUNTRY_FOUND',
            'actedUpon': ['dwc:country'],
            'consulted': [],
            'parameters': {}
        },
        {
            'testId': 'VALIDATION_OCCURRENCEID_NOTEMPTY', 
            'actedUpon': ['dwc:occurrenceID'],
            'consulted': [],
            'parameters': {}
        },
        {
            'testId': 'VALIDATION_COORDINATES_NOTZERO',
            'actedUpon': ['dwc:decimalLatitude', 'dwc:decimalLongitude'],
            'consulted': [],
            'parameters': {}
        }
    ]
    
    applicable_tests = []
    
    print(f"\\n=== Testing Sample Tests ===")
    for test in sample_tests:
        test_columns = test['actedUpon'] + test['consulted']
        test_columns_lower = [col.lower() for col in test_columns]
        
        all_present = True
        for test_col in test_columns_lower:
            found = False
            if test_col in dwc_mapping:
                mapped_cols = dwc_mapping[test_col]
                if any(mapped_col in csv_columns_lower for mapped_col in mapped_cols):
                    found = True
            else:
                if test_col in csv_columns_lower:
                    found = True
            
            if not found:
                all_present = False
                break
        
        if all_present:
            applicable_tests.append(test)
            print(f"✓ {test['testId']} is applicable")
        else:
            print(f"✗ {test['testId']} missing columns")
    
    print(f"\\nFound {len(applicable_tests)} applicable tests")
    
    if not applicable_tests:
        print("❌ No applicable tests - cannot test CLI performance")
        return
    
    # Create tuples for the first few rows as a test
    test_rows = df.head(10)  # Just test 10 rows for performance
    print(f"Testing with {len(test_rows)} rows")
    
    # Prepare test requests like production does
    test_requests = []
    
    for test in applicable_tests:
        test_columns = test['actedUpon'] + test['consulted']
        
        # Column mapping
        column_mapping = {}
        df_cols_lower = {c.lower(): c for c in test_rows.columns}
        
        for test_col in test_columns:
            tc_lower = test_col.lower()
            if tc_lower in dwc_mapping:
                for alias in dwc_mapping[tc_lower]:
                    if alias in df_cols_lower:
                        column_mapping[test_col] = df_cols_lower[alias]
                        break
            elif tc_lower in df_cols_lower:
                column_mapping[test_col] = df_cols_lower[tc_lower]
        
        # Extract tuples
        tuples = []
        for _, row in test_rows.iterrows():
            tuple_data = []
            for test_col in test_columns:
                if test_col in column_mapping:
                    value = str(row[column_mapping[test_col]]) if pd.notna(row[column_mapping[test_col]]) else ""
                    tuple_data.append(value)
                else:
                    tuple_data.append("")
            tuples.append(tuple_data)
        
        test_request = {
            "testId": test['testId'],
            "actedUpon": test['actedUpon'],
            "consulted": test['consulted'],
            "parameters": test['parameters'],
            "tuples": tuples
        }
        test_requests.append(test_request)
    
    # Create CLI input file
    cli_input = {
        "requestId": "local-test-123",
        "tests": test_requests
    }
    
    print(f"\\n=== CLI Input ===")
    print(f"Tests: {len(test_requests)}")
    print(f"Total tuples: {sum(len(t['tuples']) for t in test_requests)}")
    
    input_json = json.dumps(cli_input, indent=2)
    print(f"Input size: {len(input_json)} bytes")
    print(f"Input preview: {input_json[:300]}...")
    
    # Write input file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(input_json)
        input_file = f.name
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        output_file = f.name
    
    print(f"\\nInput file: {input_file}")
    print(f"Output file: {output_file}")
    
    # Check if CLI JAR exists locally (it probably doesn't)
    cli_jar_paths = [
        '/opt/bdq/bdq-cli.jar',
        './bdq-cli.jar',
        './java/bdq-cli/target/bdq-cli-1.0.0.jar'
    ]
    
    cli_jar = None
    for path in cli_jar_paths:
        if os.path.exists(path):
            cli_jar = path
            break
    
    if not cli_jar:
        print(f"\\n❌ CLI JAR not found in expected locations:")
        for path in cli_jar_paths:
            print(f"  {path} - {'✓ exists' if os.path.exists(path) else '✗ not found'}")
        print("\\nTo test CLI performance, you would need to:")
        print("1. Build the Java CLI locally")
        print("2. Or copy the JAR from the Docker image")
        
        print(f"\\n📄 Generated test input file: {input_file}")
        print("You can manually test the CLI with:")
        print(f"java -jar <path-to-cli.jar> --input={input_file} --output={output_file}")
        
        return input_file, output_file
    
    # Test CLI execution
    java_cmd = [
        'java', '-Xms512m', '-Xmx2048m', '-XX:+UseG1GC', 
        '-jar', cli_jar, 
        f'--input={input_file}', 
        f'--output={output_file}'
    ]
    
    print(f"\\n=== CLI Execution ===")
    print(f"Command: {' '.join(java_cmd)}")
    
    start_time = time.time()
    try:
        result = subprocess.run(
            java_cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout for local test
        )
        end_time = time.time()
        
        execution_time = end_time - start_time
        print(f"\\n⏱️  Execution time: {execution_time:.1f} seconds")
        print(f"Return code: {result.returncode}")
        
        if result.stdout:
            print(f"STDOUT: {result.stdout}")
        if result.stderr:
            print(f"STDERR: {result.stderr}")
        
        if result.returncode == 0 and os.path.exists(output_file):
            with open(output_file, 'r') as f:
                output_content = f.read()
            
            print(f"\\n📄 Output file size: {len(output_content)} bytes")
            print(f"Output content: {output_content}")
            
            try:
                output_data = json.loads(output_content)
                print(f"\\n📊 Parsed output:")
                print(f"Keys: {list(output_data.keys())}")
                if 'results' in output_data:
                    results = output_data['results']
                    print(f"Results: {len(results)} test results")
                    for test_id, test_result in results.items():
                        tuple_results = test_result.get('tupleResults', [])
                        print(f"  {test_id}: {len(tuple_results)} tuple results")
            except json.JSONDecodeError as e:
                print(f"❌ Failed to parse output JSON: {e}")
        
    except subprocess.TimeoutExpired:
        print("❌ CLI execution timed out after 5 minutes")
    except Exception as e:
        print(f"❌ CLI execution failed: {e}")
    
    finally:
        # Cleanup
        try:
            os.unlink(input_file)
            os.unlink(output_file)
        except:
            pass
    
    return input_file, output_file

if __name__ == "__main__":
    test_cli_local()