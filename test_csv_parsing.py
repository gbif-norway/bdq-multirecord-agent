#!/usr/bin/env python3

import json
import subprocess
import tempfile
import os

def test_csv_parsing():
    """Test CLI with TG2_tests.csv parsing - should load many more tests"""
    
    # Test the CSV parsing with a variety of tests from different libraries
    cli_input = {
        "requestId": "csv-parsing-test",
        "tests": [
            # geo_ref_qc tests  
            {
                "testId": "VALIDATION_COUNTRYCODE_STANDARD",
                "actedUpon": ["dwc:countryCode"],
                "consulted": [],
                "parameters": {},
                "tuples": [["NO"], ["INVALID"]]
            },
            # rec_occur_qc tests
            {
                "testId": "VALIDATION_OCCURRENCEID_NOTEMPTY",
                "actedUpon": ["dwc:occurrenceID"],
                "consulted": [],
                "parameters": {},
                "tuples": [["test-id"], [""]]
            },
            # sci_name_qc tests (might not be loaded yet, but let's test)
            {
                "testId": "VALIDATION_PHYLUM_FOUND", 
                "actedUpon": ["dwc:phylum"],
                "consulted": [],
                "parameters": {},
                "tuples": [["Chordata"], ["InvalidPhylum"]]
            },
            # event_date_qc tests (might not be loaded yet, but let's test)
            {
                "testId": "VALIDATION_EVENTDATE_INRANGE",
                "actedUpon": ["dwc:eventDate"],
                "consulted": [],
                "parameters": {},
                "tuples": [["1990-01-01"], ["3000-01-01"]]
            }
        ]
    }
    
    input_json = json.dumps(cli_input, indent=2)
    print("Testing CSV parsing capabilities:")
    print(f"Tests: {len(cli_input['tests'])}")
    for test in cli_input['tests']:
        print(f"  {test['testId']}: {len(test['tuples'])} tuples")
    
    # Write to temp files
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(input_json)
        input_file = f.name
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        output_file = f.name
    
    cli_jar = './java/bdq-cli/target/bdq-cli-1.0.0.jar'
    java_cmd = [
        'java', '-Xms256m', '-Xmx1024m',
        '-jar', cli_jar,
        f'--input={input_file}',
        f'--output={output_file}'
    ]
    
    print(f"\nExecuting: {' '.join(java_cmd)}")
    
    try:
        result = subprocess.run(
            java_cmd,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        print(f"Return code: {result.returncode}")
        
        if result.stdout:
            print(f"STDOUT: {result.stdout}")
        if result.stderr:
            print(f"STDERR: {result.stderr}")
        
        if os.path.exists(output_file):
            with open(output_file, 'r') as f:
                output_content = f.read()
            
            if output_content:
                try:
                    output_data = json.loads(output_content)
                    results = output_data.get('results', {})
                    
                    print(f"\n📊 Results Summary:")
                    working_tests = []
                    failed_tests = []
                    
                    for test_id in ["VALIDATION_COUNTRYCODE_STANDARD", "VALIDATION_OCCURRENCEID_NOTEMPTY", 
                                  "VALIDATION_PHYLUM_FOUND", "VALIDATION_EVENTDATE_INRANGE"]:
                        if test_id in results and results[test_id].get('tupleResults'):
                            working_tests.append(test_id)
                            print(f"  ✅ {test_id}: {len(results[test_id]['tupleResults'])} results")
                        else:
                            failed_tests.append(test_id)
                            print(f"  ❌ {test_id}: No results (mapping not found)")
                    
                    print(f"\n📈 CSV Parsing Success:")
                    print(f"  Working tests: {len(working_tests)}/4")
                    print(f"  Failed tests: {len(failed_tests)}/4")
                    
                    if len(working_tests) >= 2:
                        print("🎉 CSV parsing is working - multiple test libraries loaded!")
                    elif len(working_tests) >= 1:
                        print("⚠️  CSV parsing partially working - some tests loaded")
                    else:
                        print("❌ CSV parsing failed - falling back to hardcoded mappings")
                        
                    # Check for CSV loading messages in logs
                    if "test mappings from" in result.stderr:
                        mapping_line = [line for line in result.stderr.split('\n') if 'test mappings from' in line]
                        if mapping_line:
                            print(f"📄 {mapping_line[0].split('] ')[-1]}")
                        
                except json.JSONDecodeError:
                    print("❌ Could not parse JSON output")
            else:
                print("❌ Empty output file")
        else:
            print("❌ No output file created")
        
    except subprocess.TimeoutExpired:
        print("❌ CLI timed out")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        try:
            os.unlink(input_file)
            os.unlink(output_file)
        except:
            pass

if __name__ == "__main__":
    test_csv_parsing()