#!/usr/bin/env python3

import csv
import json
import time
import subprocess
import tempfile
import os

def test_cli_simple_occur():
    print("=== Testing rec_occur_qc Tests ===")
    
    # Read the occurrence.txt file
    with open('occurrence.txt', 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')
        rows = list(reader)
    
    print(f"Loaded CSV: {len(rows)} rows")
    
    # Take first 10 rows for performance test
    test_rows = rows[:10]
    print(f"Testing with {len(test_rows)} rows")
    
    # Test rec_occur_qc tests that should work
    test_requests = [
        {
            "testId": "VALIDATION_OCCURRENCEID_NOTEMPTY",
            "actedUpon": ["dwc:occurrenceID"],
            "consulted": [],
            "parameters": {},
            "tuples": [[row.get('occurrenceID', '')] for row in test_rows]
        },
        {
            "testId": "VALIDATION_BASISOFRECORD_NOTEMPTY", 
            "actedUpon": ["dwc:basisOfRecord"],
            "consulted": [],
            "parameters": {},
            "tuples": [[row.get('basisOfRecord', '')] for row in test_rows]
        }
    ]
    
    cli_input = {
        "requestId": "occur-test-123",
        "tests": test_requests
    }
    
    input_json = json.dumps(cli_input, indent=2)
    print(f"\\n=== CLI Input ===")
    print(f"Tests: {len(test_requests)}")
    for test in test_requests:
        print(f"  {test['testId']}: {len(test['tuples'])} tuples")
        print(f"    Sample: {test['tuples'][0] if test['tuples'] else 'None'}")
    
    # Write to temp files
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(input_json)
        input_file = f.name
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        output_file = f.name
    
    cli_jar = './java/bdq-cli/target/bdq-cli-1.0.0.jar'
    if not os.path.exists(cli_jar):
        print(f"❌ CLI JAR not found at {cli_jar}")
        return
    
    # Test with the CLI
    java_cmd = [
        'java', '-Xms256m', '-Xmx1024m',
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
            timeout=60
        )
        end_time = time.time()
        
        execution_time = end_time - start_time
        print(f"\\n⏱️  Execution time: {execution_time:.1f} seconds")
        print(f"Return code: {result.returncode}")
        print(f"Performance: {execution_time/len(test_rows):.2f} seconds per row")
        
        if result.stdout:
            print(f"\\n📤 STDOUT:")
            print(result.stdout)
        
        if result.stderr:
            print(f"\\n❌ STDERR:")
            print(result.stderr)
        
        # Check output
        if os.path.exists(output_file):
            with open(output_file, 'r') as f:
                output_content = f.read()
            
            print(f"\\n📥 Output ({len(output_content)} bytes):")
            print(output_content)
            
            if output_content:
                try:
                    output_data = json.loads(output_content)
                    print(f"\\n📊 Analysis:")
                    
                    if 'results' in output_data:
                        results = output_data['results']
                        total_results = 0
                        
                        for test_id, result_data in results.items():
                            tuple_results = result_data.get('tupleResults', [])
                            total_results += len(tuple_results)
                            print(f"  {test_id}: {len(tuple_results)} results")
                            
                            if tuple_results:
                                sample = tuple_results[0]
                                print(f"    Sample result: {sample}")
                        
                        print(f"\\nTotal results: {total_results}")
                        if total_results > 0:
                            print("✅ CLI is producing results!")
                        else:
                            print("❌ CLI produced no results")
                    
                    if 'errors' in output_data:
                        errors = output_data['errors']
                        if errors:
                            print(f"\\nErrors: {errors}")
                        else:
                            print("\\nNo errors reported")
                    
                except json.JSONDecodeError as e:
                    print(f"❌ Could not parse JSON: {e}")
        
    except subprocess.TimeoutExpired:
        print(f"\\n❌ CLI timed out")
    except Exception as e:
        print(f"\\n❌ Error: {e}")
    
    finally:
        # Cleanup
        try:
            os.unlink(input_file)
            os.unlink(output_file)
        except:
            pass

if __name__ == "__main__":
    test_cli_simple_occur()