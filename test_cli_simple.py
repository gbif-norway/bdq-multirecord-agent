#!/usr/bin/env python3

import csv
import json
import time
import subprocess
import tempfile
import os

def test_cli_simple():
    print("=== Simple Local CLI Test ===")
    
    # Read the occurrence.txt file
    with open('occurrence.txt', 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')
        rows = list(reader)
    
    if not rows:
        print("❌ Could not read occurrence.txt")
        return
    
    print(f"Loaded CSV: {len(rows)} rows")
    print(f"Columns: {list(rows[0].keys())}")
    
    # Take just first 5 rows for quick test
    test_rows = rows[:5]
    print(f"Testing with {len(test_rows)} rows for quick performance check")
    
    # Create a simple test request
    test_request = {
        "testId": "VALIDATION_COUNTRY_FOUND",
        "actedUpon": ["dwc:country"],
        "consulted": [],
        "parameters": {},
        "tuples": []
    }
    
    # Extract country values
    for row in test_rows:
        country = row.get('country', '')
        test_request["tuples"].append([country])
    
    cli_input = {
        "requestId": "simple-test-123",
        "tests": [test_request]
    }
    
    input_json = json.dumps(cli_input, indent=2)
    print(f"\\n=== CLI Input ===")
    print(f"Input size: {len(input_json)} bytes")
    print(f"Test: {test_request['testId']}")
    print(f"Tuples: {len(test_request['tuples'])}")
    print(f"Sample tuple: {test_request['tuples'][0] if test_request['tuples'] else 'None'}")
    
    # Write to temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write(input_json)
        input_file = f.name
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        output_file = f.name
    
    print(f"\\nGenerated CLI test files:")
    print(f"Input: {input_file}")
    print(f"Output: {output_file}")
    
    # Check for CLI JAR
    potential_jars = [
        '/opt/bdq/bdq-cli.jar',
        './bdq-cli.jar', 
        './java/bdq-cli/target/bdq-cli-1.0.0.jar',
        './java/bdq-cli/target/bdq-cli.jar'
    ]
    
    cli_jar = None
    print(f"\\n=== Looking for CLI JAR ===")
    for jar_path in potential_jars:
        exists = os.path.exists(jar_path)
        print(f"  {jar_path}: {'✓' if exists else '✗'}")
        if exists and not cli_jar:
            cli_jar = jar_path
    
    if not cli_jar:
        print(f"\\n❌ No CLI JAR found. The input file has been generated at:")
        print(f"   {input_file}")
        print(f"\\nTo test manually, run:")
        print(f"   java -jar <path-to-cli.jar> --input={input_file} --output={output_file}")
        print(f"\\nYou can also examine the input format:")
        
        with open(input_file, 'r') as f:
            content = f.read()
        print(f"\\n📄 Input file content:")
        print(content)
        
        return input_file, output_file
    
    # Test with the CLI
    print(f"\\n=== Testing CLI: {cli_jar} ===")
    
    java_cmd = [
        'java', '-Xms256m', '-Xmx1024m',
        '-jar', cli_jar,
        f'--input={input_file}',
        f'--output={output_file}'
    ]
    
    print(f"Command: {' '.join(java_cmd)}")
    
    start_time = time.time()
    try:
        result = subprocess.run(
            java_cmd,
            capture_output=True,
            text=True,
            timeout=120  # 2 minute timeout
        )
        end_time = time.time()
        
        execution_time = end_time - start_time
        print(f"\\n⏱️  Execution time: {execution_time:.1f} seconds")
        print(f"Return code: {result.returncode}")
        print(f"Performance: {execution_time/len(test_rows):.2f} seconds per row")
        
        if result.stdout:
            print(f"\\n📤 STDOUT ({len(result.stdout)} chars):")
            print(result.stdout[:500] + ("..." if len(result.stdout) > 500 else ""))
        
        if result.stderr:
            print(f"\\n❌ STDERR ({len(result.stderr)} chars):")
            print(result.stderr[:500] + ("..." if len(result.stderr) > 500 else ""))
        
        # Check output file
        if os.path.exists(output_file):
            with open(output_file, 'r') as f:
                output_content = f.read()
            
            print(f"\\n📥 Output file ({len(output_content)} bytes):")
            print(output_content[:500] + ("..." if len(output_content) > 500 else ""))
            
            if output_content:
                try:
                    output_data = json.loads(output_content)
                    print(f"\\n📊 Parsed results:")
                    print(f"Top-level keys: {list(output_data.keys())}")
                    
                    if 'results' in output_data:
                        results = output_data['results']
                        print(f"Results keys: {list(results.keys())}")
                        
                        for test_id, result_data in results.items():
                            tuple_results = result_data.get('tupleResults', [])
                            print(f"  {test_id}: {len(tuple_results)} results")
                            if tuple_results:
                                sample = tuple_results[0]
                                print(f"    Sample: {sample}")
                    
                except json.JSONDecodeError as e:
                    print(f"❌ Could not parse JSON output: {e}")
        else:
            print(f"\\n❌ Output file was not created")
        
    except subprocess.TimeoutExpired:
        print(f"\\n❌ CLI timed out after 2 minutes")
        print("This suggests serious performance issues!")
    except Exception as e:
        print(f"\\n❌ Error running CLI: {e}")
    
    # Performance analysis
    if 'execution_time' in locals():
        if execution_time > 10:
            print(f"\\n🚨 PERFORMANCE ISSUE:")
            print(f"   {execution_time:.1f}s for {len(test_rows)} rows = {execution_time/len(test_rows):.1f}s per row")
            print(f"   Extrapolated for 200 rows: {(execution_time/len(test_rows)*200):.0f}s = {(execution_time/len(test_rows)*200/60):.1f} minutes")
            print(f"   This matches the production issue!")
        else:
            print(f"\\n✅ Performance looks reasonable: {execution_time:.1f}s for {len(test_rows)} rows")
    
    return input_file, output_file

if __name__ == "__main__":
    try:
        test_cli_simple()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()