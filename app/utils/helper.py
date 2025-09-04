
    def _get_unique_tuples(self, df, acted_upon: List[str], consulted: List[str]) -> List[List[str]]:
        """Get unique tuples for test execution"""
        # Combine acted_upon and consulted columns
        all_columns = acted_upon + consulted
        
        # Get unique combinations
        unique_df = df[all_columns].drop_duplicates()
        tuples = unique_df.values.tolist()
        
        logger.debug(f"Found {len(tuples)} unique tuples for columns: {all_columns}")
        return tuples
    
    
    def _expand_single_test_results_to_all_rows(self, df, tuple_results: List[Dict]) -> List[BDQTestResult]:
        """Expand tuple results to individual row results"""
        row_results = []
        
        for tuple_result in tuple_results:
            tuple_index = tuple_result['tuple_index']
            
            # Find all rows that match this tuple
            all_columns = test_mapping.acted_upon + test_mapping.consulted
            matching_rows = df[df[all_columns].apply(
                lambda row: list(row.values) == tuple_results[tuple_index].get('tuple_values', []), 
                axis=1
            )]
            
            # Create BDQTestResult for each matching row
            for _, row in matching_rows.iterrows():
                bdq_result = BDQTestResult(
                    record_id=str(row.get('occurrenceID', row.get('taxonID', 'unknown'))),
                    test_id=test_mapping.test_id,
                    status=tuple_result['status'],
                    result=tuple_result['result'],
                    comment=tuple_result['comment'],
                    amendment=None  # TODO: Extract amendment if available
                )
                row_results.append(bdq_result)
        
        return row_results