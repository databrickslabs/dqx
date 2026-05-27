class SemanticValidator:
    """
    Provides semantic validation for data quality rules to ensure 
    expressions and columns are well-formed before execution.
    """
    
    @staticmethod
    def validate_sql_expression(expr: str) -> bool:
        if not isinstance(expr, str) or not expr.strip():
            raise ValueError("Rule expression must be a non-empty string.")
        
        # Prevent destructive SQL commands in quality rules
        forbidden_keywords = ['DROP', 'TRUNCATE', 'DELETE', 'UPDATE', 'INSERT', 'ALTER']
        upper_expr = expr.upper()
        import re
        for kw in forbidden_keywords:
            if re.search(rf'\b{kw}\b', upper_expr):
                raise ValueError(f"Semantic validation failed: Forbidden keyword '{kw}' found in rule expression.")
        return True

    @staticmethod
    def validate_column_exists(dataset_columns: list, expected_column: str) -> bool:
        if expected_column not in dataset_columns:
            raise ValueError(f"Semantic validation failed: Column '{expected_column}' does not exist in the dataset.")
        return True
