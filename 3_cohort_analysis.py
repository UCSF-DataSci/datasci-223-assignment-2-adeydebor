import polars as pl
import os

def analyze_patient_cohorts(input_file: str) -> pl.DataFrame:
    """
    Analyze patient cohorts based on BMI ranges.
    
    Args:
        input_file: Path to the input CSV file
        
    Returns:
        DataFrame containing cohort analysis results with columns:
        - bmi_range: The BMI range (e.g., "Underweight", "Normal", "Overweight", "Obese")
        - avg_glucose: Mean glucose level by BMI range
        - patient_count: Number of patients by BMI range
        - avg_age: Mean age by BMI range
    """
    # Check if input file exists
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file {input_file} not found")
    
    # Convert CSV to Parquet for efficient processing
    parquet_file = "patients_large.parquet"
    pl.read_csv(input_file).write_parquet(parquet_file)
    
    # Create a lazy query to analyze cohorts
    cohort_results = pl.scan_parquet(parquet_file).pipe(
        lambda df: df.filter((pl.col("BMI") >= 10) & (pl.col("BMI") <= 60))
    ).pipe(
        lambda df: df.select(["BMI", "Glucose", "Age"])
    ).pipe(
        lambda df: df.with_columns(
            pl.when(pl.col("BMI") < 18.5)
            .then(pl.lit("Underweight"))
            .when(pl.col("BMI") < 25)
            .then(pl.lit("Normal"))
            .when(pl.col("BMI") < 30)
            .then(pl.lit("Overweight"))
            .otherwise(pl.lit("Obese"))
            .alias("bmi_range")
        )
    ).pipe(
        lambda df: df.group_by("bmi_range").agg([
            pl.col("Glucose").mean().alias("avg_glucose"),
            pl.len().alias("patient_count"),
            pl.col("Age").mean().alias("avg_age")
        ])
    ).collect()
    
    # Clean up temporary parquet file
    if os.path.exists(parquet_file):
        os.remove(parquet_file)
    
    return cohort_results

def main():
    """Main function to run the cohort analysis."""
    # Input file - adjust path based on project structure
    input_file = os.path.join("patients_large.csv")
    
    # Alternative: use absolute path if needed
    # input_file = "/workspaces/datasci-223-assignment-2-adeydebor/patients_large.csv"
    
    try:
        # Run analysis
        results = analyze_patient_cohorts(input_file)
        
        # Print summary statistics
        print("\nCohort Analysis Summary:")
        print(results)
        
        # Print additional statistics
        total_patients = results.select(pl.col("patient_count").sum()).item()
        print(f"\nTotal patients analyzed: {total_patients}")
        
        # Sort by BMI range for better readability
        sorted_results = results.sort("bmi_range")
        print("\nDetailed Results (sorted by BMI range):")
        for row in sorted_results.iter_rows(named=True):
            print(f"BMI Range: {row['bmi_range']}")
            print(f"  - Average Glucose: {row['avg_glucose']:.2f}")
            print(f"  - Patient Count: {row['patient_count']}")
            print(f"  - Average Age: {row['avg_age']:.1f}")
            print()
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please ensure the patients_large.csv file exists in the current directory.")
    except Exception as e:
        print(f"An error occurred during analysis: {e}")

if __name__ == "__main__":
    main()
