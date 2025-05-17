import argparse


# === Spark job for LSH Cosine recommender ===
parser = argparse.ArgumentParser(description="Spark job for LSH Cosine recommender")

parser.add_argument("input_file", help="Path to input file")
parser.add_argument("output_file", help="Path to output file")
parser.add_argument("-bucketLength", type=float, default=2.0, help="LSH parameter: bucket length (default: 2.0)")
parser.add_argument("-numHashTables", type=int, default=1, help="LSH parameter: number of hash tables (default: 1)")
parser.add_argument("-threshold", type=float, default=1.2, help="Recommendation similarity threshold (default: 1.2)")

args = parser.parse_args()

# Example usage:
# spark-submit deploy.py input.csv output.csv
# spark-submit deploy.py input.csv output.csv -bucketLength 1.5 -numHashTables 3 -threshold 0.8

input_file = args.input_file
output_file = args.output_file
bucket_length = args.bucketLength
num_hash_tables = args.numHashTables
threshold = args.threshold


# === Main Code ===