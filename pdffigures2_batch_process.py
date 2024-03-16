import os
import subprocess
import time
import stat
import argparse
import fnmatch
from datetime import datetime

# Easily alterable variables for database and category
# Leave empty ('') to process all databases or categories
TARGET_DATABASE = 'arxiv'  # e.g., 'arxiv' or '' for all
TARGET_CATEGORY = 'cancer'  # e.g., 'cancer' or '' for all

# Setup argument parsing for customizable script parameters
parser = argparse.ArgumentParser(description='Process PDF files based on user-specified criteria.')
parser.add_argument('--base_pdf_dir', default="/bigdata/preston/data/pdfs", help='The base directory where source PDFs are located.')
parser.add_argument('--batch_size', type=int, default=100, help='The number of files to process in a single batch.')
parser.add_argument('--file_pattern', default='*.pdf', help='File name pattern to match for processing (e.g., "*.pdf").')
parser.add_argument('--date_after', type=lambda d: datetime.strptime(d, '%Y-%m-%d'), nargs='?', help='Only process files modified after this date (format YYYY-MM-DD).')
parser.add_argument('--date_before', type=lambda d: datetime.strptime(d, '%Y-%m-%d'), nargs='?', help='Only process files modified before this date (format YYYY-MM-DD).')

args = parser.parse_args()

def ensure_directories_exist_and_writable(paths):
    for path in paths:
        try:
            if not os.path.exists(path):
                os.makedirs(path)
            if not os.access(path, os.W_OK):
                os.chmod(path, stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR)
        except Exception as e:
            print(f"Error ensuring directory {path} exists and is writable: {e}")
            exit(1)

def file_matches_criteria(file_path):
    if args.file_pattern and not fnmatch.fnmatch(os.path.basename(file_path), args.file_pattern):
        return False
    if args.date_after or args.date_before:
        file_mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        if args.date_after and file_mod_time <= args.date_after:
            return False
        if args.date_before and file_mod_time >= args.date_before:
            return False
    return True

def process_pdf_file(pdf_file_path, output_base_dir):
    parts = pdf_file_path.split(os.sep)
    database, category = parts[-3], parts[-2]
    
    output_dir = os.path.join(output_base_dir, database, category, "output")
    stat_dir = os.path.join(output_dir, "statistics")
    images_dir = os.path.join(output_dir, "images")
    data_dir = os.path.join(output_dir, "data")
    ensure_directories_exist_and_writable([stat_dir, images_dir, data_dir])
    
    # Command to process the PDF using the specified tool
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    stat_file = os.path.join(stat_dir, f"stats_{timestamp}.json")
    images_output_prefix = os.path.join(images_dir, "figure")
    data_output_prefix = os.path.join(data_dir, "data")

    command = [
        "sbt",
        f'"runMain org.allenai.pdffigures2.FigureExtractorBatchCli {pdf_file_path} -s {stat_file} -m {images_output_prefix} -d {data_output_prefix}"'
    ]

    try:
        # Execute the command
        subprocess.run(" ".join(command), shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while processing {pdf_file_path}: {e}")

def should_process(database, category):
    # Process all if TARGET_DATABASE or TARGET_CATEGORY is empty
    process_database = True if TARGET_DATABASE == '' else database == TARGET_DATABASE
    process_category = True if TARGET_CATEGORY == '' else category == TARGET_CATEGORY
    return process_database and process_category

def process_sources_and_categories(base_pdf_dir, output_base_dir, batch_size):
    for root, dirs, files in os.walk(base_pdf_dir):
        path_parts = root.split(os.sep)
        if len(path_parts) < len(base_pdf_dir.split(os.sep)) + 2:
            continue  # Not deep enough in directory structure
        database, category = path_parts[-3], path_parts[-2]
        if not should_process(database, category):
            continue  # Skip if not matching target database/category

        pdf_files = [os.path.join(root, f) for f in files if f.endswith('.pdf') and file_matches_criteria(os.path.join(root, f))]
        for pdf_file in pdf_files:
            process_pdf_file(pdf_file, output_base_dir)

if __name__ == "__main__":
    process_sources_and_categories(args.base_pdf_dir, args.base_pdf_dir, args.batch_size)
