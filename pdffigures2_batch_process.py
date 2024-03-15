import os
import subprocess
import time
import stat
from datetime import datetime

# Set the base directory where the source PDFs are located.
base_pdf_dir = "/bigdata/preston/data/pdfs"

# Set the base directory where the processed output files will be saved.
output_base = "/bigdata/preston/output"

# Define batch size
batch_size = 100  # Adjust based on your needs

def ensure_directories_exist_and_writable(paths):
    for path in paths:
        try:
            if not os.path.exists(path):
                os.makedirs(path)
            # Check if directory is writable
            if not os.access(path, os.W_OK):
                os.chmod(path, stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR)
        except Exception as e:
            print(f"Error ensuring directory {path} exists and is writable: {e}")
            exit(1)

def process_directory(category_dir, stat_dir, images_dir, data_dir, batch):
    # Generate a dynamic file name based on the current date and time
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    stat_file = os.path.join(stat_dir, f"stats_{timestamp}.json")

    # Construct the sbt command with increased JVM heap size and dynamic stat file path
    sbt_command = f'sbt -J-Xmx8g "runMain org.allenai.pdffigures2.FigureExtractorBatchCli {category_dir} ' \
                  f'-s {stat_file} ' \
                  f'-m {images_dir} ' \
                  f'-d {data_dir}"'
    print(f"Processing batch of {len(batch)} files from category: {category_dir}")
    try:
        subprocess.run(sbt_command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while executing sbt command: {e}")
        exit(1)

def process_sources_and_categories(base_pdf_dir, output_base, batch_size):
    while True:
        for source in os.listdir(base_pdf_dir):
            source_dir = os.path.join(base_pdf_dir, source)
            if os.path.isdir(source_dir):
                for category in os.listdir(source_dir):
                    category_pdf_dir = os.path.join(source_dir, category, "pdfs")
                    if os.path.isdir(category_pdf_dir):
                        stat_dir = os.path.join(output_base, source, category, "statistics")
                        images_dir = os.path.join(output_base, source, category, "images")
                        data_dir = os.path.join(output_base, source, category, "data")
                        ensure_directories_exist_and_writable([stat_dir, images_dir, data_dir])

                        pdf_files = [f for f in os.listdir(category_pdf_dir) if f.endswith('.pdf')]
                        for i in range(0, len(pdf_files), batch_size):
                            batch = pdf_files[i:i+batch_size]
                            process_directory(category_pdf_dir, stat_dir, images_dir, data_dir, batch)
                            time.sleep(5)
        print("Waiting for new files...")
        time.sleep(60)  # Adjust the sleep time as necessary

# When the script is run, process the categories and sources.
if __name__ == "__main__":
    process_sources_and_categories(base_pdf_dir, output_base, batch_size)



# import os
# import subprocess
# import shutil
# import time
# import stat

# # Set the base directory where the source PDFs are located.
# base_pdf_dir = "/home/preston/DAVAI-Lake/extract/pdfs/cancer/pdfs"

# # Set the base directory where the processed output files will be saved.
# output_base = "/home/preston/DAVAI-Lake/extract/pdfs/output"

# def ensure_directory_exists_and_writable(path):
#     try:
#         if not os.path.exists(path):
#             os.makedirs(path)
#         # Check if directory is writable
#         if not os.access(path, os.W_OK):
#             os.chmod(path, stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR)
#     except Exception as e:
#         print(f"Error ensuring directory {path} exists and is writable: {e}")
#         exit(1)

# def process_directory(category_dir, output_dir):
#     # Construct the sbt command with increased JVM heap size
#     sbt_command = f'sbt -J-Xmx8g "runMain org.allenai.pdffigures2.FigureExtractorBatchCli {category_dir} ' \
#                   f'-d {output_dir}"'
#     print(f"Processing files from category: {category_dir}")
#     try:
#         subprocess.run(sbt_command, shell=True, check=True)
#     except subprocess.CalledProcessError as e:
#         print(f"An error occurred while executing sbt command: {e}")
#         exit(1)

# def move_files_to_category_dir(nested_category_dir, category_dir):
#     # Move all files from nested category directory to the main category directory
#     for filename in os.listdir(nested_category_dir):
#         shutil.move(os.path.join(nested_category_dir, filename), os.path.join(category_dir, filename))

# def clean_up_nested_category_dir(output_dir, category):
#     # The directory to be removed is the nested category directory
#     nested_category_dir = os.path.join(output_dir, category)
    
#     # Move files up to the parent category directory
#     move_files_to_category_dir(nested_category_dir, output_dir)
    
#     # Remove the now empty nested category directory
#     os.rmdir(nested_category_dir)
#     print(f"Removed directory: {nested_category_dir}")

# def process_pdfs(base_pdf_dir, output_base):
#     category = os.path.basename(os.path.dirname(base_pdf_dir))
    
#     # Output directly to the category directory
#     output_dir = os.path.join(output_base, category)
#     ensure_directory_exists_and_writable(output_dir)
    
#     # Create a nested category directory to satisfy the sbt tool's directory structure requirements
#     nested_output_dir = os.path.join(output_dir, category)
#     ensure_directory_exists_and_writable(nested_output_dir)
    
#     pdf_files = [f for f in os.listdir(base_pdf_dir) if f.endswith('.pdf')]
#     if pdf_files:
#         process_directory(base_pdf_dir, nested_output_dir)
#         # Clean up the nested category directory after processing
#         clean_up_nested_category_dir(output_dir, category)
#     else:
#         print(f"No PDF files found in {base_pdf_dir}")

# # When the script is run, start processing the PDFs.
# if __name__ == "__main__":
#     process_pdfs(base_pdf_dir, output_base)


