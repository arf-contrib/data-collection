import os
import requests
import logging
import sys
import tarfile
import hashlib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

'''
This script packages OpenVDM cruise data for R2R submission.
Creates tar.gz files for large datasets individually and bundles remaining data.
"Large" datasets are called out in R2R_LARGE_DATASETS as a comma delimited list.  
Anything NOT called out will be bundled into a "general" package
This script assumes:
a) You use OpenVDM
b) You have your cruise datasets mounted at /mnt/CruiseData
c) You can write to the R2R_OUTPUT_DIR and have a bunch of free storage space

v.1  13 Nov 2025  Julian Race - R2R packaging script

'''
# API URL to fetch cruiseID
API_URL = "http://openvdm.sikuliaq.alaska.edu/api/warehouse/getCruiseID"
SOURCE_ROOT = "/mnt/CruiseData"
LOG_FILE = "/var/log/SKQ_R2R_Package.log"

# R2R Configuration
R2R_OUTPUT_DIR = "/mnt/CruiseData/r2r_packages"  # Where to output tar.gz files
R2R_LARGE_DATASETS = ["em304", "em710", "ek80", "radar"]  # Datasets to package separately
R2R_EMAIL_TO = "notify.list@your.domain"
R2R_EMAIL_FROM = "from@yourship.domain"
R2R_SMTP_SERVER = "your.smtp.server"  #  email relay

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)


def get_cruise_id():
    """Fetch cruise ID from API."""
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        cruise_id = data.get("cruiseID")
        logging.info(f"Retrieved cruise ID: {cruise_id}")
        return cruise_id
    except requests.RequestException as e:
        logging.error(f"Error fetching cruise ID: {e}")
        return None


def calculate_md5(filepath):
    """Calculate MD5 hash of a file."""
    md5_hash = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def get_directory_size(path):
    """Get total size of directory in bytes."""
    total = 0
    try:
        for entry in os.scandir(path):
            if entry.is_file(follow_symlinks=False):
                total += entry.stat().st_size
            elif entry.is_dir(follow_symlinks=False):
                total += get_directory_size(entry.path)
    except PermissionError:
        pass
    return total


def format_bytes(bytes):
    """Format bytes to human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes < 1024.0:
            return f"{bytes:.2f} {unit}"
        bytes /= 1024.0
    return f"{bytes:.2f} PB"


def create_tarball(source_path, output_path, description, show_progress=False):
    """Create a tar.gz file from source directory."""
    logging.info(f"Creating {description}: {output_path}")
    print(f"\nCreating {description}...")
    
    try:
        if show_progress:
            # Count total files for progress bar
            total_files = sum(1 for _ in os.walk(source_path) for _ in _[2])
            files_processed = 0
            
            def progress_filter(tarinfo):
                nonlocal files_processed
                if tarinfo.isfile():
                    files_processed += 1
                    if total_files > 0:
                        percent = (files_processed / total_files) * 100
                        bar_length = 40
                        filled = int(bar_length * files_processed / total_files)
                        bar = '=' * filled + '-' * (bar_length - filled)
                        print(f'\r  Progress: [{bar}] {percent:.1f}% ({files_processed}/{total_files} files)', end='', flush=True)
                return tarinfo
            
            with tarfile.open(output_path, "w:gz") as tar:
                tar.add(source_path, arcname=os.path.basename(source_path), filter=progress_filter)
            print()  # New line after progress bar
        else:
            with tarfile.open(output_path, "w:gz") as tar:
                tar.add(source_path, arcname=os.path.basename(source_path))
        
        file_size = os.path.getsize(output_path)
        logging.info(f"Successfully created {output_path} ({format_bytes(file_size)})")
        return True
    except Exception as e:
        logging.error(f"Error creating {output_path}: {e}")
        return False


def package_for_r2r(cruise_id, source_dir):
    """Package cruise data for R2R submission."""
    print(f"\n{'='*60}")
    print("Preparing R2R Package")
    print(f"{'='*60}\n")
    
    # Check if running interactively for progress bars
    show_progress = sys.stdin.isatty()
    
    # Create output directory if it doesn't exist
    output_dir = os.path.join(R2R_OUTPUT_DIR, cruise_id)
    os.makedirs(output_dir, exist_ok=True)
    logging.info(f"R2R output directory: {output_dir}")
    
    package_info = []
    all_dirs = []
    
    # Get all directories in cruise folder
    try:
        all_dirs = [d for d in os.listdir(source_dir) 
                   if os.path.isdir(os.path.join(source_dir, d))]
    except Exception as e:
        logging.error(f"Error reading source directory: {e}")
        return
    
    # Copy root files (don't tar.gz them)
    root_files = [f for f in os.listdir(source_dir) 
                  if os.path.isfile(os.path.join(source_dir, f))]
    
    if root_files:
        print(f"\nCopying root files ({len(root_files)} files) to output directory...")
        import shutil
        for file_name in root_files:
            src_path = os.path.join(source_dir, file_name)
            dst_path = os.path.join(output_dir, file_name)
            try:
                shutil.copy2(src_path, dst_path)
                logging.info(f"  Copied {file_name}")
            except Exception as e:
                logging.error(f"  Error copying {file_name}: {e}")
        print(f"  Copied {len(root_files)} files")
    
    # Separate general directories from large datasets
    general_dirs = [d for d in all_dirs if d not in R2R_LARGE_DATASETS and d != "r2r"]
    large_dirs_found = [d for d in R2R_LARGE_DATASETS if d in all_dirs]
    
    # Calculate sizes for large datasets and sort
    print("\nCalculating directory sizes...")
    large_dataset_info = []
    for dataset in large_dirs_found:
        dataset_path = os.path.join(source_dir, dataset)
        size = get_directory_size(dataset_path)
        large_dataset_info.append({'name': dataset, 'path': dataset_path, 'size': size})
        print(f"  {dataset}: {format_bytes(size)}")
    
    # Sort by size (smallest to largest)
    large_dataset_info.sort(key=lambda x: x['size'])
    
    # STEP 1: Package general directories first
    if general_dirs:
        print(f"\n{'='*60}")
        print(f"STEP 1: Packaging general data ({len(general_dirs)} directories)")
        print(f"{'='*60}")
        general_output = os.path.join(output_dir, f"{cruise_id}_general.tar.gz")
        
        try:
            total_source_size = 0
            if show_progress:
                # Count total files for progress
                total_files = 0
                for dir_name in general_dirs:
                    dir_path = os.path.join(source_dir, dir_name)
                    total_files += sum(1 for _ in os.walk(dir_path) for _ in _[2])
                
                files_processed = 0
                
                def progress_filter(tarinfo):
                    nonlocal files_processed
                    if tarinfo.isfile():
                        files_processed += 1
                        if total_files > 0:
                            percent = (files_processed / total_files) * 100
                            bar_length = 40
                            filled = int(bar_length * files_processed / total_files)
                            bar = '=' * filled + '-' * (bar_length - filled)
                            print(f'\r  Progress: [{bar}] {percent:.1f}% ({files_processed}/{total_files} files)', end='', flush=True)
                    return tarinfo
                
                with tarfile.open(general_output, "w:gz") as tar:
                    for dir_name in general_dirs:
                        dir_path = os.path.join(source_dir, dir_name)
                        total_source_size += get_directory_size(dir_path)
                        logging.info(f"  Adding {dir_name} to general package")
                        tar.add(dir_path, arcname=dir_name, filter=progress_filter)
                print()  # New line after progress bar
            else:
                with tarfile.open(general_output, "w:gz") as tar:
                    for dir_name in general_dirs:
                        dir_path = os.path.join(source_dir, dir_name)
                        total_source_size += get_directory_size(dir_path)
                        logging.info(f"  Adding {dir_name} to general package")
                        tar.add(dir_path, arcname=dir_name)
            
            compressed_size = os.path.getsize(general_output)
            md5_hash = calculate_md5(general_output)
            
            package_info.append({
                'name': f"{cruise_id}_general.tar.gz",
                'path': general_output,
                'source_size': total_source_size,
                'compressed_size': compressed_size,
                'md5': md5_hash
            })
            print(f"\n  ✓ Successfully created general package ({format_bytes(compressed_size)})")
            logging.info(f"Successfully created general package ({format_bytes(compressed_size)})")
        except Exception as e:
            logging.error(f"Error creating general package: {e}")
            print(f"  ✗ Error creating general package: {e}")
    
    # STEP 2: Package large datasets individually (smallest to largest)
    if large_dataset_info:
        print(f"\n{'='*60}")
        print(f"STEP 2: Packaging large datasets (smallest to largest)")
        print(f"{'='*60}")
        
        for idx, dataset_info in enumerate(large_dataset_info, 1):
            dataset = dataset_info['name']
            dataset_path = dataset_info['path']
            source_size = dataset_info['size']
            
            print(f"\n[{idx}/{len(large_dataset_info)}] Packaging {dataset} ({format_bytes(source_size)})...")
            
            output_file = os.path.join(output_dir, f"{cruise_id}_{dataset}.tar.gz")
            
            if create_tarball(dataset_path, output_file, f"{dataset} dataset", show_progress=show_progress):
                compressed_size = os.path.getsize(output_file)
                md5_hash = calculate_md5(output_file)
                
                package_info.append({
                    'name': f"{cruise_id}_{dataset}.tar.gz",
                    'path': output_file,
                    'source_size': source_size,
                    'compressed_size': compressed_size,
                    'md5': md5_hash
                })
                compression_ratio = (compressed_size / source_size * 100) if source_size > 0 else 0
                print(f"  ✓ Compressed to {format_bytes(compressed_size)} ({compression_ratio:.1f}% of original)")
    
    # Log datasets not found
    missing_datasets = [d for d in R2R_LARGE_DATASETS if d not in all_dirs]
    if missing_datasets:
        logging.info(f"Large datasets not found (skipped): {', '.join(missing_datasets)}")
    
    # Write MD5 file
    md5_file = os.path.join(output_dir, f"{cruise_id}_r2r_packages.md5")
    with open(md5_file, 'w') as f:
        for info in package_info:
            f.write(f"{info['md5']}  {info['name']}\n")
    
    # Generate summary report
    summary = generate_r2r_summary(cruise_id, package_info, output_dir)
    
    # Print summary
    print(summary)
    
    # Save summary to file
    summary_file = os.path.join(output_dir, f"{cruise_id}_r2r_summary.txt")
    with open(summary_file, 'w') as f:
        f.write(summary)
    
    # Send email 
    send_r2r_email(cruise_id, summary)
    
    logging.info("R2R packaging completed")


def generate_r2r_summary(cruise_id, package_info, output_dir):
    """Generate a summary report of the R2R packages."""
    summary = f"\n{'='*70}\n"
    summary += f"R2R Package Summary - {cruise_id}\n"
    summary += f"{'='*70}\n\n"
    
    summary += f"Output Directory: {output_dir}\n\n"
    
    total_source = 0
    total_compressed = 0
    
    summary += f"{'Package':<40} {'Original':<15} {'Compressed':<15} {'Ratio':<10}\n"
    summary += f"{'-'*70}\n"
    
    for info in package_info:
        ratio = (info['compressed_size'] / info['source_size'] * 100) if info['source_size'] > 0 else 0
        summary += f"{info['name']:<40} {format_bytes(info['source_size']):<15} "
        summary += f"{format_bytes(info['compressed_size']):<15} {ratio:.1f}%\n"
        total_source += info['source_size']
        total_compressed += info['compressed_size']
    
    summary += f"{'-'*70}\n"
    overall_ratio = (total_compressed / total_source * 100) if total_source > 0 else 0
    summary += f"{'TOTAL':<40} {format_bytes(total_source):<15} "
    summary += f"{format_bytes(total_compressed):<15} {overall_ratio:.1f}%\n\n"
    
    summary += "MD5 Checksums:\n"
    summary += f"{'-'*70}\n"
    for info in package_info:
        summary += f"{info['md5']}  {info['name']}\n"
    
    summary += f"\n{'='*70}\n"
    
    return summary


def send_r2r_email(cruise_id, summary):
    """Send email with R2R package summary."""
    try:
        msg = MIMEMultipart()
        msg['From'] = R2R_EMAIL_FROM
        msg['To'] = R2R_EMAIL_TO
        msg['Subject'] = f"R2R Package Summary - {cruise_id}"
        
        body = f"R2R packaging completed for cruise {cruise_id}\n\n{summary}"
        msg.attach(MIMEText(body, 'plain'))
        
        with smtplib.SMTP(R2R_SMTP_SERVER) as server:
            server.send_message(msg)
        
        logging.info(f"Summary email sent to {R2R_EMAIL_TO}")
        print(f"\nEmail sent successfully to {R2R_EMAIL_TO}")
    except Exception as e:
        logging.error(f"Error sending email: {e}")
        print(f"\nWarning: Could not send email - {e}")


def interactive_mode():
    print("=== OpenVDM R2R Packaging Tool ===")
    
    # Get cruise ID
    print("\nFetching current cruise info from OpenVDM...")
    cruise_id = get_cruise_id()
    
    if cruise_id:
        print(f"Done. Found Cruise ID: {cruise_id}")
        custom_id = input(f"Enter a Cruise ID to use (or press enter for {cruise_id}): ").strip()
        if custom_id:
            cruise_id = custom_id
    else:
        cruise_id = input("Could not fetch cruise ID from API. Enter Cruise ID manually: ").strip()
        if not cruise_id:
            print("No cruise ID provided. Exiting.")
            return
    
    print(f"Using Cruise ID: {cruise_id}\n")
    
    source_dir = os.path.join(SOURCE_ROOT, cruise_id)
    if not os.path.exists(source_dir):
        logging.error(f"Source directory {source_dir} does not exist. Exiting.")
        return
    
    logging.info(f"Source directory: {source_dir}")
    
    # Start R2R packaging
    package_for_r2r(cruise_id, source_dir)


def main():
    logging.info("Starting SKQ R2R Packaging Script")
    
    if sys.stdin.isatty():
        # Interactive mode
        interactive_mode()
    else:
        # Non-interactive mode - fetch cruise ID and run
        cruise_id = get_cruise_id()
        if not cruise_id:
            logging.error("No valid cruise ID found. Exiting.")
            return
        
        source_dir = os.path.join(SOURCE_ROOT, cruise_id)
        if not os.path.exists(source_dir):
            logging.error(f"Source directory {source_dir} does not exist. Exiting.")
            return
        
        package_for_r2r(cruise_id, source_dir)
        logging.info("Script execution completed.")


if __name__ == "__main__":
    main()
