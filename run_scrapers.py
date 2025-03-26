import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import importlib
import shutil
import pandas as pd
############### try to solve the system path problem here
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'scrapers')))
from scrapers.macro_handler import S3MacroManager
from scrapers.utils import setup_logging

logger = setup_logging('RunScrapers', level=logging.INFO)

def clean_tmp_directory():
    """Delete all files and folders in the tmp directory."""
    tmp_dir = os.path.join(os.getcwd(), 'tmp')
    if os.path.exists(tmp_dir):
        logger.info(f"Cleaning up temporary directory: {tmp_dir}")
        shutil.rmtree(tmp_dir)
        os.makedirs(tmp_dir)
    else:
        os.makedirs(tmp_dir)


def run_scraper_module(module_name, date, headless, overwrite):
    """Run the scraper module with the given date and headless option."""
    logger.info(f"Running scraper module: {module_name}")
    try:
        # Dynamically import the module
        scraper_module = importlib.import_module(f'scrapers.{module_name}')
        
        # Check if the module has a main function and call it with the headless option
        if hasattr(scraper_module, 'main'):
            scraper_module.main(date_from=date, headless=headless, overwrite=overwrite)  
            return module_name, "Success"
        else:
            logger.error(f"Module {module_name} does not have a 'main' function.")
            return module_name, "No main function"
    except Exception as e:
        logger.exception(f"Exception occurred while running scraper {module_name}: {e}")
        return module_name, "Failed"

def run_scrapers(directory, date, exclude_scripts, specific_scrapers=None, headless=True, overwrite = False):
    """Run all or specific scrapers with the given options."""
    # Clean the tmp directory before running the scrapers
    clean_tmp_directory()
        
    # Get a list of all Python scripts in the specified directory
    scripts = [f[:-3] for f in os.listdir(directory) if
               f.endswith('.py') and f not in exclude_scripts]

    # If specific scrapers are provided, filter the scripts to run only those
    if specific_scrapers:
        scripts = [script for script in scripts if script in specific_scrapers]

    # If specific scrapers are provided, filter the scripts to run only those
    if specific_scrapers:
        scripts = [script for script in scripts if script in specific_scrapers]
        if not scripts:
            logger.error(f"No valid scrapers found matching the provided names: {specific_scrapers}")
            return
        
    # Sort the scripts alphabetically
    scripts.sort()

    with ThreadPoolExecutor(max_workers=1) as executor:
        # Start all scripts in parallel
        futures = {executor.submit(run_scraper_module, script, date, headless, overwrite): script for script in scripts}
        script_status = {script: "Pending" for script in scripts}

        for future in as_completed(futures):
            script_path = futures[future]
            try:
                script_path, status = future.result()
                script_status[script_path] = status
            except Exception as e:
                script_status[script_path] = "Exception"
                logger.exception(f"Script {script_path} generated an exception: {e}")

            logger.info(f"Script statuses: {script_status}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run scraper scripts.")
    parser.add_argument('-df', '--date_from', help="Date to pass to scripts (format: YYYY-MM-DD)")
    parser.add_argument('-s', '--scrapers', nargs='+', help="Specific scrapers to run (e.g., merrill, morgan_stanley)")
    parser.add_argument('--headless', action='store_true', help="Run browser in headless mode (default: False)")
    parser.add_argument('--overwrite', action='store_true', help="Reapply the process and overwrite")
    args = parser.parse_args()

    # Set the directory containing the scraper scripts
    scrapers_directory = "scrapers"
    sys.path.append(os.path.abspath(scrapers_directory))

    s3 = S3MacroManager()
    dates = s3.get_latest_scrapping_date()

    if len(sys.argv) == 1:
        x = pd.DataFrame( dates )
        print( x )
        exit(0)
    # Get the date from
    if args.date_from:
        date = args.date_from
        logger.info(f"Scraping articles since {date}")
    else:
        date = max(dates, key=lambda x: x['Date'])['Date']

        logger.info(f"Scraping articles since {date}")
        if date is None:
            print("No valid date subdirectories found.")
            exit(1)

    # List of scripts to exclude
    exclude_scripts = ["__init__.py", "utils.py", "llm_functions.py", "macro_handler.py", "base_scraper.py"]

    # Run the scrapers (either all or specified ones) with the headless option
    run_scrapers(scrapers_directory, date, exclude_scripts, args.scrapers, headless=args.headless, overwrite = args.overwrite)
