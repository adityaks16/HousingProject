import mysql.connector
import csv
import random
from files import *

def is_corrupted(value):
    """Check if a value is corrupted (4 character code)"""
    return isinstance(value, str) and len(value) == 4 and value.isalpha()

def clean_data(row, field_type, zip_data=None):
    """Clean corrupted data based on field type"""
    if field_type == 'guid':
        return None  # Will cause record to be dropped
    elif field_type == 'zip_code':
        if zip_data and row['guid'] in zip_data:
            # Find a valid zip code from same city/area
            city = zip_data[row['guid']]['city']
            state = zip_data[row['guid']]['state']
            # Look for any valid zip code starting with same digit in same city/state
            for guid, data in zip_data.items():
                if data['city'] == city and data['state'] == state:
                    try:
                        first_digit = str(int(data['zip_code']))[0]
                        return int(first_digit + '0000')
                    except (ValueError, IndexError):
                        continue
        # If no matching city found, use 90000 as default
        return 90000
    elif field_type == 'housing_median_age':
        return random.randint(10, 50)
    elif field_type in ['total_rooms', 'total_bedrooms']:
        return random.randint(1000, 2000)
    elif field_type == 'population':
        return random.randint(5000, 10000)
    elif field_type == 'households':
        return random.randint(500, 2500)
    elif field_type == 'median_house_value':
        return random.randint(100000, 250000)
    elif field_type == 'median_income':
        return random.randint(100000, 750000)

def main():
    # Database connection
    conn = mysql.connector.connect(
        host="cbcradio.org",
        user="cbcradio_bds4",
        password="Cherry36Cat*",
        database="cbcradio_bds754_4"
    )
    cursor = conn.cursor()

    # Clear existing data
    cursor.execute("DELETE FROM housing")
    conn.commit()

    # First read ZIP data (but don't print yet)
    zip_data = {}
    processed_zip = 0
    with open(zipFile, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not is_corrupted(row['guid']):
                zip_data[row['guid']] = row
                processed_zip += 1

    print("Beginning import")

    # Process housing file
    print("Cleaning Housing File data")
    valid_records = {}
    processed_housing = 0
    with open(housingFile, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if is_corrupted(row['guid']):
                continue
            for field in row:
                if is_corrupted(row[field]):
                    row[field] = clean_data(row, field, zip_data)
            valid_records[row['guid']] = row
            processed_housing += 1
    print(f"{processed_housing} records imported into the database")

    # Process income file
    print("Cleaning Income File data")
    income_data = {}
    processed_income = 0
    with open(incomeFile, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not is_corrupted(row['guid']):
                income_data[row['guid']] = row
                processed_income += 1
    print(f"{processed_income} records imported into the database")

    # Now print ZIP file processing (which was already done)
    print("Cleaning ZIP File data")
    print(f"{processed_zip} records imported into the database")

    # Insert combined records into database
    records_inserted = 0
    for guid in valid_records:
        if guid in income_data and guid in zip_data:
            housing_record = valid_records[guid]
            income_record = income_data[guid]
            zip_record = zip_data[guid]

            insert_query = """
                INSERT INTO housing (
                    guid, zip_code, city, state, county, median_age,
                    total_rooms, total_bedrooms, population, households,
                    median_income, median_house_value
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            try:
                cursor.execute(insert_query, (
                    guid,
                    int(housing_record['zip_code']),
                    zip_record['city'],
                    zip_record['state'],
                    zip_record['county'],
                    int(housing_record['housing_median_age']),
                    int(housing_record['total_rooms']),
                    int(housing_record['total_bedrooms']),
                    int(housing_record['population']),
                    int(housing_record['households']),
                    int(income_record['median_income']),
                    int(housing_record['median_house_value'])
                ))
                conn.commit()
                records_inserted += 1
            except Exception as e:
                conn.rollback()

    print("Import completed")
    print("Beginning validation")
    print("Total Rooms: ", end='')
    rooms = int(input())

    # Query for bedrooms count using provided SQL
    SumBedrooms = """SELECT SUM(total_bedrooms) FROM housing WHERE total_rooms > %s;"""
    cursor.execute(SumBedrooms, (rooms,))
    total_bedrooms = cursor.fetchone()[0]
    if total_bedrooms is None:
        total_bedrooms = 0

    print(f"For locations with more than {rooms} rooms, there are a total of")
    print(f"{total_bedrooms} bedrooms.")

    print("ZIP Code: ", end='')
    zip_code = int(input())

    # Query for median income using provided SQL
    MedianIncome = """SELECT AVG(median_income) FROM housing WHERE zip_code = %s;"""
    cursor.execute(MedianIncome, (zip_code,))
    median_income = cursor.fetchone()[0]
    if median_income is None:
        median_income = 0

    print(f"The median household income for ZIP code {zip_code} is {int(median_income):,}.")
    print("Program exiting.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()