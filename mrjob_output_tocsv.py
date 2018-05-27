#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      alex
#
# Created:     27/05/2018
# Copyright:   (c) alex 2018
# Licence:     <your licence>
#-------------------------------------------------------------------------------
# Covert MRJOB sentiment score output txt file to csv
import sys
import csv

def main(old_csv, new_csv):

    with open(old_csv, 'r') as f1:
        with open(new_csv, 'w') as f2:
            reader = csv.reader(f1)
            writer = csv.writer(f2)
            for line in reader:
                csv_format_line = line[0].replace('\t', ', ')
                writer.writerow([csv_format_line])

if __name__ == '__main__':
    old_path = sys.argv[1]
    main(old_path, "C:/Users/alex/Desktop/yelp_data/yelp-dataset/fixed.csv")
