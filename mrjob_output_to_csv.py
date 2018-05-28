#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      alex, Lily
#
# Created:     27/05/2018
# Copyright:   (c) alex 2018
# Licence:     <your licence>
#-------------------------------------------------------------------------------
# Covert MRJOB sentiment score output txt file to csv
# To run, type in your terminal: python3 filename.csv
# Important note: to be able to run, you should have set your mrjob output to
# a csv file like so: > filename.csv
import csv
import argparse


def convert_csv(old_csv, new_csv):

    with open(old_csv, 'r') as f1:
        with open(new_csv, 'w') as f2:
            reader = csv.reader(f1, delimiter='\t')
            writer = csv.writer(f2, delimiter=',')
            for line in reader:
                writer.writerow(line)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
            description='Convert mrjob output csv file to properly delimited csv file',
            )

    parser.add_argument(
            'old_csv',
            type=str,
            help='The mrjob output csv file to convert.',
            )

    args = parser.parse_args()
    old_csv = args.old_csv
    new_csv = '{0}.csv'.format("".join(old_csv.split('.csv')) + "_converted")

    convert_csv(old_csv, new_csv)