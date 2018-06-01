#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Alex, Lily
#
# Created:     27/05/2018
#-------------------------------------------------------------------------------
# Coverts MRJOB tab-delimited output csv file to comma-delimited csv file
# Important note: to be able to run, you should have set your mrjob output to
# a csv file like so: > filename.csv

import csv
import argparse


def convert_csv(old_csv, new_csv):
    '''
    Writes contents of old_csv into new_csv, with proper formatting

    Inputs:
        old_csv: (file path) mrjob output that is tab-delimited
        new_csv: (file path) output file

    Returns:
        Nothing, but new csv is created
    '''
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