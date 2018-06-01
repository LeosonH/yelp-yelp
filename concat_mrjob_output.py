# CS123 Spring '18
# Concatenate multiple csv files. This was used to concatenate
# the multiple mrjob outputs files from the multiple chunks of data
# Lily Li

import argparse
import csv


def concat_csvs(files, new_csv):
    '''
    Writes contents of files into new_csv

    Inputs:
        files: (list of file paths) mrjob output files
        new_csv: (file path) output file

    Returns:
        Nothing, but new csv is created
    '''
    with open(new_csv, 'w') as out_file:
        writer = csv.writer(out_file, delimiter=',')
        for file in files:
            with open(file, 'r') as in_file:
                reader = csv.reader(in_file, delimiter=',')
                for line in reader:
                    writer.writerow(line)


if __name__ == '__main__':
    files = ['data/mrjob_scores1_converted.csv', 'data/mrjob_scores2_converted.csv',
        'data/mrjob_scores3_converted.csv', 'data/mrjob_scores4_converted.csv',
        'data/mrjob_scores5_converted.csv', 'data/mrjob_scores6_converted.csv']
    new_csv = 'data/final_business_scores_FULL.csv'
    concat_csvs(files, new_csv)