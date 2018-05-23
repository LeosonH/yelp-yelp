import csv
import argparse

holder_dict = {}

def clean_csv(old_path, new_path):
	# Clean csvs to remove newlines from long text fields

	with open(old_path, 'r', encoding = 'utf-8') as csvfile:
		with open(new_path, 'w', encoding='utf-8') as csvfile2:
			reader = csv.reader(csvfile)
			writer = csv.writer(csvfile2)
			for row in reader:
				stripped = row[-1].replace('\n', '')
				to_write = row[:-1]
				to_write.append(stripped) 
				writer.writerow(to_write)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
            description='Clean csvs to remove newlines within cells',
            )

    parser.add_argument(
            'old_csv',
            type=str,
            help='The csv file to clean.',
            )

    args = parser.parse_args()
    old_csv = args.old_csv
    new_csv = '{0}.csv'.format("".join(old_csv.split('.csv')) + "_cleaned")

    clean_csv(old_csv, new_csv)