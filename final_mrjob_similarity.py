#-------------------------------------------------------------------------------
# Name:        module2
# Purpose:
#
# Author:      alex
#
# Created:     28/05/2018
# Copyright:   (c) alex 2018
# Licence:     <your licence>
#-------------------------------------------------------------------------------

# Similarity Score mapreduce code

class MRSimilarityScores(MRJob):
    '''
    Class for MapReduce work.
    '''

    def configure_options(self):
        super(MRSimilarityScores, self).configure_options()
        self.add_file_option('--businesses', help='path to master.csv')

    def calculate_haversine_distance(lon1, lat1, lon2, lat2):
        """
        Calculate the haversine distance in miles between two businesses
        """
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        r = 3956 # radius of earth in miles
        return c * r

    def format_hours(input_hours):

        hours = []
        for i in input_hours:
            if i:
                start, end = i.split("-")
                start_hrs_mins = re.match("([0-9]*):?([0-9]*)", start)
                start_frac = int(start_hrs_mins.group(2)) / 60
                end_hrs_mins = re.match("([0-9]*):?([0-9]*)", end)
                end_frac = int(end_hrs_mins.group(2)) / 60
                fixed_start = int(start_hrs_mins.group(1)) + start_frac
                fixed_end = int(end_hrs_mins.group(1)) + end_frac
                hours.append((fixed_start, fixed_end))
            else:
                hours.append((0,0))
        return hours

    def hours_overlap(hours1, hours2):

        h1 = format_hours(hours1)
        h2 = format_hours(hours2)

        overlap = 0
        h1_hours = 0
        for day in range(7):
            h1_hours += h1[day][1] - h1[day][0]
            both_open = min(h1[day][1] - h2[day][0], h2[day][1] - h1[day][0])
            if both_open > 0:
                overlap += both_open
        return overlap / h1_hours

    def mapper(self, _, line):
        '''
        Mapper function. Takes in a row from the csv file and pairs it
        with all other business in the file to calculate a similarity score with
        all other local businesses

        Inputs:
            self: an instance of the MRSuccessScores class
            _: a dummy placeholder for the key of the pair
            line (str): a row from the csv file

        Yield:
            A key value pair of the person's name and their status

        '''
        bus1 = next(csv.reader([line]))
        # first file to be processed must be "main" file (i.e. not a file in add_file_option())
        business_id1 = bus1[0]
        lat1 = bus1[7]
        lon1 = bus1[8]
        stars1 = bus1[9]
        rev_count1 = bus1[10]
        categories1 = bus1[12]
        hours1 = bus1[13:20]


        total_score = 0
        with open('master.csv') as f:
            reader = csv.reader(f)
            for bus2 in reader:
                business_id2 = bus2[0]
                lat2 = bus2[7]
                lon2 = bus2[8]
                stars2 = bus2[9]
                rev_count2 = bus2[10]
                categories2 = bus2[12]
                hours2 = bus2[13:20]

                distance = calculate_haversine_distance(lon1, lat1, long2, lat2)

                if distance < 50 and business_id1 != business_id2:
                    hours_overlap = hours_overlap(hours1, hours2)
                    review_count_sim = .3 * (((rev_count1 + rev_count2) - abs(rev_count1 - rev_count2)) / (rev_count1 + rev_count2)) + .7
                    category_sim = .5 * len(set(categories1).intersection(set(categories2))) / min(len(categories1), len(categories2)) + .5
                    score = (5 - distance / 10) * ((5 - abs(stars1 - stars2)) / 5) * hours_overlap * review_count_sim
                    total_score += score

        yield business_id1, total_score


if __name__ == '__main__':
    MRSuccessScores.run()
