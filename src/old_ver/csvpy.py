import csv

with open('Westin.mdb.FULL.dlog1.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        print(row['V1'], row['V2'])
