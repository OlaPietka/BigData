import csv


def parse_csv(filename):
    """
    Parse csv file
    Args:
        filename: Filename of csv

    Returns:
        None
    """
    with open('{}.csv'.format(filename), newline='', encoding="utf-8-sig") as csv_in:
        reader = csv.reader(csv_in, delimiter=',')
        with open('parse_{}.csv'.format(filename), 'w', newline='', encoding="utf-8-sig") as csv_out:
            writer = csv.writer(csv_out, delimiter=',')
            writer.writerow(['Osoba', 'Tytuł', 'Ocena'])

            for row in reader:
                print(row)
                for i in range(1, len(row[1:])+1, 2):
                    writer.writerow([row[0], row[i].lower(), int(row[i+1])])
