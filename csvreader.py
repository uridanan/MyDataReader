import csv

def importcsv(filename, keyindexlist):
    # Open csv file with dict reader
    with open(filename) as csvfile:
        reader = csv.reader(csvfile)
        try:
            #Exract headers
            headers = extractheaders(reader)
            #Import data to map
            map = csvreader2map(reader, headers, keyindexlist)
        except Exception as err:
            print(err)
        finally:
            print("Import CSV Complete: " + filename)
            csvfile.close()
            return map


#Import specific columns from csv from file
def importfromcsv(filename, headers, keyindexlist):
    # Open csv file with dict reader
    with open(filename) as csvfile:
        reader = csv.reader(csvfile, headers)

        try:
            map = csvreader2map(reader, headers, keyindexlist)

        except Exception as err:
            print(err)
        finally:
            print("Import from CSV Complete: " + filename)
            csvfile.close()
            return map



def csvreader2map(reader, headers, keyindexlist):
    #Create empty two dimensional dictionary
    map = dict()
    for h in headers:
        map[h] = dict()

    #Fill the dict
    for row in reader:
        key = getCompositeKey(keyindexlist, row)
        count = 0
        for value in row:
            map[headers[count]][key] = value
            count += 1

    return map


def extractheaders(reader):
    for row in reader:
        return row

def getCompositeKey(keys, values):
    key = ""
    for i in keys:
        key = key + values[int(i)]
    return key

def main():
    importfromcsv('in.csv', ['0','First','Last'], [0,1])
    importcsv('in.csv', [0, 1])

main()