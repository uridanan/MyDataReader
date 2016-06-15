import csv
import six


###################################################################################################
#CSV Export Methods

#Implicitely extract all columns directly from the map when exporting
def export2csv(filename, map):
    #Extract column names
    firstRow = getfirstentry(map)
    columns = firstRow.keys()
    #Call the explicit method
    exportcolumns2csv(filename, map, columns)

#Explicitely specify the columns you want to export and in what order
def exportcolumns2csv(filename, map, columns):
    return exportcolumns2csvheaders(filename, map, columns, columns)


#Explicitely specify and rename the columns you want to export and in what order
def exportcolumns2csvheaders(filename, map, columns, rename):
    print ("Open output file")
    f = open(filename, 'wt')
    try:
        #Write headers
        coma = ','
        headers = coma.join(rename)
        writeln(f,headers)

        #Write data
        rows = map.itervalues()
        for row in rows:
            values = list()
            for c in columns:
                value = ""
                if isinstance(row[c], six.string_types):
                    value = row[c].encode('UTF8')
                else:
                    value = str(row[c]).encode('UTF8')
                values.append(doublequote(value))
            line = coma.join(values)
            writeln(f,line)

    except Exception as err:
        print(err)
    finally:
        f.close()
        print ("Close output file")



#Support Methods
def writeln(file, string):
    file.write(string)
    file.write('\n')

def getfirstentry(map):
    for entry in map.itervalues():
        return entry

def wrap(char, string):
    return char + string + char

def doublequote(string):
    return wrap('"', string)


###################################################################################################

###################################################################################################
#CSV Import Methods

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


#Support Methods

def extractheaders(reader):
    for row in reader:
        return row

def getCompositeKey(keys, values):
    key = ""
    for i in keys:
        key = key + values[int(i)]
    return key

###################################################################################################

###################################################################################################
#Testing

# def main():
#     importfromcsv('in.csv', ['0','First','Last'], [0,1])
#     importcsv('in.csv', [0, 1])
#
# main()

###################################################################################################