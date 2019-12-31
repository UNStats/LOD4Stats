"""
Description:
    This script converts statistical data series given in .csv file to RDF file.

author: Antoni Kedzierski
date: 2:37 PM, 29-Nov-2019
"""

import json
import os, glob, re
import pandas as pd
import sys, time

# Global codes used widely in code
globalCodes = {
    # General ontology relations
    'type': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
    'version': 'http://schema.org/version',
    'comment': 'http://www.w3.org/2000/01/rdf-schema#comment',
    'label': 'http://www.w3.org/2000/01/rdf-schema#label',
    'units': 'http://metadata.un.org/sdg/codes/units',
    'unitMeasure': 'http://purl.org/linked-data/sdmx/2009/attribute#unitMeasure',
    'measureObsValue': 'http://purl.org/linked-data/sdmx/2009/measure#obsValue',
    'subPropertyOf': 'http://www.w3.org/2000/01/rdf-schema#subPropertyOf',
    'conceptScheme': 'http://www.w3.org/2004/02/skos/core#ConceptScheme',

    # Cube data structures
    'cubeDataSet': "http://purl.org/linked-data/cube#DataSet",
    'cubedataSet': "http://purl.org/linked-data/cube#dataSet",
    'cubeStructure': 'http://purl.org/linked-data/cube#structure',
    'cubeDataStructureDef': 'http://purl.org/linked-data/cube#DataStructureDefinition',
    'cubeslice': 'http://purl.org/linked-data/cube#slice',
    'cubeSlice': 'http://purl.org/linked-data/cube#Slice',
    'cubesliceKey': 'http://purl.org/linked-data/cube#sliceKey',
    'cubeSliceKey': 'http://purl.org/linked-data/cube#SliceKey',
    'cubeSliceStructure': 'http://purl.org/linked-data/cube#sliceStructure',
    'cubeComponent': 'http://purl.org/linked-data/cube#component',
    'cubeComponentSpec': 'http://purl.org/linked-data/cube#ComponentSpecification',
    'componentProperty': 'http://purl.org/linked-data/cube#componentProperty',
    'cubeMeasure': 'http://purl.org/linked-data/cube#measure',
    'cubeMeasureProperty': 'http://purl.org/linked-data/cube#MeasureProperty',
    'cubeDimmension': 'http://purl.org/linked-data/cube#dimension',
    'cubeComponentProperty': 'http://purl.org/linked-data/cube#componentProperty',
    'cubeobservation': 'http://purl.org/linked-data/cube#observation',
    'cubeObservation': 'http://purl.org/linked-data/cube#Observation',
    'cubeDimmensionProperty': 'http://purl.org/linked-data/cube#DimensionProperty',
    'cubeCodeList': 'http://purl.org/linked-data/cube#codeList'
}


def cleanLiteral(literal):
    literal = re.sub("\n", "", literal)
    literal = re.sub("\"", "'", literal)
    literal = re.sub(r'\\', "|", literal)
    return literal


# Converter class
class csvConverter:
    BASE_URI = "http://metadata.un.org/sdg/"
    TYPE_TRIPLE = '<%s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <%s> .\n'
    OBJECT_TRIPLE = '<%s> <%s> <%s> .\n'
    STRING_TRIPLE = '<%s> <%s> "%s" .\n'
    EN_STRING_TRIPLE = '<%s> <%s> "%s"@en .\n'
    YEAR_TRIPLE = '<%s> <%s> "%s"^^<http://www.w3.org/2001/XMLSchema#gYear> .\n'

    # Init converter object
    def __init__(self, path=os.getcwd()):
        # Prepare paths
        self.PATH = path
        self.RELEASE = '2019.Q2.G.01'
        self.RELEASE_PATH = self.PATH + '/' + self.RELEASE + '/'
        self.FILES = glob.glob(self.RELEASE_PATH + '*.csv')
        self.CODES_FILENAME = 'codes.json'

        # Missing codes
        self.missingCodes = {}
        self.missingCodesList = {}

        # If there are no files in a directory
        if len(self.FILES) == 0:
            print("There are no files in this directory!")
            self.__del__()

        # Load codes
        with open(self.PATH + '/' + self.CODES_FILENAME, encoding='utf-8') as codeFile:
            self.CODES = json.load(codeFile)
        codeFile.close()

    def __del__(self):
        print("Program has ended...")

    def getCodesURI(self, dimmension, code, desc):
        cleanDim = cleanLiteral(dimmension)
        cleanCode = cleanLiteral(code)

        # If dimmension does not exist, create empty one
        if cleanDim not in self.CODES:
            if cleanDim not in self.missingCodes:
                self.CODES[cleanDim] = {'uri': 'http://metadata.un.org/sdg/codes/' + dimmension.replace('Code', ''), 'label': 'missing...',
                                        'codes': {}, 'missing': True }

        # If a code does not exist in the dimmension, add it
        if cleanCode not in self.CODES[cleanDim]['codes']:
            codeID = self.CODES[dimmension]['uri'] + '/' + cleanCode
            self.CODES[cleanDim]['codes'][cleanCode] = {'uri': codeID, 'label': desc, 'missing': True }

        return self.CODES[cleanDim]['codes'][cleanCode]['uri']

    # Inform user which codes were missing
    def fixCodes(self):
        missingDims = []
        missingCodes = []

        for dim in self.CODES:
            if 'missing' in self.CODES[dim]:
                missingDims.append(dim)
                self.CODES[dim].pop('missing', None)
                print('Updating dimmension', dim + '...')
            for code in self.CODES[dim]['codes']:
                if 'missing' in self.CODES[dim]['codes'][code]:
                    missingCodes.append({'dim': dim, 'code': code})
                    self.CODES[dim]['codes'][code].pop('missing', None)
                    print('Updating code', code + '...')

        print()
        print('Missing dimmensions:', missingDims)
        print('Missing codes:', missingCodes)
        print('If you want to update your codes, replace "codes.json" with "updated-codes.json"...')

        if len(missingCodes) != 0 or len(missingDims) != 0:
            with open(self.PATH + '/' + 'codes-updated.json', 'w', encoding='utf-8') as codeFile:
                json.dump(self.CODES, codeFile)
            codeFile.close()

    # Convert selected file a part of RDF
    def convertFile(self, filePath):
        # Define result as string
        result = ''

        # Import csv file to pandas data fram
        csvData = pd.read_csv(filePath, delimiter='\t', dtype=str)

        # Specify series code and release package
        fileName = csvData['seriesCode'][0]
        release = csvData['release'][0]

        # Tell user which file is converting
        # print(fileName)

        # URI's used as subjects and objects
        document = self.BASE_URI + fileName
        dsd = document + '/dsd'
        dsdTimeSeries = dsd + '/timeSeries'
        seriesMeasure = document + '/measure'

        # Save all result to string line-by-line
        result += self.TYPE_TRIPLE % (document, globalCodes['cubeDataSet'])
        result += self.OBJECT_TRIPLE % (document, globalCodes['cubeStructure'], dsd)
        result += self.TYPE_TRIPLE % (dsd, globalCodes['cubeDataStructureDef'])
        result += self.STRING_TRIPLE % (document, globalCodes['version'], release)

        # Define units type
        unitCode = ''
        for i in range(len(csvData)):
            if csvData['unitsCode'][i] != '' and str(csvData['unitsCode'][i]) != 'nan':
                unitCode = csvData['unitsCode'][i]
                break

        result += self.OBJECT_TRIPLE % (document, globalCodes['units'], self.getCodesURI('unitsCode', unitCode, 'unitsDesc'))
        result += self.OBJECT_TRIPLE % (document, globalCodes['unitMeasure'], self.getCodesURI('unitsCode', unitCode, 'unitsDesc'))

        # Load series measure
        result += self.OBJECT_TRIPLE % (dsd, globalCodes['cubeMeasure'], seriesMeasure)
        result += self.OBJECT_TRIPLE % (dsd, globalCodes['cubeComponentProperty'], seriesMeasure)
        result += self.OBJECT_TRIPLE % (dsd, globalCodes['cubeMeasure'], globalCodes['measureObsValue'])
        result += self.OBJECT_TRIPLE % (dsd, globalCodes['cubeComponentProperty'], globalCodes['measureObsValue'])

        # Load dimmensions
        result += self.OBJECT_TRIPLE % (dsd, globalCodes['cubeDimmension'], self.CODES['yearCode']['uri'])
        result += self.OBJECT_TRIPLE % (dsd, globalCodes['cubeComponentProperty'], self.CODES['yearCode']['uri'])

        # Load missing components that will be used
        for key in csvData:
            if key in self.CODES and not key == 'unitsCode':
                result += self.OBJECT_TRIPLE % (dsd, globalCodes['cubeDimmension'], self.CODES[key]['uri'])
                result += self.OBJECT_TRIPLE % (dsd, globalCodes['cubeComponentProperty'], self.CODES[key]['uri'])
                result += self.OBJECT_TRIPLE % (dsdTimeSeries, globalCodes['cubeDimmension'], self.CODES[key]['uri'])

        result += self.OBJECT_TRIPLE % (dsd, globalCodes['cubesliceKey'], dsdTimeSeries)
        result += self.TYPE_TRIPLE % (dsdTimeSeries, globalCodes['cubeSliceKey'])
        result += self.EN_STRING_TRIPLE % (dsdTimeSeries, globalCodes['comment'],
                                           'Temporal slice of ' + release + ' (all years grouped together, while other dimensions fixed at specific values.)')

        result += self.STRING_TRIPLE % (document, globalCodes['label'], csvData['seriesDesc'][0])

        # Pass through all rows of csv file
        for i in range(csvData.__len__()):
            # If there are no values, pass
            allEmpty = True
            for yearCode in self.CODES['yearCode']['codes']:
                if yearCode not in csvData:
                    continue
                if csvData[yearCode][i] != '' and str(csvData[yearCode][i]) != 'nan':
                    allEmpty = False
                    break

            if allEmpty:
                continue

            # URI representing a slice
            documentSlice = document + '/' + str(i + 1)

            # Create a slice represanting a line of csv file
            result += self.OBJECT_TRIPLE % (document, globalCodes['cubeslice'], documentSlice)
            result += self.OBJECT_TRIPLE % (documentSlice, globalCodes['cubeSliceStructure'], dsdTimeSeries)
            result += self.TYPE_TRIPLE % (documentSlice, globalCodes['cubeSlice'])
            result += self.OBJECT_TRIPLE % (documentSlice, globalCodes['cubeSliceStructure'], dsdTimeSeries)

            for key in csvData:
                if key == 'unitsCode':
                    continue
                elif key == 'geoAreaCode':
                    result += self.OBJECT_TRIPLE % (
                    documentSlice, self.CODES[key]['uri'], self.getCodesURI(key, csvData[key][i], csvData['geoAreaName'][i]))
                elif key in self.CODES:
                    result += self.OBJECT_TRIPLE % (documentSlice, self.CODES[key]['uri'],
                                                    self.getCodesURI(key, csvData[key][i],
                                                                     csvData[key.replace("Code", "Desc")][i]))

            # Read each year value and append to a slice
            for yearCode in self.CODES['yearCode']['codes']:
                if not yearCode in csvData:
                    continue
                if str(csvData[yearCode][i]) == 'nan':
                    continue

                # Clean yearCode and create observation URI
                year = self.CODES['yearCode']['codes'][yearCode]
                observation = documentSlice + '/' + year

                # Save obserations to the result
                result += self.OBJECT_TRIPLE % (documentSlice, globalCodes['cubeobservation'], observation)
                result += self.TYPE_TRIPLE % (observation, globalCodes['cubeObservation'])
                result += self.OBJECT_TRIPLE % (observation, globalCodes['cubedataSet'], document)
                result += self.YEAR_TRIPLE % (observation, self.CODES['yearCode']['uri'], year)

                # Append units
                if csvData['unitsCode'][i] != '' and str(csvData['unitsCode'][i]) != 'nan':
                    result += self.OBJECT_TRIPLE % (observation, globalCodes['unitMeasure'],
                                                    self.getCodesURI('unitsCode', csvData['unitsCode'][i],
                                                                     csvData['unitsDesc'][i]))
                    result += self.OBJECT_TRIPLE % (observation, globalCodes['units'], self.getCodesURI('unitsCode', csvData['unitsCode'][i],
                                                                     csvData['unitsDesc'][i]))

                result += self.STRING_TRIPLE % (observation, globalCodes['measureObsValue'], csvData[yearCode][i])
                result += self.STRING_TRIPLE % (observation, seriesMeasure, csvData[yearCode][i])

        return result

    # Use this to convert all .csv files into large RDF
    def convertAll(self, n=-1, dump=True):
        if n < 0:
            n = len(self.FILES)

        result = ''
        for key in self.CODES:
            if key == 'unitsCode' or key == 'yearCode':
                continue
            # result += self.TYPE_TRIPLE % (self.CODES[key]['uri'], globalCodes['cubeDimmensionProperty'])
            result += self.STRING_TRIPLE % (self.CODES[key]['uri'], globalCodes['label'], self.CODES[key]['label'])
            result += self.OBJECT_TRIPLE % (self.CODES[key]['uri'], globalCodes['cubeCodeList'], self.CODES[key]['uri'] + 'Codes')

        result += self.OBJECT_TRIPLE % (self.CODES['unitsCode']['uri'], globalCodes['cubeCodeList'], self.CODES['unitsCode']['uri'])
        result += self.OBJECT_TRIPLE % (self.CODES['unitsCode']['uri'], globalCodes['subPropertyOf'], globalCodes['unitMeasure'])
        result += self.TYPE_TRIPLE % (globalCodes['measureObsValue'], globalCodes['cubeMeasureProperty'])
        result += self.STRING_TRIPLE % (globalCodes['measureObsValue'], globalCodes['label'], 'The value of the measured property at a particular period')

        # Progress bar
        barLen = 40
        if n < 40:
            barLen = n
        markerPos = 0
        self.FILES = self.FILES[:n]
        totalFiles = len(self.FILES)

        for i, file in enumerate(self.FILES):
            time.sleep(0.1)
            if markerPos * totalFiles / barLen <= i:
                markerPos += 1
            sys.stdout.write('\r[%s]  FILES DONE: %d/%d  FILE NAME: %s' % (
            '█' * markerPos + ' ' * (barLen - markerPos), i + 1, totalFiles, file.split('/')[-1]))
            sys.stdout.flush()

            # Here conversion takes place
            result += self.convertFile(file)

        # Tell user about missing codes, automatically add it to codes.json
        self.fixCodes()

        # Dump results to RDF
        if dump:
            with open('result.nt', 'w') as outfile:
                outfile.write(result)
            return '\nDone!'
        else:
            return result



if __name__ == '__main__':
    converter = csvConverter(path='/home/akedzierski/Dokumenty/SDG Data Series/sdg-series-data-cubes-converter')
    converter.convertAll()