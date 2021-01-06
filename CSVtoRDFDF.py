import pandas as pd
import csv
path = 'Chunks/'
csvDataFileName = 'CSD_0225D81C97298F6FD63C8868EE8E2228.csv'
# Fajnie wyglądający datasecik
# csvDataFileName = 'CSD_9D9E93D1D461D7BAE47FB67EC0E01B62.csv'
wholePath = path + csvDataFileName


#Function used to make RDFs out of only single relation
def EasyUniversalGraphParser(stringPredCol, relName, stringObjCol, csvIn=wholePath):
    stringPred = '<' + stringPredCol + '> '
    stringObj = '<' + stringObjCol + '> '
    listtriples = []

    with open(csvIn, newline='\n') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        for row in reader:
            # Jeżeli wartosc wiersza kolumny nie równa sie -1 to mozna wyciagnac sensowne dane
            if row[stringObjCol] != "-1" and row[stringPredCol] != "-1":
                temp = [stringPred + row[stringPredCol], relName, stringObj + row[stringObjCol]]
                listtriples.append(temp)
    return listtriples


def returnUniversalRDF(StringPred, StringRel, StringObj, csvIn=wholePath):
    listtriples = EasyUniversalGraphParser(StringPred, StringRel, StringObj, csvIn)
    df = pd.DataFrame(listtriples, columns=[StringPred, StringRel, StringObj])
    df.drop_duplicates(inplace=True)  # Drop duplicates
    return df


# Funkcja robocza na bazie której powstał EasyUniversal...
# def hasAgeDF():
#     stringProdID = '<product_id> '
#     stringAge = '<product_age_group> '
#     listtriplesHasAge = []
#     with open(wholePath, newline='\n') as csvfile:
#         reader = csv.reader(csvfile, delimiter=',')
#         for row in reader:
#             #Jeżeli wartosc wiersza kolumny product_age_group nie równa sie -1 to mozna wyciagnac sensowne dane
#             if row[6] != "-1":
#                 temp = [stringProdID+row[19], 'has_age', stringAge+row[6]]
#                 listtriplesHasAge.append(temp)
#     return listtriplesHasAge


def retprodCatDFs(csvIn):
    stringProdID = '<product_id> '
    stringProdCat = ['<prod_cat1> ', '<prod_cat_2> ', '<prod_cat_3> ', '<prod_cat_4> ', '<prod_cat_5> ', '<prod_cat_6> ',
                     '<prod_cat_7> ']

    listtriplesProdCat1 = [] #Triple prod_cat1 has_subcat prod_cat2
    listtriplesProdCat2 = [] #Triple prod_cat2 has_subcat prod_cat3
    listtriplesProdCat3 = []
    listtriplesProdCat4 = []
    listtriplesProdCat5 = []
    listtriplesProdCat6 = [] #Triple prod_cat6 has_subcat prod_cat7
    listtriplesProdCat = [listtriplesProdCat1, listtriplesProdCat2, listtriplesProdCat3, listtriplesProdCat4,
                          listtriplesProdCat5, listtriplesProdCat6]

    listCathasProduct = [] #Another list for triples prod_catx has_product prod_id


    with open(csvIn, newline='\n') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            # colNum = 11 #Column with product category 1
            for x in range(11, 17):
                if row[x] != '-1':  # col
                    if row[x+1] != '-1':  # next col
                        temp = [stringProdCat[x-11]+row[x], 'cat has subcat', stringProdCat[x-10]+row[x+1]]
                        listtriplesProdCat[x-11].append(temp)
                    else:
                        temp = [stringProdCat[x-11]+row[x], 'cat has subcat', 'item']
                        listtriplesProdCat[x-11].append(temp)
                        temp = [stringProdCat[x-11] + row[x], 'cat has product', stringProdID + row[19]]
                        listCathasProduct.append(temp)
    return listtriplesProdCat, listCathasProduct


def retProdCatRDFs(csvIn):
    rdfProdCat = retprodCatDFs(csvIn)
    DFs = []

    for x in range(6):
        rdfProdCat[0][x] = rdfProdCat[0][x][1:]
        df = pd.DataFrame(rdfProdCat[0][x], columns=['Subject', 'Relation', 'Object'])
        df.drop_duplicates(inplace=True)  #Drop duplicates
        DFs.append(df)

    catItem = pd.DataFrame(rdfProdCat[1], columns=['Subject', 'Relation', 'Object'])
    catItem.drop_duplicates(inplace=True)
    DFs.append(catItem) #Let's not forget about cat->item this time

    return DFs


if __name__ == '__main__':
    xd = returnUniversalRDF('product_id', 'has age', 'product_age_group', 'Chunks/CSD_9D9E93D1D461D7BAE47FB67EC0E01B62.csv')
    dd = retProdCatRDFs('Chunks/CSD_9D9E93D1D461D7BAE47FB67EC0E01B62.csv')
    print(xd)
