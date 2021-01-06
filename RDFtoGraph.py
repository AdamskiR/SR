import pandas as pd
import dgl
from CSVtoRDFDF import retProdCatRDFs, returnUniversalRDF


def retIDs(dataF):
    ## assign ids to all objects
    predicates = {}
    objects = {}
    predcnt = 0
    objcnt = 0

    for index, row in dataF.iterrows():
        if row[0] not in predicates:
            predicates[row[0]] = predcnt
            predcnt = predcnt + 1

        if row[2] not in objects:
            objects[row[2]] = objcnt
            objcnt = objcnt + 1

    return predicates, objects


def retIDsOneColumnATaTime(dataF, COLofDF):
    ## assign ids to all objects
    objects = {}
    objcnt = 0

    for index, row in dataF.iterrows():
        if row[COLofDF] not in objects:
            if row[COLofDF] != 'item':
                objects[row[COLofDF]] = objcnt
                objcnt = objcnt + 1

    return objects


def retMappedRelation(df, dictsIDs):
    #Creating list of relations for ex. [(0,1), (1,2) ... ]
    relationList = []
    for index, row in df.iterrows():
        first = dictsIDs[0][row[0]]
        second = dictsIDs[1][row[2]]
        tuple = (first, second)
        relationList.append(tuple)
    return relationList


def retMappedRelationCategories(dataF, dictsIDs):
    #Robimy LISTĘ! łączeń (0, 1), (1,2)
    relationList = []
    for index, row in dataF.iterrows():
        if row[2] != 'item':
            first = dictsIDs[row[0]]
            second = dictsIDs[row[2]]
            tuple = (first, second)
            relationList.append(tuple)
    return relationList


def retUniversalHeterograph(df):
    tupleRelations = tuple(df.columns.values)
    print(tupleRelations)
    dictIDs = retIDs(df)
    mappedRelations = retMappedRelation(df, dictIDs)

    dataDict = {
        tupleRelations: mappedRelations
    }
    g = dgl.heterograph(dataDict)
    return g


def retHeterographProdCat(df):
    dictIDsMASTER = {}

    #Umiesczanie wszytskich ID w słowniku
    for x in range(0,6):
        IDs = retIDsOneColumnATaTime(df[x], 0)
        dictIDsMASTER.update(IDs)
    dictIDsMASTER.update(retIDsOneColumnATaTime(df[6], 2)) # Also IDs

    #Problme polega na tym, że gdy parsuje podwójnia dane z csv to jedna kolumna sie wysrywa w momencie gdy w nbastepnej nie ma wartosci
    #Zrobic oddzielnie parsowanie samych kol i podkol i potem osttamnia kolumna -> item

    mappedRels = []
    for x in range(0, 7):
        mappedRels.append(retMappedRelationCategories(df[x], dictIDsMASTER))

    #Funkcja mapujące relacje prod_catx -> productID
    dict2 = {}
    prodCatsDone = []
    for x, y in df[6].iterrows():
        catName = y[0][:12]
        if catName in prodCatsDone: #Juz prodcat ktorystam caly zrobiony wiec mozna pominac i szukac nastepnego
            continue
        relation = (catName, 'has_product', 'prod_id')
        ListOfSameCats = []
        for x2, y2 in df[6].iterrows():
            if y2[0][:12] == catName:
                IDofCat = dictIDsMASTER[y2[0]]
                IDofProd = dictIDsMASTER[y2[2]]
                ListOfSameCats.append((IDofCat, IDofProd))
        d = {relation: ListOfSameCats}
        dict2.update(d)
        prodCatsDone.append(catName)

    dataDict = {
        ('<prod_cat_1>', 'has_category12', '<prod_cat_2>'): mappedRels[0],
        ('<prod_cat_2>', 'has_category23', '<prod_cat_3>'): mappedRels[1],
        ('<prod_cat_3>', 'has_category34', '<prod_cat_4>'): mappedRels[2],
        ('<prod_cat_4>', 'has_category45', '<prod_cat_5>'): mappedRels[3],
        ('<prod_cat_5>', 'has_category56', '<prod_cat_6>'): mappedRels[4],
        ('<prod_cat_6>', 'has_category67', '<prod_cat_7>'): mappedRels[5]
    }

    dataDict.update(dict2)

    g = dgl.heterograph(dataDict)
    return g

if __name__ == '__main__':
    # xDf = returnUniversalRDF('product_id', 'has age', 'product_age_group', 'Chunks/CSD_9D9E93D1D461D7BAE47FB67EC0E01B62.csv')
    # print(xDf)
    # print(retUniversalHeterograph(xDf))

    df = retProdCatRDFs('Chunks/CSD_9D9E93D1D461D7BAE47FB67EC0E01B62.csv')
    gProdCat = retHeterographProdCat(df)
    print(gProdCat)
