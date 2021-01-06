import pandas as pd
import networkx as nx
from RDFtoGraph import retIDs
import matplotlib.pyplot as plt
from CSVtoRDFDF import retProdCatRDFs, returnUniversalRDF


def retMappedRelation(dataF):
    #Robimy LISTĘ! łączeń (0, 1), (1,2)
    relationList = []
    for index, row in dataF.iterrows():
        first = row[0]
        second = row[2]
        tuple = (first, second)
        relationList.append(tuple)
    return relationList


def showUniversalKawaii(df):
    # KAMADA KAWAII
    G = nx.MultiGraph()
    rels = retMappedRelation(df)
    G.add_edges_from(rels)
    pos = nx.kamada_kawai_layout(G)
    nx.draw_kamada_kawai(G, edge_color='grey')
    nx.draw_networkx_labels(G, pos)
    plt.show()


def showUniversalMultipartite(df):
    G = nx.MultiGraph()
    rels = retMappedRelation(df)
    nodes = retIDs(df)

    listOfNodes1 = []
    for x in nodes[0]: #predicates
        listOfNodes1.append(x)

    listOfNodes2 = []
    for x in nodes[1]: #objects
        listOfNodes2.append(x)

    G.add_nodes_from(listOfNodes1, layer=0)
    G.add_nodes_from(listOfNodes2, layer=1)
    print(listOfNodes2)

    G.add_edges_from(rels)
    pos = nx.multipartite_layout(G, subset_key="layer")
    nx.draw(G, pos, with_labels=True, edge_color='grey') #node_color=color,

    plt.show()


def showKawaiiProdCat(DFs):
    # KAMADA KAWAII
    G = nx.MultiGraph()

    # Trzeba zdropowac kolumienki z polem 'item' bo to psuje graf
    DFsWithoutItems = []
    for x in range(0, 7):
        df = DFs[x][DFs[x].Object != 'item']
        DFsWithoutItems.append(df)

    for x in range(0, 7):
        rels = retMappedRelation(DFsWithoutItems[x])
        G.add_edges_from(rels)

    pos = nx.kamada_kawai_layout(G)
    nx.draw_kamada_kawai(G, edge_color='r')

    rels = retMappedRelation(DFsWithoutItems[6])

    nx.draw_networkx_edges(G, pos, edgelist=rels, width=1, alpha=1, edge_color="g")
    nx.draw_networkx_labels(G, pos, font_size=6)

    plt.show()


def showMultipartiteProdCat(DFs):
    G = nx.MultiGraph()

    DFsWithoutItems = []
    for x in range(0, 7):
        df = DFs[x][DFs[x].Object != 'item']
        DFsWithoutItems.append(df)

    listOfNodes = []
    for x in range(0,6): #predicates
        temp = []
        for y1, y2 in DFs[x].iterrows():
            temp.append(y2[0])
        listOfNodes.append(temp)

    # Don't forget bout the products
    temp = []
    for y1, y2 in DFs[6].iterrows():
        temp.append(y2[2])
    listOfNodes.append(temp)

    print(listOfNodes)

    ## Not possible to add same node few times, so we can do it like this
    for it, x in enumerate(listOfNodes):
        G.add_nodes_from(x, layer=it)

    for x in range(0, 6):
        rels = retMappedRelation(DFsWithoutItems[x])
        G.add_edges_from(rels)

    pos = nx.multipartite_layout(G, subset_key="layer")

    nx.draw(G, pos, with_labels=True, width=0.5, font_size=5, edge_color='r')

    #Relation also here so i could colour them
    rels = retMappedRelation(DFsWithoutItems[6])
    nx.draw_networkx_edges(G, pos, edgelist=rels, width=0.5, alpha=0.5, edge_color="g",)
    plt.show()


if __name__ == '__main__':
    dataF = returnUniversalRDF('product_id', 'has age', 'product_age_group', 'Chunks/CSD_6E2A0A83CA3B44030C176D1439120336.csv')
    # showUniversalKawaii(dataF)
    # showUniversalMultipartite(dataF)

    dfcatprod = retProdCatRDFs('Chunks/CSD_6E2A0A83CA3B44030C176D1439120336.csv')
    # showKawaiiProdCat(dfcatprod)
    showMultipartiteProdCat(dfcatprod)
