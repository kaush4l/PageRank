from pyparsing import Regex, re
from pyspark import SparkContext,SparkConf
import sys

conf = SparkConf().setAppName("cloud3")
sc = SparkContext(conf=conf)

len=0.0
#function definition for isolating titles
def link(kk):
        if not kk:
                return
        else:
                title = re.search('\<title\>(.*?)\<\/title\>',kk).group(1)
                rest = re.findall('\[\[(.*?)\]\]',kk)
                if not rest:
                        rest = []
                return (title,(1.0/len,rest))
#function to refine rank and external links
def combine(x,y):
        rank = x[0]+y[0]
        plink = []
        if x[1]:
                plink = x[1]
        if y[1]:
                plink = y[1]
        return (rank,plink)
#function to append link and rank to each node
def calRank(entry):
        titlenode = entry[0]
        rank = (entry[1])[0]
        plink = (entry[1])[1]
        str = []
        str.append((titlenode,(0.0,plink)))
        if not plink:
                return str
        rankToDist = rank/len(plink)
        for outlink in plink:
                str.append((outlink,(rankToDist,"")))
        return str

inputFile = str(sys.argv[1])
outputDir = str(sys.argv[2])
#code for importing the input
lines = sc.textFile(inputFile)
lines = lines.filter(lambda x: x!="")
print lines.count()
#code to count total number of nodes with links
linknode = lines.flatMap(lambda x: re.findall('\<title\>(.*?)\<\/title\>',x))
print linknode.count()

d1 = lines.flatMap(lambda x: re.findall('\[\[(.*?)\]\]',x)).distinct()
fullLink = linknode.union(d1).distinct()
len = lines.count()
print len
# evaluate rank of each node
ulist = lines.map(lambda x: link(x))
for i in range(0, 10):
        lirank = ulist.flatMap(lambda x : calRank(x))
        ulist = lirank.reduceByKey(lambda x,y : combine(x,y))
        ulist = ulist.map(lambda x: (x[0],(x[1][0]*.85+.15 ,x[1][1])))
print ulist.count()

sc.parallelize(ulist.coalesce(1).takeOrdered(100, key = lambda x: -x[1][0])).coalesce(1).map(lambda x: x[0]+str("   ")+str(x[1][0])).saveAsTextFile(outputDir)

