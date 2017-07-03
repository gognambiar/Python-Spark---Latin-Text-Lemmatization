import pyspark
import re

def fdigram(line):
	line2 = line.split("> ",1)
	line3 = line2[1]
	line3 = line3.lower()
	line3 = line3.replace('j','i')
	line3 = line3.replace('v','u')
	line1 = line3.split(" ")
	line1 = [re.sub('[^A-Za-z0-9]', '', x) for x in line1]
	line1 = [x for x in line1 if x != '']
	line2[0] = line2[0] + ">"
	el = []
	sl = []
	for i in range(len(line1)-2):
		for j in range(i+1,len(line1)-1):
			for k in range(j+1,len(line1)):
				el.append(line1[i] + "|"+ line1[j] + "|"+ line1[k])
		

	for ell in el:
		ell1 = ell.split("|")
		if(lemma.get(ell1[0]) != None):
			ell2 = lemma.get(ell1[0])
		else:
			ell2 = [ell1[0]]

		if(lemma.get(ell1[1]) != None):
			ell3 = lemma.get(ell1[1])
		else:
			ell3 = [ell1[1]]

		if(lemma.get(ell1[2]) != None):		
			ell4 = lemma.get(ell1[2])
		else:
			ell4 = [ell1[2]]

	
		for i in range(len(ell2)):
			for j in range(len(ell3)):
				for k in range(len(ell4)):
					sl.append([str(ell2[i]) + "|" + str(ell3[j]) + "|" + str(ell4[k]),str(line2[0])])
	return sl



sc = pyspark.SparkContext()

fl = open("new_lemmatizer.csv","r")
lemma = {}

for line in fl:
	c = line.strip().split(",")
	ct = 1
	p = []
	while(ct<(len(c))):
		if(c[ct] != ""):
			p.append(c[ct])
		ct += 1
	lemma[c[0]] = p	


digram1 = sc.textFile("latin").flatMap(lambda line: line.split("\n"))

digram2 = digram1.filter(lambda line: len(line.split(">")) != 1)
digram = digram2.flatMap(fdigram).reduceByKey(lambda v1,v2: v1+v2)

digram.saveAsTextFile("output")
