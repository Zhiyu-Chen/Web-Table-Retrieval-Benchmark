import os
'''
generating WDC dump file list
'''

base_url = "http://data.dws.informatik.uni-mannheim.de/webtables/2015-07/englishCorpus/compressed/"
f = open("file_list.txt",'w')
for i in range(1,51):
	if i < 10:
		f_no = "0" + str(i)
	else:
		f_no = str(i)
	url = "http://data.dws.informatik.uni-mannheim.de/webtables/2015-07/englishCorpus/compressed/{}.tar.gz".format(f_no)
	fname = f_no+".tar.gz"
	f.write(url + '\n')


f.close()

