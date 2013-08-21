


#TODO: data_hora cadastro e update
#TODO: salvar no ElasticSearch

#TODO: listar itens para baixar com a data atual
#TODO: baixar e descompactar itens



import json
from datetime import datetime
import pyes, pprint


import os
from os import listdir
import urllib2
import json
from StringIO import StringIO
import zipfile



config = {
	"server": "http://127.0.0.1:9200"
}


impedimentos = {}

url_lista = 'http://arquivos.portaldatransparencia.gov.br/downloads.asp?c=%s'
url_download = "http://arquivos.portaldatransparencia.gov.br/downloads.asp?a=%(ano)s&m=%(mes)s&d=%(dia)s&consulta=%(tipo)s"


def check_transparencia(tipo):
	print "verificando dados de %s..." % tipo
	jsonp = urllib2.urlopen(url_lista % tipo).read()
	j = json.loads(jsonp[ jsonp.index("(")+1 : jsonp.rindex(")") ])
	last_date = j[-1]
	last_date["tipo"] = tipo
	last_date["filename"] = "%(tipo)s_%(ano)s%(mes)s%(dia)s" % last_date
	return last_date


def download_file(last):
	print "baixando arquivo %(filename)s..." % last
	zipdata = StringIO()
	zipdata.write(urllib2.urlopen(url_download % last).read())
	zipfolder = "../download/%(filename)s" % last
	with zipfile.ZipFile(zipdata) as myzip:
		myzip.extractall(zipfolder)
	return zipfolder


def save_file(filename, json_data):
	print "salvando arquivo json..."
	with open(filename, "w") as the_file:
		the_file.write(json.dumps(json_data, encoding="iso-8859-1"))
	print "file saved"





def save_elasticsearch():
	print 'Connecting to ES...'
	conn = pyes.ES(config["server"]) #, encoder="iso-8859-1")
	try:
		print 'Creating index...'
		conn.indices.create_index("impedimentos")
	except:
		pass

	print "ES encoder: "
	print conn.encoder
	print ""


	mapping = {
		"documento": { "type": "string", "analyzer" : "keyword"},
		"tipo_pessoa": { "type": "string", "analyzer" : "keyword"},
		#"nome": { "type": "string", "analyzer" : "keyword"},
		#"origem": { "type": "string", "analyzer" : "keyword"},
		#"data_fim" : { "type" : "string", "analyzer" : "keyword" },
		#"data_ini" : { "type" : "date", "format" : "dd/MM/YYYY" }
    }

	print 'Mapping...'
	conn.indices.put_mapping("impedimento", {'properties': mapping}, ["impedimentos"])
    
	erros = 0
	print 'Indexing!'
	for v in impedimentos:
		#print v
		p = impedimentos[v]
		conn.index(p, 'impedimentos', 'impedimento', p['documento'], bulk=True)
		#try:
		#	conn.index(p, 'monitor', 'registro', p['id'], bulk=True)
		#except:
		#	print "erro INDICE"
		#	erros = erros + 1

	print "%d erro(s)." % erros








def process_CEIS(force=False):
	print ">>> CEIS <<<"
	#TODO: verificar se ja existe o arquivo/processamento e zerar se FORCE=TRUE
	last = check_transparencia("CEIS")
	folder = download_file(last)
	file_to_process = "%s/%s" % (folder, listdir(folder)[0])
	print "processando CEIS..."
	with open(file_to_process, 'r') as raw:
		for a in raw.readlines()[1:]:
			#try:
				b = a.replace('\r\n', '').split('\t')
				impedimento = {}
				documento = b[7]
				#print documento
				if impedimentos.has_key(documento):
					impedimento = impedimentos[documento]
				else: #"sem impedimento"
					impedimento = {
						"documento": documento,
						"nome": b[8].decode("iso-8859-1"),
						"origem": "%s (%s)" % (b[2], b[3]),
						"tipo_pessoa": "fisica" if len(documento) == 11 else "juridica",
						"data_cadastro": "",
						"data_update": "",
						"raw":[]
					}
				impedimento["raw"].append(a.replace('\r\n', ''))
				impedimentos[documento] = impedimento
			#except:
		#	print "error on " + b
	return


def process_CEPIM(force=False):
	print ">>> CEPIM <<<"
	#TODO: verificar se ja existe o arquivo/processamento e zerar se FORCE=TRUE
	last = check_transparencia("CEPIM")
	folder = download_file(last)
	file_to_process = "%s/%s" % (folder, listdir(folder)[0])
	print "processando CEPIM..."
	with open(file_to_process, 'r') as raw:
		for a in raw.readlines()[1:]:
			#try:
				b = a.replace('\r\n', '').split('\t')
				impedimento = {}
				documento = b[0]
				if impedimentos.has_key(documento):
					impedimento = impedimentos[documento]
				else:
					impedimento = {
						"documento": documento,
						"nome": b[1].decode("iso-8859-1"),
						"origem": "%s" % (b[3]),
						"tipo_pessoa": "fisica" if len(documento) == 11 else "juridica",
						"data_cadastro": "",
						"data_update": "",
						"raw":[]
					}
				impedimento["raw"].append(a.replace('\r\n', ''))
				impedimentos[documento] = impedimento
			#except:
			#	print "error on " + b		
	return




process_CEIS()
process_CEPIM()

save_file("../raw/impedimentos.json", impedimentos)
save_elasticsearch()







#OK		TODO: verificar se existe a pasta
#OK		TODO: baixar e extrair o arquivo na pasta correspondente
#TODO: abrir o arquivo e importar para ElasticSearch










