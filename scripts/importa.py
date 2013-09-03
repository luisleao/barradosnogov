

#OK		TODO: data_hora cadastro e update
#TODO: criar indice separado para CEIS e CEPIM




import json
from datetime import datetime
import pyes, pprint


import os
from os import listdir
import urllib2
import json
from StringIO import StringIO
import zipfile

import codecs



#TODO: criar dois indices independentes
#TODO: fazer a busca somando os indices







config = {
	#"server": "http://127.0.0.1:9200"
	"server": "http://apps.thacker.com.br:9200"
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
	zipfolder = "../download/%(filename)s" % last
	zipdata = StringIO()
	zipdata.write(urllib2.urlopen(url_download % last).read())
	with zipfile.ZipFile(zipdata) as myzip:
		myzip.extractall(zipfolder)
	return zipfolder


def decode_file(filename):
	print "decoding %s..." % filename
	decoded_file = "%s.decoded" % filename
	with codecs.open(filename, 'r', encoding='iso-8859-1') as infile:
		with codecs.open(decoded_file, 'w', encoding='utf-8') as outfile:
			for line in infile:
				outfile.write(line)
	return decoded_file


def save_file(filename, json_data):
	print "salvando arquivo json..."
	with open(filename, "w") as the_file:
		the_file.write(json.dumps(json_data, encoding="utf-8")) #iso-8859-1"))
	print "file saved"





def save_elasticsearch():
	print 'Connecting to ES...'
	conn = pyes.ES(config["server"])

	conn.delete_index_if_exists("ceis")
	conn.delete_index_if_exists("cepim")
	conn.refresh()

	#try:
	#	#conn.indices.create_index("impedimentos")
	#except:
	#	pass

	print 'Creating index...'
	conn.indices.create_index("ceis")
	conn.indices.create_index("cepim")


	#TODO: separar mapping de cada um dos tipos (Pedro vai mandar codigo que esta usando no Dicionario)
	mapping_CEIS = {
		"documento": { "type": "string", "analyzer" : "keyword"},
		"tipo_pessoa": { "type": "string", "analyzer" : "keyword"},
		"nome": { 
			"type": "multi_field", 
			"fields": {
				"nome": { "type": "string", "index": "analyzed"},
				"raw": { "type": "string", "index": "not_analyzed"}
			}
		}

		#"nome": { "type": "string", "analyzer" : "keyword"},
		#"origem": { "type": "string", "analyzer" : "keyword"},
		#"data_fim" : { "type" : "string", "analyzer" : "keyword" },
		#"data_ini" : { "type" : "date", "format" : "dd/MM/YYYY" }
    }

	mapping_CEPIM = {
		"documento": { "type": "string", "analyzer" : "keyword"},
		"tipo_pessoa": { "type": "string", "analyzer" : "keyword"},
		"nome": { 
			"type": "multi_field", 
			"fields": {
				"nome": { "type": "string", "index": "analyzed"},
				"raw": { "type": "string", "index": "not_analyzed"}
			}
		}

		#"nome": { "type": "string", "analyzer" : "keyword"},
		#"origem": { "type": "string", "analyzer" : "keyword"},
		#"data_fim" : { "type" : "string", "analyzer" : "keyword" },
		#"data_ini" : { "type" : "date", "format" : "dd/MM/YYYY" }
    }

	print 'Mapping...'
	#conn.indices.put_mapping("CEIS", {'properties': mapping}, ["impedimentos"])
	#conn.indices.put_mapping("CEPIM", {'properties': mapping}, ["impedimentos"])
	conn.indices.put_mapping("ceis", {'properties': mapping_CEIS}, ["ceis"])
	conn.indices.put_mapping("cepim", {'properties': mapping_CEPIM}, ["cepim"])
    
	erros = 0
	print 'Indexing!'
	for v in impedimentos:
		p = impedimentos[v]
		conn.index(p, p['tabela'].lower(), "impedimento", p['documento'], bulk=True)
		#conn.index(p, 'impedimentos', p['tabela'], p['documento'], bulk=True)

	conn.refresh()

	print "%d erro(s)." % erros





def process_CEIS(force=False):
	print ">>> CEIS <<<"
	#TODO: verificar se ja existe o arquivo/processamento e zerar se FORCE=TRUE
	last = check_transparencia("CEIS")

	folder = "../download/%(filename)s" % last
	if not os.path.exists(folder) or force:
		folder = download_file(last)

	file_to_process = "%s/%s" % (folder, listdir(folder)[0])
	decoded_file = decode_file(file_to_process)

	print "processando CEIS..."
	with codecs.open(file_to_process, 'r', encoding='iso-8859-1') as raw:	
		#with open(decoded_file, 'r') as raw:
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
						"nome": b[8], #b[8].decode("iso-8859-1"),
						"origem": "%s (%s)" % (b[2], b[3]),
						"tipo_pessoa": "fisica" if len(documento) == 11 else "juridica",
						"data_cadastro": "",
						"data_update": "",
						"raw":[],
						"tabela": "CEIS",
						"bloqueios":[],
					}
				#impedimento["raw"].append(a.decode("iso-8859-1").replace('\r\n', ''))
				impedimento["raw"].append(a.replace('\r\n', ''))
				impedimentos[documento] = impedimento
			#except:
		#	print "error on " + b
	return


def process_CEPIM(force=False):
	print ">>> CEPIM <<<"
	#TODO: verificar se ja existe o arquivo/processamento e zerar se FORCE=TRUE
	last = check_transparencia("CEPIM")

	folder = "../download/%(filename)s" % last
	if not os.path.exists(folder) or force:
		folder = download_file(last)

	#if os.path.exists(folder) and not force:
	#	print "arquivo ja existe"
	#	return

	file_to_process = "%s/%s" % (folder, listdir(folder)[0])
	decoded_file = decode_file(file_to_process)
	print "processando CEPIM..."
	with codecs.open(file_to_process, 'r', encoding='iso-8859-1') as raw:	
		#with open(decoded_file, 'r') as raw:
		for a in raw.readlines()[1:]:
			#try:
				b = a.replace('\r\n', '').split('\t')
				impedimento = {}
				documento = b[0]
				if impedimentos.has_key(documento):
					print "tem chave"
					impedimento = impedimentos[documento]
				else:
					print "novo"
					impedimento = {
						"documento": documento,
						"nome": b[1], #.decode("iso-8859-1").encode('utf8'),
						"origem": "%s" % (b[3]),
						"tipo_pessoa": "fisica" if len(documento) == 11 else "juridica",
						"data_cadastro": "",
						"data_update": "",
						"raw":[],
						"tabela": "CEPIM",
						"bloqueios":[],
					}

				impedimento["raw"].append(a.replace('\r\n', ''))
				impedimento["bloqueios"].append({"convenio": b[2], "info": b[4]})

				#impedimento["raw"].append(a.decode("iso-8859-1").encode('utf8').replace('\r\n', ''))
				impedimentos[documento] = impedimento
			#except:
			#	print "error on " + b		
	return




process_CEIS()
process_CEPIM()

print "total de impedimentos: %s." % len(impedimentos)

save_file("../raw/impedimentos.json", impedimentos)

save_elasticsearch()


