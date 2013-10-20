#!/usr/bin/python
# -*- coding: utf-8 -*- 


#OK		TODO: data_hora cadastro e update
#TODO: criar indice separado para CEIS e CEPIM


import sys

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







CONFIG = {
	"homolog": "http://127.0.0.1:9200",
	"prod": "http://apps.thacker.com.br:9200",
	"current": "prod"
	#"server": "http://apps.thacker.com.br:9200"
}


impedimentos_ceis = {}
impedimentos_cepim = {}


url_lista = 'http://arquivos.portaldatransparencia.gov.br/downloads.asp?c=%s'
url_download = "http://arquivos.portaldatransparencia.gov.br/downloads.asp?a=%(ano)s&m=%(mes)s&d=%(dia)s&consulta=%(tipo)s"




def timefix(t):
	try:
		if len(t)==0:
			return None
		#very dirty!
		r = datetime.datetime.strptime(t[1:].replace("/",""), "%d%m%Y")
		r = r.strftime("%d/%m/%Y")
	except:
		if t[0] == 'i' and t[4:6] == '00':
			t = t[0:4] + '01' + t[6:11]
		if t[0] == 'f' and t[4:6] == '00':
			t = t[0:4] + '12' + t[6:11]
		if t[0] == 'i' and t[1:3] == '00':
			t = t[0] + '01' + t[3:11]
		if t[0] == 'f' and t[1:3] == '00':
			if int(t[4:6]) in [1,3,5,7,9,11]:
				t = t[0] + '31' + t[3:11]
			if int(t[4:6]) == 2:
				t = t[0] + '28' + t[3:11]
			if int(t[4:6]) in [4,6,8,10,12]:
				t = t[0] + '30' + t[3:11]
		if t[4:6] == '02' and int(t[1:3]) > 28:
			t = t[0] + '28' + t[3:11] 
		if t[0] == 'f' and t[1:] == '':
			return None
		if t[0] == 'i' and t[1:] == '':
			return None
		r = t[1:]
	return r

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
	conn = pyes.ES(CONFIG[CONFIG["current"]])

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
				"nome": { "type": "string"},
				"untouched": { "type": "string", "index": "not_analyzed", "include_in_all": False}
			}
		}
    }


	mapping_CEIS = {
		"documento": { "type": "string", "analyzer": "keyword"},
		"tipo_pessoa": { "type": "string", "analyzer": "keyword"},
		"nome": { 
			"type": "multi_field", 
			"fields": {
				"nome": { "type": "string", "index": "analyzed"},
				"untouched": { "type": "string", "index": "not_analyzed"}
			}
		}
    }

	mapping_CEPIM = {
		"documento": { "type": "string", "analyzer" : "keyword"},
		"tipo_pessoa": { "type": "string", "analyzer" : "keyword"},
		"nome": { 
			"type": "multi_field", 
			"fields": {
				"nome": { "type": "string"},
				"untouched": { "type": "string", "index": "not_analyzed", "include_in_all": False}
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
	conn.refresh()

	erros = 0
	print 'Indexing!'
	for v in impedimentos_ceis:
		p = impedimentos_ceis[v]
		conn.index(p, p['tabela'].lower(), "ceis", p['documento'], bulk=True)

	for v in impedimentos_cepim:
		p = impedimentos_cepim[v]
		conn.index(p, p['tabela'].lower(), "cepim", p['documento'], bulk=True)
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
				if impedimentos_ceis.has_key(documento):
					impedimento = impedimentos_ceis[documento]
				else: #"sem impedimento"
					
					impedimento = {
						"documento": documento,
						"nome": b[8].upper(), #b[8].decode("iso-8859-1"),
						#"origem": "%s (%s)" % (b[2], b[3]),
						"tipo_pessoa": "fisica" if len(documento) == 11 else "juridica",
						"data_cadastro": datetime.now().strftime("%d/%m/%Y"),
						"arquivo": file_to_process,
						"dados_ceis": [],
						"raw":[],
						"tabela": "CEIS"
					}

				#impedimento["raw"].append(a.decode("iso-8859-1").replace('\r\n', ''))
				impedimento["raw"].append(a.replace('\r\n', ''))

				"""
				Data Início Sanção
				Data Final Sanção	
				Orgão Sancionador	
				UF Orgão Sancionador	
				Origem Informações	
				Data Origem Informações	
				Tipo Sanção	
				CPF ou CNPJ Sancionado	
				Nome Informado pelo Órgão Sancionador	
				Razão Social - Cadastro Receita	
				Nome Fantasia - Cadastro Receita	
				UF do CPF ou CNPJ
				"""

				impedimento["dados_ceis"].append({
					"inicio_sancao": timefix("i"+b[0].strip()),
					"termino_sancao": timefix("f"+b[1].strip()),
					"orgao_sancionador": b[2].upper(),
					"orgao_sancionador_uf": b[3].upper(),
					"origem_informacao": b[4],
					"origem_informacao_data": timefix("i"+b[5].strip()),
					"tipo_sancao": b[6].upper(),
					"nome": b[9].upper(),
					"nome_receita": b[9].upper(),
					"nome_fantasia": b[10].upper()
				})
				impedimentos_ceis[documento] = impedimento
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
				if impedimentos_cepim.has_key(documento):
					#print "tem chave"
					impedimento = impedimentos_cepim[documento]
				else:
					#print "novo"
					impedimento = {
						"documento": documento,
						"nome": b[1].upper(), #.decode("iso-8859-1").encode('utf8'),
						#"origem": "%s" % (b[3]),
						"tipo_pessoa": "fisica" if len(documento) == 11 else "juridica",
						"data_cadastro": datetime.now().strftime("%d/%m/%Y"),
						"arquivo": file_to_process,
						"dados_cepim": [],
						"raw":[],
						"tabela": "CEPIM"
					}



				impedimento["raw"].append(a.replace('\r\n', ''))
				#impedimento["bloqueios"].append({"convenio": b[2], "info": b[4]})

				"""
				CNPJ Entidade	
				Nome Entidade	
				Número Convênio	
				Órgão Concedente	
				Motivo Impedimento
				"""

				impedimento["dados_cepim"].append({
					"cnpj_entidade": b[0],
					"nome_entidade": b[1].upper(),
					"numero_convenio": b[2],
					"orgao_concedente": b[3].upper(),
					"motivo_impedimento": b[4].upper(),
				})


				#impedimento["raw"].append(a.decode("iso-8859-1").encode('utf8').replace('\r\n', ''))
				impedimentos_cepim[documento] = impedimento
			#except:
			#	print "error on " + b		
	return




print "***** %s *****" % datetime.now().strftime("%d/%m/%Y")

try:
    arg = sys.argv[1]

    if arg in CONFIG:
    	CONFIG["current"] = arg
    else:
    	print "Parametro invalido: %s" % arg
    	sys.exit(1)

except IndexError:
	pass

print "enviando para %s..." % CONFIG["current"].upper()

process_CEIS()
process_CEPIM()
print "total de impedimentos: CEIS: %s; CEPIM %s." % (len(impedimentos_ceis), len(impedimentos_cepim))
save_file("../raw/impedimentos_ceis.json", impedimentos_ceis)
save_file("../raw/impedimentos_cepim.json", impedimentos_cepim)
save_elasticsearch()


