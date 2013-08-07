import json
from datetime import datetime
import pyes, pprint



#TODO: data_hora cadastro e update
#TODO: salvar no ElasticSearch





impedimentos = {}



def ceis():
	print 'Getting CEIS...'
	ceis_raw = open('../raw/20130723_ceis.csv', 'r')

	for a in ceis_raw.readlines()[1:]:
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
		#except:
		#	print "error on " + b
	return


def cepim():
	print 'Getting CEPIM...'
	cepim_raw = open('../raw/20130801_cepim.csv', 'r')

	for a in cepim_raw.readlines()[1:]:
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






"""
[
	'31/12/2012', 
	'30/12/2017', 
	'MINIST\xc9RIO P\xdaBLICO FEDERAL', 
	'--', 
	'Di\xe1rio Oficial da Uni\xe3o Se\xe7\xe3o 1 p\xe1g 306', 
	'31/12/2012', 
	'Impedimento - Lei do Preg\xe3o', 
	'04875253000160', 
	'Ergo Center Promo\xe7\xe3o de Sa\xfade, Ergonomia eEventos Ltda-ME.', 
	'ERGO CENTER - PROMOCAO DA SAUDE, ERGONOMIA E EVENTOS LTDA', 
	'\x00', 
	'ES'
]

[
	'00057491000107', 
	'CONSORCIO INTERMUNICIPAL DO VALE DO JIQUIRICA', 
	'401004', 
	'MINISTERIO DO MEIO AMBIENTE', 
	'IRREGULARIDADES NA PRESTA\xc3\x87\xc3\x83O DE CONTAS (ATRASO, OMISS\xc3\x83O OU IMPUGNA\xc3\x87\xc3\x83O)'
]


"""

"""


impedimento = {
	documento: "",
	nome: "",
	origem: "",
	tipo_pessoa: "juridica",
	data_cadastro: "",
	data_update: "",
	raw: {}
}




IMPEDIMENTO:
Documento (chave-primaria)
Nome (texto - Nome ou Razao Social)
Origem Registro: CEIS ou CEPIM
Tipo Pessoa: Fisica ou Juridica (CPF/CNPJ)
Raw (texto - json)
DataHora Cadastro (primeiro registro do arquivo)
DataHora Update (do arquivo)




def encerra():
	print 'Getting encerramentos...'
	encerra_raw = open('../raw/encerra.txt', 'r')

	encerramentos = {}
	for a in encerra_raw.readlines()[2:]:
		try:
			b = a.split('#')
			if len(a.split('#')) == 5:
				pl = b[0] + '-' + b[1] + '-' + ''.join(b[2].split('/'))
			encerramentos[pl] = b[4].decode("iso-8859-1").strip()	
		except:
			print "error on " + pl
	return encerramentos

def projeta(encerramentos, ementas):
	print 'Getting tramitacoes...'
	arquivo = open('../raw/tramita.txt', 'r')
	projetos = {}
	for a in arquivo.readlines()[2:]:
		b = a.split('#')
		if len(a.split('#')) == 6:
			pl = b[0] + '-' + b[1] + '-' + ''.join(b[2].split('/'))
			tramite = {}
			try:
				tramite["data_ini"] = b[4]
				tramite["data_fim"] = b[5].strip()
				tramite["tempo"] = (datetime.strptime(b[5].strip(), "%d/%m/%Y") - datetime.strptime(b[4], "%d/%m/%Y")).days
			except:
				pass
			tramite["unidade"] = b[3]

			if projetos.has_key(pl):
				projetos[pl]['tramite'].append(tramite)
			else:
				projetos[pl] = { 'id' : pl }
				projetos[pl]['tramite'] = [tramite]
				if encerramentos.has_key(pl):
					projetos[pl]['encerramento'] = encerramentos[pl]
				if ementas.has_key(pl):
					projetos[pl]['ementa'] = ementas[pl]
	return projetos

"""



def write_json(arquivo, projetos):
	arquivo2 = open(arquivo, 'w')
	arquivo2.write(json.dumps(projetos, sort_keys=True, indent=4, separators=(',', ': ')))
	arquivo2.close()




def upa_neguim():
	print 'Connecting to ES...'
	conn = pyes.ES('http://127.0.0.1:9200')
	try:
		print 'Creating index...'
		conn.indices.create_index("impedimentos")
	except:
		pass


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





def upa_neguim_old(projetos):
	print 'Connecting to ES...'
	conn = pyes.ES('http://127.0.0.1:9200')
	try:
		print 'Creating index...'
		conn.indices.create_index("monitor")
	except:
		pass

	mapping = {
		"data_fim" : { "type" : "string", "analyzer" : "keyword" },
		"data_ini" : { "type" : "date", "format" : "dd/MM/YYYY" }
    }

	print 'Mapping...'
	conn.indices.put_mapping("projeto", {'properties': mapping}, ["monitor"])
    
	erros = 0
	print 'Indexing!'
	for v in projetos:
		p = projetos[v]
		conn.index(p, 'monitor', 'projeto', p['id'], bulk=True)
		try:
			conn.index(p, 'monitor', 'projeto', p['id'], bulk=True)
		except:
			print "erro"
			erros = erros + 1
	print erros



def sample(dicionario, qtd=10):
	i = 0;
	for item in dicionario:
		if i < qtd:
			pprint.pprint(dicionario[item])
			i = i + 1
		else:
			break







print "importing..."
ceis()
cepim()
upa_neguim()
print "import OK"

with open("../raw/impedimentos.json", "w") as the_file:
	the_file.write(json.dumps(impedimentos))
print "file saved"


#encerramentos = encerra()
#projetos = projeta(encerramentos, ementas)
#upa_neguim(projetos)

