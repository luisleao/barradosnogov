

q = {
    "query" : {
        "query_string" : { 
            "query" : '',
            "fields" : ["documento", "nome"],
            "default_operator" : "AND",
        }
    },
    "size": 100,
    "from" : 0,
    "sort" : [
        "nome"
    ]
};



var ehNumero = function(numero) {
	var patt = /(^\d{1,}$)/;
	return patt.test(numero);
};







//42778183000110

function render(q) {

	var url = SETTINGS['SERVER'] + SETTINGS['COLECOES'].join(',') + '/_search?source=' + JSON.stringify(q);
	console.log(url);
    $.getJSON(url, function (data){
		console.log(data);
    	for (index in data.hits.hits) {
    		console.log(data.hits.hits[index]);
    		var documento = data.hits.hits[index]._source.documento;
    		switch (documento.length) {
    			case 11:
    				documento = documento.substr(0,3)+"."+documento.substr(3,3)+"."+documento.substr(6,3)+"-"+documento.substr(9);
    				break;
    			case 14:
    				documento = documento.substr(0,2)+"."+documento.substr(2,3)+"."+documento.substr(5,3)+"/"+documento.substr(8,4)+"-"+documento.substr(12);
    				break;
    		}
			data.hits.hits[index]._source.documento_formatado = documento;
    	}


    	switch (data.hits.total) {
    		case 0:
    			//mensagem não encontrado
    			alert("Nenhum registro encontrado!");
    			break;
    		case 1:
    			//exibir detalhe
				tempo_detalhe.clear();
				//if (window.location.hash.slice(1) != data.hits.hits[0]['_source'].documento)
    			//	window.location.hash = data.hits.hits[0]['_source'].documento;

    			
    			$("#buscar").fadeOut("slow", function(){
    				//render
    				//var impedimento = data.hits.hits[0]['_source'];
		            tempo_detalhe.append(data.hits.hits[0]['_source']);

    				$("#detalhe_empresa").fadeIn();
    			})

    			break;

    		default:
    			//TODO: listar registros
				tempo_impedimentos.clear();

    			$("#impedimentos h2").text(data.hits.total + " resultado(s) encontrado(s)");
    			$("#buscar").fadeOut("slow", function(){
    				$.each(data.hits.hits, function (index, t) {
						console.log(t);
						tempo_impedimentos.append(t['_source'])
					});
    				$("#impedimentos").fadeIn();
    			})
    	}
        console.log(data);

    });
}


function procurar(palavra) {
   	//window.location.hash = palavra;

	$("#impedimentos h1 span").text(palavra);
    $(".resultados").attr("id", palavra);
    $("#hash").attr("href", "#"+palavra);

    q['from'] = 0;
    q['query']['query_string']['query'] = palavra;
    render(q);
}






var hashChanged = function() {
	var palavra = window.location.hash.slice(1);
	$("#buscar input").val(palavra);


	if (palavra == "") {
		$("#detalhe_empresa, #impedimentos").fadeOut("slow", function(){
			$("#buscar").fadeIn();
		})

	} else {
		procurar(palavra);
		$("#detalhe_empresa:visible, #impedimentos:visible").hide();
	}
}



$(document).ready(function () {

    tempo_impedimentos = Tempo.prepare("impedimentos");
    tempo_detalhe = Tempo.prepare("detalhe_empresa");

    if (window.location.hash) {
    	//TODO: verificar se passou documento no hash
    	hashChanged();
    }


	if (("onhashchange" in window) && !($.browser.msie)) {
		window.onhashchange = function () {
			hashChanged();
		}
    }
    else {
		var prevHash = window.location.hash;
		window.setInterval(function () {
			if (window.location.hash != prevHash) {
				storedHash = window.location.hash;
				hashChanged();
			}
		}, 100);
    }


    $("#search").on("submit", function(){
        var palavra = $("#buscar input").val();
        window.location.hash = palavra;
        //procurar(palavra);
    	return false;
    });

    $("h1 a").click(function(e){
    	e.preventDefault();
    	window.location.hash = "";
    })
});




