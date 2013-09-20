SETTINGS = {};
SETTINGS['SERVER_PROD'] = "http://apps.thacker.com.br:9200/";
SETTINGS['SERVER_DEV'] = "http://127.0.0.1:9200/";
SETTINGS['COLECOES'] = ['ceis','cepim'];


switch(document.location.hostname) {
	case "localhost":
		SETTINGS["SERVER"] = SETTINGS["SERVER_DEV"];
		break;
	default:
		SETTINGS["SERVER"] = SETTINGS["SERVER_PROD"];

}