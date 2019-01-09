# -*- coding: utf-8 -*-
# This file is part of Ecotaxa, see license.md in the application root directory for license informations.
# Copyright (C) 2015-2016  Picheral, Colin, Irisson (UPMC-CNRS)


from appli import app
import logging,sys
import logging.handlers



if __name__ == '__main__':
    app.debug = True

    # handler = logging.handlers.RotatingFileHandler('ecotaxaoserver.log', maxBytes=10000, backupCount=1)
    # handler.setLevel(logging.INFO)
    # LoggingFormat = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    # handler.setFormatter(logging.Formatter(LoggingFormat))
    # app.logger.addHandler(handler)
    app.logger.info("EcoTaxaoServer Startup")
    # L'executable est l'interpreteur qui à permis de lancer, mais en WSGI c'est écrasé par le lanceur WSGI
    if app.PythonExecutable=="TBD": # si pas forcé par un fichier de configuration
        app.PythonExecutable=sys.executable



    app.run(host= '0.0.0.0',threaded=True,port=5001)
