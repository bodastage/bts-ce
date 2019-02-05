Rem create mediation directories
Set MEDIATION_DIR=mediation\data\cm
mkdir %MEDIATION_DIR%

For %v in (ericsson huawei,nokia,zte) Do For %f in (in out parsed raw) Do mkdir %MEDIATION_DIR%\%v\%f

Rem Ericsson
For %i in (raw parsed) Do For %j in (bulkcm eaw cnaiv2 backup) Do mkdir %MEDIATION_DIR%\ericsson\%i\%j

Rem Huawei
For %i in (raw parsed) Do For %j in (gexport nbi mml cfgsyn rnp backup) Do mkdir %MEDIATION_DIR%\huawei\%i\%j

Rem ZTE
For %i in (raw parsed) Do For %j in (bulkcm rnp backup) Do mkdir %MEDIATION_DIR%\zte\%i\%j

Rem Nokia
For %i in (raw parsed) Do For %j in (raml2 backup) Do mkdir %MEDIATION_DIR%\nokia\%i\%j

Rem Create reports folder
mkdir mediation\data\reports