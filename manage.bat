@Echo Off
Rem Management sript
Rem 
Rem Manage application
Rem 
Rem Date 22/01/2018
Rem Author Emmanuel Robert Ssebaggala <emmanuel.ssebaggala@bodastage.com> 

Rem application root directory 
For /F %%i In ("%~dp0") Do Set BD_ROOT_DIR=%%~fi
cd %BD_ROOT_DIR%

Rem Status
Rem -------------------------
If "%~1"==""  (
	Echo Boda Telecom Suite CE - Management commands
	Echo -----------------------------------------------------
	Echo manage version           -- Application version
	Echo manage setup        	  -- Start the setup wizard
	Echo manage start             -- Start application processes i.e. API server, Web Client, ActiveMQ
	Echo manage stop              -- Stop application
	Echo manage status            -- See process statuses
	Echo manage logs              -- See logs from containers
	Echo manage images            -- See images
	Echo.
	Rem Echo manage upgrage -- Upgrade
	Rem Echo manage list modules -- List installed modules
	Echo -----------------------------------------------------
	Echo Boda Telecom Suite - Community Edition
	Echo Copyright 2017-2018. Bodastage Solutions. http://www.bodastage.com
)

Rem Run setup 
Rem -------------------------
If "%~1"=="setup" ( 
    Rem create default machine 
	Echo Creating docker machine
	docker-machine create -d hyperv -hyper-virtual-switch "Primary Virtual Switch" default
	
	Echo creating service containers
    docker-compose up -d
)

Rem start 
Rem -------------------------
If "%~1"=="start" ( 
    Rem start machine 
	docker-machine start > Nul
	
    docker-compose start
)

Rem restart 
Rem -------------------------
If "%~1"=="start" ( 
    docker-compose restart
)


Rem stop 
Rem -------------------------
If "%~1"=="stop" ( 
    docker-compose stop
)

Rem logs 
Rem -------------------------
If "%~1"=="logs" ( 
    docker-compose logs
)

Rem images 
Rem -------------------------
If "%~1"=="logs" ( 
    docker-compose images
)