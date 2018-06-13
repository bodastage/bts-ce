#!/bin/bash
#
#   Copyright 2018 Bodastage Solutions
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
#
#    Management utility for BTS-CE
#	
# set -x 

cur_dir=$(realpath $(dirname $0))


eval "$(docker-machine env default)" 2>/dev/null

function show_help(){

echo "Boda Telecom Suite CE - Management Utility"
echo "-----------------------------------------------------"
echo "bts version                        -- Application version"
echo "bts setup                          -- Setup application, create and start services"
echo "bts start [service_name]           -- Start application services"
echo "bts stop [service_name]            -- Stop application"
echo "bts status                         -- See process statuses"
echo "bts logs [service_name]            -- See logs from containers"
echo "bts images                         -- See images"
echo "bts rm [service_name]              -- Stop and remove"
echo "bts create [service_name]          -- Create services"
echo "bts ps [service_name]              -- Display running processes"
echo "bts pause [service_name]           -- Pause services"
echo "bts unpause [service_name]         -- Pause services"
echo "bts exec service_name command      -- Run command in service's container"
echo "bts recreate                       -- Re-create the services. Useful when working with a version update"
echo ""
# echo "bts manage upgrage -- Upgrade"
# echo "bts manage list modules -- List installed modules"
echo "-----------------------------------------------------"
echo "Boda Telecom Suite - Community Edition"
echo "Copyright 2017-2018. Bodastage Solutions. http://www.bodastage.com"

}


function run_setup(){
	echo "Running the setup..."
	curl -fsSL get.docker.com -o get-docker.sh
	chmod +x get-docker.sh 
	$cur_dir/get-docker.sh
	
	if [[ $? -eq 0 ]]
	then 
	    $cur_dir/bts.sh create
	fi
}

function show_version(){
	 version=$(cat $cur_dir/VERSION 2>/dev/null)
	 echo "Version: $version"
	 echo "Boda Telecom Suite - Community Edition"
	 echo "Copyright 2017-2018. Bodastage Solutions. http://www.bodastage.com"
}


if [[ $# -eq 0 ]]
then
	show_help
	exit 0
fi 

for i in "$@"
do 
  case $i in 
     version)
		show_version
		exit 0
	 ;;
	 status)
	 docker container ls
	 exit 0
	 ;;
	 start)	 
		 if [[ $2 != "" ]]
		 then
			docker-compose start $2
		     exit 0 
	     fi

		 docker-compose start
	     exit 0
	 ;;
	 restart)
		 if [[ $2 != "" ]]
		 then
			docker-compose restart $2
			exit 0
		 fi
		 
		 docker-compose restart
	     exit 0
	 ;;
	 stop)
		 if [[ $2 != "" ]]
		 then
			docker-compose stop $2
			exit 0
		 fi
		 
		 docker-compose stop
	     exit 0
	 ;;
	 logs)
		 if [[ $2 != "" ]]
		 then
			docker-compose logs $2
			exit 0
		 fi
		 
		 docker-compose logs
	     exit 0

	 ;;
	 images)
		 if [[ $2 != "" ]]
		 then
			docker-compose images $2
			exit 0
		 fi

		 docker-compose images 
	     exit 0
	 ;;
	 rm)
		if [[ $2 != "" ]]
		then
			docker-compose stop $2
			docker-compose rm -f $2
			exit 0
		 fi
		 
		docker-compose stop
		docker-compose rm -f 
	    exit 0
	 ;;
	 pause)
		if [[ $2 != "" ]]
		then
			docker-compose pause $2
			exit 0
		 fi
		 
		docker-compose pause
	    exit 0
	 ;;
	 unpause)
		if [[ $2 != "" ]]
		then
			docker-compose unpause $2
			exit 0
		 fi
		 
		docker-compose unpause
	    exit 0
	 ;;
	 create)
		if [[ $2 != "" ]]
		then
			docker-compose up -d $2
			exit 0
		fi
		 
		docker-compose up -d
	    exit 0
	 ;;
	 exec)
		if [[ $2 != "" ]]
		then
			docker-compose stop $*
			exit 0
		fi
		
		docker-compose unpause
		exit 0
	 ;;
	 setup)
		run_setup
		$cur_dir/bts.sh create
		exit 0
	 ;;
	 *|help)
	 show_help
	 exit 0
	 ;;
  esac
done