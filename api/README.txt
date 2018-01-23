Boda Telecom Suite  API Server


Folder structure
--------------------------------
btsapi
    |-- run.py
    |-- config.py
	|-- tests 			 	# Tests
    |__ /env             	# Virtual Environment
    |__ /btsapi             # Our Application Module
         |-- __init__.py
         |-- /modules   
			   |-- __init__.py
		       |--networkaudit
					 |-- __init__.py
					 |-- controllers.py
         |__ /templates
             |-- 404.html
             |__ /module
                 |-- template.html
         |__ /static
			 |--style.css