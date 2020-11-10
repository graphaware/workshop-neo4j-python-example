# workshop-neo4j-python-example
##Requirements
This project has been testes with python version 3.7.x.

In order to check your python version, run one of the following commands:

`
$ python --version
`

Or, specifically on Mac,

`
$ python3 --version
` 

In the rest of the Readme.md we will use the python3 case since it is the most common scenario and it works in any case 
(even though you have multiple python versions installed) 
 
##Virtual Environment Creation

Personally, I prefer to run always in a virtual environment. 
In this way all the dependencies will be installed here and everything will be more controlled. 

Run the following command, from the home of the project:

`
$ python3 -m venv ./venv
`

In the case you are missing some dependencies follow the instructions returned to install what you need.

Activate the environment:

`
$ source ./venv/bin/activate
`

From now on, in the terminal where you executed this last command everything will happen in that environment.
That's why from now on, calling python or python3 will be exactly the same.

If you are using an IDE, such as Pycharm, remember to configure it to use the created virtual environment.  

##Install Dependencies
With the virtual environment activated, go in the scripts directory and run:

`
$ make
`
 This will install the necessary dependencies for all the scripts.