# ![Dragoman](https://github.com/SDM-TIB/Dragoman/blob/master/images/dragoman.png "Dragoman")
An Optimized, RML-engine-agnostic Interpreter for Functional Mappings. It planns the optimized execution of FnO functions integrated in RML mapping rules, interprets and transforms the rules into function-free ones efficiently. Since Dragoman is engine-agnostic it can be adopted by any RML-compliant Knowledge Graph creation framework. Read more about [Dragoman](https://doi.org/10.15488/13537)

## You can use Dragoman with your own library of functions! Here is [how](https://tib.eu/cloud/s/ikjiHyf8RNrEHSY):
1. Make a copy of functions.py that is located in ./Interpreter/ and rename it (we consider it as new_function_script.py)
2. Edit new_function_script.py by adding your functions definitions following the sctructure provided in the script and save the chnages
3. Go to the connection.py and replace ".functions" with ".new_function_script" at line 6 and save the changes

That's it! You are ready to go :)

# Installing and Running the Dragoman 
## Installing Requirements
```
pip install -r requirements.txt
```
## Executing Dragoman
```
python run_translator.py /path/to/config
```

## Version 
```
1.0
```

## License
This work is licensed under Apache 2.0

# Authors
- Samaneh Jozashoori (samaneh.jozashoori@tib.eu) 
- Enrique Iglesias (iglesias@l3s.de) 
- Maria-Esther Vidal (maria.vidal@tib.eu)
