# Dragoman
An Optimized Interpreter for Functional Mappings that is RML-engine-agnostic. It planns the optimized execution of FnO functions integrated in RML mapping rules and interprets and transforms them efficiently. Since Dragoman is engine-agnostic it can be adopted by any RML-compliant Knowledge Graph creation framework.

## You can use Dragoman with your own library of functions! Here is how:
1. Make a copy of functions.py that is located in ./Interpreter/ and rename it (we consider it as new_function_script.py)
2. Edit new_function_script.py by adding your functions definitions following the sctructure provided in the script and save the chnages
3. Go to the connection.py and replace ".functions" with ".new_function_script" at line 6 and save the changes

That's it! You are ready to go :)
