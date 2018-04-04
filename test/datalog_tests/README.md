This folder contains tests for the Datalog compiler.
The structure of the test files is as follows:

- Files with suffix `.dl` are datalog files
- Files whose name is like `*fail.dl` are negative tests: they are supposed to fail compiling
  The other files are supposed to compile successfully.

- Files with the name `*.ast.expected` contain the output produced by the compiler.  For each 
  `*.dl` file that should succeed there should be a corresponding `.ast.expected` file
- Files with the name `*.ast` are temporary, they are produced by the compiler and must 
  be identical with the `.ast.expected` files 

  
