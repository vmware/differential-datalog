" Vim syntax file
" Language:	Datalog
" Filenames:    *.dl
"
" Place this file (or a link to it) under ~/.vim/syntax and add
" the following line to your .vimrc to enable syntax highlighting
" automatically for Cocoon files:
" au BufRead,BufNewFile *.dl             set filetype=dl

syn clear

syn case match

syn region dlComment   start="/\*"  end="\*/" contains=dlTodo
syn region dlCommentL  start="//" skip="\\$" end="$" keepend contains=dlTodo
syn region dlString    start=/\v"/ skip=/\v\\./ end=/\v"/

syn match  dlDelimiter         "->"
syn match  dlDelimiter         ":-"
syn match  dlDelimiter	       "[\[\]!?@#\~&|\^=<>%+-,;\:\.@]"

syn region dlRawString start='\[|' end="|]"

"Regular keywords
syn keyword dlStatement        mut break continue return and extern function apply transformer not or input index on output relation stream multiset match var let switch FlatMap Aggregate import as primary key group_by

syn keyword dlTodo             contained TODO FIXME XXX

"Loops
"syn keyword dlRepeat

"Conditionals
syn keyword dlConditional      if else for in

"Constants
syn keyword dlConstant         true false

"Storage class
"syn keyword dlStorageClass

"Keywords for ADTs
syn keyword dlType	        bool istring string bigint bit signed type typedef

syn sync lines=250

" Verilog-style numeric literals
syn match dlNumber "\(\<\d\+\|\)'[sS]\?[bB]\s*[0-1?]\+\>"
syn match dlNumber "\(\<\d\+\|\)'[sS]\?[oO]\s*[0-7?]\+\>"
syn match dlNumber "\(\<\d\+\|\)'[sS]\?[dD]\s*[0-9?]\+\>"
syn match dlNumber "\(\<\d\+\|\)'[sS]\?[hH]\s*[0-9a-fA-F?]\+\>"
syn match dlNumber "\<[+-]\=[0-9]\+\(\.[0-9]*\|\)\(e[0-9]*\|\)\>"


if !exists("did_dl_syntax_inits")
  let did_dl_syntax_inits = 1
  hi link dlStatement          Statement
  hi link dlOperator           Operator
  hi link dlType               Type
  hi link dlComment            Comment
  hi link dlCommentL           Comment
  hi link dlString             String
  hi link dlRawString          String
  hi link dlDelimiter          String
  hi link dlConstant           Constant
  hi link dlRepeat             Repeat
  hi link dlConditional        Conditional
  hi link dlTodo               Todo
  hi link dlNumber             Number
  hi link dlStorageClass       StorageClass
endif

let b:current_syntax = "dl"

" vim: ts=8
