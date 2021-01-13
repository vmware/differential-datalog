# DDLog 
[![GitHub](https://github.com/vmware/differential-datalog)](https://github.com/vmware/differential-datalog)
* This directory holds the files required for syntax highlighting ddlog programs in Visual Studio Code.

## Resources
- [DDlog Language Reference Manual](https://github.com/vmware/differential-datalog/blob/master/doc/language_reference/language_reference.md)

## [License](https://github.com/vmware/differential-datalog/blob/master/LICENSE)

## Installation of required packages
* Install VSCode. Please refer https://code.visualstudio.com/Download
* Install Node.js. Please refer https://nodejs.org/en/
* Install Git. Please refer https://git-scm.com/
* Install gulp by issuing command "npm install -g gulp@4.0.0"
* Install gulp-merge-json by issuing command "npm install -g gulp-merge-json"
* Install gulp-json5-to-json by issuing command "npm install -g gulp-json5-to-json"
* Install vsce by issuing command "npm install -g vsce"
* Install ddlogtheme. Please refer README available at <path of code directory>\differential-datalog\tools\vscode\ddlog-theme

## Building package
* Download the code 
* Go to directory of vscode ddlog language extension <path of code directory>\differential-datalog\tools\vscode\ddlog-language.
* Issue command "npm install"
* Issue command "gulp compile-json" (Install gulp (npm install -g gulp@4.0.0), gulp-merge-json (npm install -g gulp-merge-json), gulp-json5-to-json( npm install -g gulp-json5-to-json) in case not installed.) 
* Issue command "vsce package". (Install vsce (npm install -g vsce) in case not installed).

## Installation of extension

* Issue command "code --install-extension vscode-ddlog-syntax-0.0.1.vsix" to install the extension.

## Running extension

* Launch Visual Studio Code.
* Open `File > Preferences > Color Theme` and pick "ddlogtheme" color theme.
* Open a ddlog language file. You can see the tokens and scopes applied to tokens.
 To view these scopes, invoke the `Developer: Inspect Editor Token and Scopes` command from the Command Palette (`Ctrl+Shift+P`) .

