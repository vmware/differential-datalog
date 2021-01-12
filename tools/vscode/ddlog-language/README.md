# DDLog 
[![GitHub](https://github.com/vmware/differential-datalog)](https://github.com/vmware/differential-datalog)
* This directory holds the files required for syntax highlighting ddlog programs in Visual Studio Code.

## Resources
- [DDlog Language Reference Manual](https://github.com/vmware/differential-datalog/blob/master/doc/language_reference/language_reference.md)

## [License](https://github.com/vmware/differential-datalog/blob/master/LICENSE)

## Installation of required packages
1.Install VSCode. Please refer https://code.visualstudio.com/Download
2.Install Node.js. Please refer https://nodejs.org/en/
3.Install Git. Please refer https://git-scm.com/
4.Install gulp by issuing command "npm install -g gulp@4.0.0"
5.Install gulp-merge-json by issuing command "npm install -g gulp-merge-json"
6.Install gulp-json5-to-json by issuing command "npm install -g gulp-json5-to-json"
7.Install vsce by issuing command "npm install -g vsce"
8.Install ddlogtheme. Please refer README available at <path of code directory>\differential-datalog\tools\vscode\ddlog-theme

## Building package
1.Clone the repository by issuing command "git clone https://github.com/desharchana19/differential-datalog.git"
2.Go to directory of vscode ddlog language extension <path of code directory>\differential-datalog\tools\vscode\ddlog-language.

3.Issue command "npm install"

4.Issue command "gulp compile-json" (Install gulp (npm install -g gulp@4.0.0), gulp-merge-json (npm install -g gulp-merge-json), gulp-json5-to-json( npm install -g gulp-json5-to-json) in case not installed.) 

5.Issue command "vsce package". (Install vsce (npm install -g vsce) in case not installed).

## Installation of extension

1.Issue command "code --install-extension vscode-ddlog-syntax-0.0.1.vsix" to install the extension.

## Running extension

* Launch Visual Studio Code.
* Open `File > Preferences > Color Theme` and pick "ddlogtheme" color theme.
* Open a ddlog language file. You can see the tokens and scopes applied to tokens.
 To view these scopes, invoke the `Developer: Inspect Editor Token and Scopes` command from the Command Palette (`Ctrl+Shift+P`) .

