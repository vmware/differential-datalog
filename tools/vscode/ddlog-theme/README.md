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
4.Install vsce by issuing command "npm install -g vsce"

## Building package
1.Go to directory of vscode ddlog theme extension <path of code directory>\differential-datalog\tools\vscode\ddlog-theme.
2.Issue command vsce package. (Install vsce (npm install -g vsce) in case not installed).

## Installation of extension
1.Go to directory of vscode ddlog theme extension <path of code directory>\differential-datalog\tools\vscode\ddlog-theme.
2.Issue command "code --install-extension ddlogtheme-0.0.1.vsix" to install the theme extension.

## Running extension

* Launch Visual Studio Code.
* Open `File > Preferences > Color Theme` and pick "ddlogtheme" color theme.
* Open a ddlog language file. You can see the tokens and scopes applied to tokens.
 To view these scopes, invoke the `Developer: Inspect Editor Token and Scopes` command from the Command Palette (`Ctrl+Shift+P`) .

