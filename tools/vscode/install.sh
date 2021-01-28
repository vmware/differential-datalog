#!/bin/bash
# This script packages and installs the vscode extensions for highlighting DDlog programs

set -ex

case "${OSTYPE}" in
    linux*)
        if [ "$(cat /etc/*-release | grep -Ec 'ubuntu|debian')" -ne 0 ]; then
            sudo apt-get install nodejs
	elif [ "$(cat /etc/*-release | grep -c -e centos -e rhel )" -ne 0 ]; then
            sudo yum install nodejs
	else
	    echo "Unhandled operating system $OSTYPE"; exit 1;
	fi
        sudo npm install -g npm
        ;;
    darwin*)
        brew install node
        npm install -g npm
        ;;
    *) echo "Unhandled operating system $OSTYPE"; exit 1;;
esac

# Package and install the highlighting extension

cd ddlog-language
npm install
./node_modules/.bin/gulp compile-json
./node_modules/.bin/vsce package
code --install-extension vscode-ddlog-syntax-0.0.1.vsix
rm vscode-ddlog-syntax-0.0.1.vsix
cd ..

# Package and install the theme
# Currently commented-out.
#
#cd ddlog-theme
#../ddlog-language/node_modules/.bin/vsce package
#code --install-extension ddlogtheme-0.0.1.vsix
#rm ddlogtheme-0.0.1.vsix
#cd ..
#
