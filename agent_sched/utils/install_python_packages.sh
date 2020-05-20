#!/bin/bash
#-----------------------------------------#
# Install required Python Packages for
# Lustre statistics collector software
#
#
# Misha Ahmadian (misha.ahmadian@ttu.edu)
#-----------------------------------------#
#
# contents of requirement.txt
while read pkg; do
  PKG_LST=`echo "$PKG_LST $pkg"`
done < ./requirements.txt
# Installrequired packages for Pyhton 2
which pip &> /dev/null
if [ $? -eq 0 ]; then
  echo "Installing [$PKG_LST ] packages for Python 2 ..."
  pip install --no-index --find-links ./src -r ./requirements.txt
else
  echo "** 'pip' for Python 2 was not found"
fi
# Installrequired packages for Pyhton 3
PIP3=$(locate -b pip3 | fgrep -w /usr/bin | head -1)
if [ $? -eq 0 ]; then
  echo "Installing [$PKG_LST ] packages for Python 3 ..."
  $PIP3 install --no-index --find-links ./src -r ./requirements.txt
else
  echo "** 'pip' for Python 3 was not found"
fi
