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
  PKG_LST=$(echo "Installing: $pkg")
done < ./requirements.txt

# Installrequired packages for Pyhton 3
PIP3=$(command -v pip3)
if [ -z "$PIP3" ]; then
  echo "Coudn't find the pip3 command."
  exit 1
fi
echo "Installing [$PKG_LST ] packages for Python 3 ..."
$PIP3 install --no-index --find-links ./src -r ./requirements.txt
echo "** 'pip' for Python 3 was not found"