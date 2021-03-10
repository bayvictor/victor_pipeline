#!/bin/bash -x 

#./dataOps.sh  # replace single quote to double quote, comma centered delimiter 
cat campaign-data.csv |sed 's/\x27/\"/g' > gooddata.csv

which pip
which pip3
which python
which python3 
#sudo apt install python3-pip -y 
#pip3 install pyspark
#pip3 install couchdb 
#./data.clean.step2.sh  # replace dot in header of gooddata.csv   

#!/bin/bash -x 
#cp gooddata_dotted_header_backup.csv gooddata.csv
 cat gooddata.csv |head -n 1|tr ',' '\n'|grep "\." >test01.txt
  
  cat test01.txt|sed 's|^|sed -i \x27s\||g;s|$|\||g' >test012.txt
  cat test01.txt|sed 's|\.|_|g;s|$|\|g\x27|g' >test02.txt
  paste test012.txt test02.txt |sed 's/\t//g' | sed 's|$| gooddata.csv |g;s|\"|\\"|g' > replace_all_dot_in_gooddata_header.csv.sh
chmod +x *.sh
echo "before replacing_dot"
echo "going to replace all dot at header of gooddata.csv"; read readline
./replace_all_dot_in_gooddata_header.csv.sh
echo "after replacement"
 
mv gooddata.csv last_gooddata.csv 
rm test*.txt
rm replace_all_dot_in_gooddata_header.csv.sh

#python3  victor033.py  last_gooddata.csv 


