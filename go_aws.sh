#!/bin/bash -x 
./RUN_ME_ONCE_for_pyenv_dataclean.sh

python3  victor033.py  last_gooddata.csv 

echo "are are deploying via terraform for this pyspark on aws emr  "; read readline 
git clone https://github.com/idealo/terraform-emr-pyspark.git
tree -d -f |grep terraform
#cp victor_pipeline.py terraform-emr-pyspark/scripts 
cp *  terraform-emr-pyspark/scripts ## after deployment in aws, where we run our remote code.   
cd terraform-emr-pyspark
echo "Control-c to stop! Please editing and configure your aws keypair, do aws client side login,press any key when ready to deploy"; read readline 
echo "./run.sh " >> scripts/bootstrap_actions.sh     # so after deployed in aws, last action in bootstrap will kick start our pipeline 
echo "./run.sh " >> scripts/pyspark_quick_setup.sh 

terraform init
terraform plan
terraform apply
#terraform destroy   ## did this \"terraform destroy \" to avoid your running aws pyspark instant billing !  
 
 
