FROM lambci/lambda:build-python3.6

RUN pip install --upgrade pip
RUN pip install snowflake-connector-python
RUN curl https://releases.hashicorp.com/terraform/0.11.7/terraform_0.11.7_linux_amd64.zip > terraform.zip
RUN unzip terraform.zip
RUN mv terraform /var/lang/bin
RUN echo First, run 'aws configure' to configure your aws cli credentials. Then cd to 'SnowAlert/IaC' and edit 'aws.tf' to include those same credentials. Finally, run 'python install-snowalert.py'. > /etc/motd
RUN echo cat /etc/motd > ~/.bashrc
